# Detailed Design Appendix

Companion to `design-document.md`. Not page-limited. Sections map to the assignment topics.

## 1. Audio delivery architecture

### 1.1 Track-processing flow

```
 Label / distributor upload
        |  (signed multipart URL, checksum)
        v
 [Ingest bucket: masters/{uploadId}/master.flac]
        |  event: master.uploaded
        v
 [Ingest + validation service]
   - checksum + format check (FLAC/WAV 16-24 bit, 44.1/48/96 kHz)
   - ffprobe metadata: duration, channels, sample rate; tags vs delivered metadata
   - loudness (EBU R128 / ReplayGain), clipping and silence checks
   - audio fingerprint -> duplicate / rights-conflict detection
   - FAIL -> status=rejected, notify label, DLQ    PASS |
        v                                              v
 event: master.validated  ------------------>  [Job queue: Kafka topic transcode-jobs]
                                                       |  (KEDA-autoscaled workers)
                                                       v
                          [Transcoding workers: one job per rendition, run in parallel]
                            AAC-LC 256 | AAC-LC 128 | HE-AAC 64 | (hot) Opus 160, Opus 96
                            -> CMAF fMP4, 6 s segments (init segment + media segments)
                            -> per-segment SHA-256; decode check; duration within 20 ms of master
                                                       |
                                                       v
                          [Object storage, immutable keys]
                            audio/v1/{trackId}/{assetVersion}/{rendition}/init.mp4
                            audio/v1/{trackId}/{assetVersion}/{rendition}/seg_00001.m4s ...
                                                       |
                                  all required renditions verified?
                                         yes |            | no -> retry (3x, backoff) -> DLQ, alert
                                             v
                          [Manifest writer] writes media playlists, then master.m3u8 LAST
                          (the manifest is the atomic "publish" of the asset version)
                                             |
                                             v
                          audio_assets rows = READY  -> event: asset.ready
                                             |
                                             v
                          [Catalogue publish workflow] -> track becomes playable per rights/release date
```

**Failure handling.** Transcode failure: retry with backoff, then DLQ plus alert; asset stays `PROCESSING`, the track is not playable. A *minimum ladder* (AAC 128 and HE-AAC 64) is required to mark a track playable; other renditions can arrive later. Corrupted segment: verified by hash after upload and re-checked by a periodic scrubber; a bad segment is regenerated from the master under a new `assetVersion`. Immutable keys mean a corrupted object is never "overwritten in place" under a cached URL. Origin failure: the CDN shield retries a replica bucket in another region (cross-region replication); the player retries with exponential backoff and may drop a rendition. Cache miss storm: origin shield collapses concurrent requests (request coalescing); prefetching the first segments of the next queued track warms the edge.

### 1.2 HLS vs DASH

| | HLS (CMAF fMP4) | DASH |
|---|---|---|
| Platform support | Native on iOS, macOS, Safari, Smart TVs; hls.js and ExoPlayer elsewhere | Not native on Apple; dash.js or ExoPlayer |
| DRM | FairPlay (cbcs); Widevine and PlayReady via CMAF | Widevine, PlayReady; FairPlay needs HLS |
| Manifest | m3u8 playlists, simple | MPD, richer (periods, multiple codecs) |
| Verdict | **Primary.** CMAF segments are shared, so DASH manifests can be added later over the same media for Android or smart-TV needs without re-encoding. | Optional later |

### 1.3 Why WebSockets carry commands, not audio

1. Audio over HTTP is cacheable: the CDN serves 97% of bytes; a WebSocket stream is per-listener, uncacheable, stateful.
2. A 5,000-listener room at 138 kbps would cost the room servers 690 Mbps and 5,000 pacing loops, versus about 1.35 MB for a pause event.
3. Clients buffer 10-30 s ahead for resilience; synchronization is done by *position*, not by stream timing, so the media path should not be coupled to room state.
4. ABR, range requests, retries and DRM are HTTP-native features.

### 1.4 Fast start, ABR and delivery security

- **Fast start:** prefetch the master playlist and first two segments of the next queued track (during the last 15-20 s of the current track); first request starts at a rendition chosen from recent bandwidth; small initial buffer (about 6-12 s, one or two segments) and then grow; CDN edge with HTTP/2 or HTTP/3; manifests are immutable and cached for a long time.
- **ABR:** the player picks among AAC 64/128/256 (or Opus) based on throughput, buffer level and device (for example, 256 only on Wi-Fi or by user setting); a downswitch before a stall.
- **Access control:** `POST /playback/sessions` checks entitlement (subscription, territory, release date, rights window) against Postgres/Redis and returns a manifest URL plus a **signed cookie or path-prefix token** scoped to `/audio/v1/{trackId}/*` with a 10-15 min TTL (query-string tokens do not propagate to relative segment URLs). DRM is not required in v1 but CMAF with cbcs keeps it additive; the license server would authorize by the same session.

## 2. Catalogue, search and publishing

### 2.1 Core schema (PostgreSQL, abbreviated)

```sql
artists(id uuid pk, name text, sort_name text, country text, updated_version bigint)
albums(id uuid pk, title text, type text, label text, release_date date, updated_version bigint)
tracks(id uuid pk, isrc text unique, album_id uuid references albums, track_no int, disc_no int,
       title text, duration_ms int, explicit bool, popularity_30d real,
       release_status text,              -- draft | processing | ready | scheduled | published | withdrawn
       release_at timestamptz,           -- scheduled release (global)
       updated_version bigint not null)  -- monotonic per-row version, used as search external version
track_artists(track_id, artist_id, role, ord, primary key(track_id, artist_id, role))
genres(id, name); track_genres(track_id, genre_id)
rights(id, track_id, licensor text, territories text[], available_from timestamptz,
       available_until timestamptz, stream_allowed bool, download_allowed bool)  -- territory sets, not a row per country
audio_assets(id, track_id, asset_version int, rendition text, codec text, bitrate_kbps int,
             manifest_key text, status text, checksum text, duration_ms int)
playlists(id, owner_id, name, visibility, updated_version bigint)
playlist_items(playlist_id, position text,   -- fractional index for cheap reorders
               track_id, added_by, added_at, primary key(playlist_id, position))
users(id, handle, country, privacy_flags jsonb, created_at)
rooms(id, host_id, created_at, expires_at, visibility, settings jsonb, home_region text)
outbox(id bigserial, aggregate_type, aggregate_id, payload jsonb, created_at)
```

Key indexes: `tracks(album_id, disc_no, track_no)`, `tracks(isrc)`, `track_artists(artist_id)`, `rights(track_id)`, partial index `tracks(release_at) where release_status='scheduled'`, `playlist_items` hash-partitioned by `playlist_id`. Source of truth for catalogue: **PostgreSQL**. The search index, caches and Redis copies are derived.

### 2.2 Search

- **Index:** OpenSearch with per-type indexes (tracks, artists, albums, playlists) behind an alias. Fields: `title` (text with edge-n-gram or `search_as_you_type`), `title.keyword`, `artist_names`, `album_title`, `popularity_30d`, `visible_in` (territories), `release_at`.
- **Prefix and fuzzy:** `search_as_you_type` or edge n-gram for 1-3 char prefixes; `match` with `fuzziness=AUTO` and `prefix_length=1` for typo tolerance on 4+ character terms (not on very short terms, to avoid noise).
- **Ranking:** `function_score` blending BM25, exact-prefix boost, artist match boost and `log(1+popularity_30d)`; territory and availability are filters, not scores.
- **Pagination:** `search_after` with a point-in-time for deep pages; limit results to 50 per page.
- **Caching:** Redis cache of top results for popular queries (TTL 60 s) and per-prefix suggestion lists for 1-3 char prefixes; CDN-cacheable for anonymous popular queries.
- **Index updates:** transactional outbox (`outbox` written in the same transaction as the catalogue change) → CDC or poller → Kafka `catalog-changes` → idempotent indexer using `version_type=external` with `updated_version`, so duplicates and late updates are rejected, not applied out of order.
- **Publishing a release without a rebuild:** the new tracks flow through the same incremental pipeline; alias swap and full rebuild are only for mapping changes. Scheduled releases: rows are indexed hidden; a scheduler flips `release_status` at `release_at` and emits an event; query filters also include `release_at <= now` as a safety net. Regional restrictions: `visible_in` filter at query time; **entitlement is rechecked against Postgres rights when a playback session is created**, so a stale search hit can never stream a restricted track (it renders as unavailable).
- **Eventual consistency, retries, reconciliation:** typical lag under 5 s. Failed index updates retry with exponential backoff, then go to a DLQ with alerts. A reconciler compares, per ID range, `count` and `max(updated_version)` between Postgres and OpenSearch hourly and re-emits mismatches; a full verify runs nightly.

## 3. Listen Together room design

### 3.1 Lifecycle

| Phase | Behavior |
|---|---|
| Create | `POST /rooms` (authenticated). Room service writes metadata to Postgres (`home_region`, settings), asks the shard owner to create the actor (`RoomCreated` event), returns `roomId` and an invite link `/r/{roomId}?t={inviteToken}` |
| Invite and join | The invite token is signed, scoped to one room, expires (default 24 h), revocable, and optionally limited to N uses. `POST /rooms/{id}/join` validates the token and the user session, then returns a **60 s single-use WebSocket ticket** (audience: gateway, claims: room, user, role). The client opens WSS and sends the ticket as its first frame (not in the URL, to avoid logging) |
| Permissions | Roles: `host`, `cohost` (optional), `listener`. Only the owner enforces them; gateways never trust client claims |
| Host transfer | Explicit by host, or automatic after the host is disconnected for 60 s: next eligible participant by tenure and role; recorded as `HostChanged`. If none is eligible, the room pauses and goes idle |
| Leave | Presence removed; any skip vote by that user stops counting; the room continues |
| Expire | No participants for 15 min or TTL (default 24 h) reached → `RoomClosed`, sockets closed with a 4xxx code, invites revoked, hot state removed; the log retained per retention policy |
| Delete | Host or admin; same as expire; audit-logged |

### 3.2 Room state

Durable (in log and snapshot): `roomId`, `hostId`, settings, `epoch`, `stateVersion`, `queueVersion`, `playback {trackId, trackInstanceId, status, positionMs, serverTimestamp}`, queue items, vote state (set of userIds per `trackInstanceId`), bounded `recentCommandIds`, moderation list, ownership metadata (`ownerPodId`, `leaseEpoch`). **Ephemeral (not recovered, rebuilt from reconnects):** presence and participant counts, socket IDs, per-listener drift, buffering state, estimated RTTs, typing or reaction indicators.

### 3.3 WebSocket protocol (versioned, backward compatible)

```jsonc
// Envelope (all messages): {"v":1,"t":"<type>","id":"<msgId>","traceparent":"00-..."}
// Client -> server
{"t":"JOIN","ticket":"...","lastVersion":41,"lastEpoch":7}      // lastVersion optional (RESUME)
{"t":"CMD","cmd":{"type":"PAUSE","commandId":"01J...","expectedVersion":41,"trackInstanceId":"T7"}}
{"t":"CMD","cmd":{"type":"SEEK","commandId":"...","positionMs":93000,"trackInstanceId":"T7"}}
{"t":"CMD","cmd":{"type":"QUEUE_ADD","commandId":"...","trackId":"...","clientItemId":"..."}}
{"t":"CMD","cmd":{"type":"SKIP_VOTE","commandId":"...","trackInstanceId":"T7"}}
{"t":"TIME","t0":1712345678901}
{"t":"TELEMETRY","appliedVersion":42,"driftMs":38,"buffering":false,"bufferMs":9200}
// Server -> client
{"t":"SNAPSHOT","state":{...},"serverNow":1712345679000}
{"t":"EVENT","e":{"type":"PAUSED","version":42,"epoch":7,"playback":{...},"commandId":"..."}}
{"t":"ACK","commandId":"...","ok":true,"version":42,"duplicate":false}
{"t":"TIME_REPLY","t0":...,"t1":...,"t2":...}
{"t":"BEACON","version":42}                                       // anti-entropy every 15 s
{"t":"RECONNECT_SOON","afterMs":15000}                            // drain notice
```

Rules: unknown fields are ignored; new event types are ignored by old clients that fall back to a snapshot; `v` is bumped only for breaking changes and two versions are supported concurrently during rollouts.

### 3.4 Keeping listeners within about 200 ms, and why it is not guaranteed

Mechanisms: shared server-time anchor (not message arrival time), scheduled starts with a lead time so propagation delay differences do not matter, clock offset from min-RTT samples, continuous drift correction by rate nudging, hard seek on large drift, and buffer-aware catch-up. Limits: **(a)** HTMLMediaElement seek accuracy is tens of ms and hardware-dependent; **(b)** Bluetooth and external output add 100-300 ms that the browser cannot observe; **(c)** clock-offset error is bounded by path asymmetry, so a mobile uplink/downlink skew of 40 ms leaves a 20 ms error; **(d)** buffering stalls remove audio until data arrives; **(e)** OS scheduling and background throttling delay timers. Hence the SLO is statistical (95% within 200 ms) and device-calibration is listed as future work.

### 3.5 Large rooms and fan-out

Described in `design-document.md` section 7 and `capacity-estimates.md` section 6. Additional rules: per-socket send buffers capped (for example 64 KB); a socket over the cap is marked `stale` and receives a snapshot on its next opportunity rather than an unbounded queue of old events; gateway-level rate limits on inbound client messages (for example 10 messages/s); presence changes are batched (every 1-2 s) so join/leave churn cannot flood the owner; the owner reads participant counts from Redis rather than holding live rosters for 5,000 people.

### 3.6 Queue and voting details

- **Ordering:** owner assigns `seq`; the queue is ordered by `seq` unless the host reorders (`MOVE itemId afterItemId`, applied by the owner). The client inserts optimistically with `clientItemId` and reconciles to the server `itemId`.
- **Limits and moderation:** 5 outstanding adds per non-host user, 200-item queue cap, duplicate-track warning, host can remove items, mute/ban users, switch the room to host-only queueing.
- **Versioning:** `queueVersion` increases on any queue mutation; `stateVersion` increases on every committed event. Reorders and removes require the current `queueVersion`; adds do not.
- **Skip votes:** one vote per authenticated participant (set semantics make duplicates idempotent); eligible if joined at least 30 s earlier; threshold = more than 50% of currently connected eligible participants (configurable); a user disconnecting stops counting after a 30 s grace; votes reset on track change; a vote round expires after 60 s. The host can skip without a vote.
- **Consistency:** see `design-document.md` section 7.

### 3.7 Ownership, failover and no-double-processing

Protocol and layers of fencing are in `design-document.md` section 7. Supplementary details: shards are claimed by rendezvous hashing preference to limit contention; the owner **self-fences** and stops processing at 70% of the lease TTL if it cannot renew; the gateway caches shard→owner mappings tagged with epoch and retries commands for up to 2 s while failover is in progress; a graceful shutdown releases leases and writes a final snapshot to make planned migrations fast (about 1-2 s). **Anti-double-replay:** each event has `(roomId, epoch, version, commandId)`; the reducer applies an event only if `version == current + 1`; the dedup window is rebuilt from `commandId`s in the replayed tail; the royalty or history pipeline uses deterministic IDs derived from `(roomId, version)` or `playbackSessionId`, so duplicates collapse downstream.

## 4. Storage design

### 4.1 Storage components

| Store | Owns | Why selected | Consistency | Class |
|---|---|---|---|---|
| PostgreSQL | users, tracks, albums, artists, rights, playlists, room metadata, outbox | Relational integrity; small data; strong transactions | Strong on primary; replicas eventually consistent (reads for browse) | Authoritative |
| Object storage | masters, renditions, segments, manifests | Cheap, durable, immutable | Strong read-after-write for new keys | Authoritative (masters); rebuildable (renditions) |
| OpenSearch | catalogue search index | Prefix, fuzzy, ranking | Eventual (seconds) | Rebuildable derived |
| Redis | hot room snapshots, event rings, pub/sub, caches, rate-limit counters | Sub-ms access | Best effort; CAS by epoch | Ephemeral / rebuildable |
| Kafka | `room-events`, `catalog-changes`, `playback-events`, `royalty-events` | Ordered, replicated, replayable | Strong for acknowledged writes (`acks=all`, min ISR 2) | Authoritative (room events); durable transport otherwise |
| etcd | shard leases and epochs | Strongly consistent | Linearizable | Authoritative, tiny |
| ScyllaDB | listening history | Write-heavy, per-user partitions, cheap per-user deletion | Tunable, LOCAL_QUORUM | Authoritative |
| ClickHouse | aggregated playback, drift, QoE, business analytics | Columnar, fast aggregates | Eventual | Rebuildable derived |
| Royalty ledger | qualified stream records, adjustments, reconciliation outcomes | Auditability | Append-only, object lock + Postgres summaries | **Immutable financial record** |

### 4.2 Indexes, partitioning, retention, replication, backups

- **Postgres:** B-tree on foreign keys and lookup keys; partial indexes for state flags; hash partitions for `playlist_items`; 1 primary + 2 synchronous-or-semi-sync replicas across AZs; PITR with WAL archiving (7-day window), daily snapshots; failover via Patroni or the managed service (RTO under 60 s).
- **Kafka:** RF=3, min ISR 2, rack-aware; `room-events` retention 7 days plus periodic **snapshots** in Postgres/object store to bound replay; `playback-events` 7 days then archived as Parquet.
- **Object storage:** versioning and object lock on masters; lifecycle moves masters to infrequent access; deleting withdrawn assets is a controlled job.
- **ScyllaDB:** partition key `(user_id, month)`, clustering `played_at desc`; TTL or archival after 13-24 months; per-user deletion on privacy requests.
- **ClickHouse:** partition by day, order by `(track_id, ts)`; TTL 13 months raw, aggregates retained.
- **Backups and restore tests** quarterly; infrastructure as code (Terraform) for all of the above.

### 4.3 From playback telemetry to history, analytics and royalties

```
client (batched beacons: start, 30 s qualified, end, QoE)
   -> Telemetry ingest (validates, rate-limits, enriches with session + entitlement)
   -> Kafka playback-events (partition key = playbackSessionId, preserves per-session order)
        |-> History processor  -> ScyllaDB (idempotent upsert on eventId)
        |-> ClickHouse sink    -> analytics (ReplacingMergeTree on eventId)
        '-> Royalty processor  -> royalty-events -> immutable ledger
```

Nothing here is on the playback path: the client keeps playing if telemetry fails, and beacons are buffered on device (bounded) and retried.

### 4.4 Example event schema

```json
{
  "eventId": "01J8Z5W9P4Q3R7T2X6Y1V0N8MB",
  "eventType": "stream.qualified",
  "schemaVersion": 1,
  "eventTime": "2026-10-13T09:15:42.318Z",
  "ingestTime": "2026-10-13T09:15:43.002Z",
  "trackId": "7d1a9c2e-5b3f-4a64-9f0e-2c8b1d3a4e55",
  "listenerId": "pl_9f2c81e0b7",
  "playbackSessionId": "ps_01J8Z5V3K2M9",
  "roomId": "rm_01J8Z2A8D4",
  "context": {
    "source": "listen_together",
    "stateVersion": 42,
    "playedMs": 31250,
    "trackDurationMs": 214000,
    "rendition": "aac_128",
    "territory": "IN",
    "clientType": "ios",
    "offline": false
  },
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

`listenerId` is a pseudonymous ID; the mapping to a user lives only in the history store with privacy controls. `eventId` is deterministic for derived events (hash of `playbackSessionId + trackId + playIndex + eventType`) so retries and duplicates collapse.

### 4.5 Idempotency, duplicates, late events, ordering, reconciliation

- **At-least-once + idempotent writes.** History: upsert by `eventId`. ClickHouse: `ReplacingMergeTree` on `eventId` (and `FINAL` or dedup views for exact counts). Ledger: unique constraint on `eventId`; inserts use `ON CONFLICT DO NOTHING`.
- **Ordering.** Only per-session ordering is needed (partition key `playbackSessionId`). Cross-session order is irrelevant.
- **Late events.** Accepted up to 72 h by event time (offline devices); counted into the period in which they occurred; if the period is already settled, a **compensating adjustment event** is appended (never an update).
- **Reconciliation.** Daily job compares: qualified-stream counts from telemetry vs unique `(sessionId)` rows in CDN segment logs and entitlement sessions (plausibility: a 30 s stream requires about 30 s of segments served); flags sessions with no matching delivery; compares ledger totals to the warehouse. Threshold: under 0.1% unexplained discrepancy; anything higher blocks settlement for that period and creates a ticket. Client telemetry is untrusted for royalties.

## 5. Failure matrix (all 12 scenarios)

| # | Failure | Detection | Mitigation | Recovery | User-visible behavior |
|---|---|---|---|---|---|
| 1 | Room owner process crash | Lease expiry (up to 10 s); gateway RPC failures | Standby claim of shard with `epoch+1`; gateway retries commands up to 2 s | Fence old producer, load snapshot, replay log tail, rebuild dedup window, republish snapshots | Audio continues from the last anchor; controls unavailable about 5-15 s; deterministic auto-advance if a track ends |
| 2 | WebSocket gateway crash | LB health checks, socket close | Clients reconnect with jittered backoff and `RESUME`; connection-rate limiting | Per-room version ring or snapshot from Redis | "Reconnecting" for a few seconds; audio uninterrupted (buffered) |
| 3 | Redis / hot store unavailable | Client errors, circuit breaker | Owners keep state in memory and commit to Kafka; gateways consume `room-events` directly; snapshots from gateway cache or owner | Rebuild Redis snapshots from owners and log; resume pub/sub | Slightly higher latency; late join slower; no state loss |
| 4 | Kafka lag or outage | Consumer-lag alerts; produce timeouts | Lag: no effect on room commit. Outage on commit path: degraded durability (memory + local WAL + Redis snapshot, bounded buffer), flagged | Drain WAL to Kafka on recovery; reconcile versions; alert on any room that lost an owner during degraded mode | Playback unaffected; rare loss of the most recent commands if an owner dies during the outage |
| 5 | Regional outage | Health checks; global region-lease expiry | GSLB moves clients; region B owners restore from mirrored log (async, RPO seconds); old region self-fences when it loses the region lease | Rooms resume *paused at last-known position*; epochs bumped; reconciliation of history and royalty events after recovery | Interruption of minutes; recent commands (seconds) may be lost; listeners reconnect and resync. **Active rooms do not survive seamlessly** |
| 6 | CDN or object-store origin failure | Elevated 5xx and rebuffer SLIs | Multi-CDN or origin failover to replica bucket; player retries and may downshift | Backfill replication gaps; scrub segments | Short stall; lower quality; very rare track errors |
| 7 | Database (Postgres) failover | Replication health, connection errors | Automatic promotion (30-60 s); read replicas and Redis cache keep catalogue reads alive; rooms unaffected (state is in log and memory) | Re-point services; replay outbox | Catalogue writes, playlist edits, and new playback sessions may fail for up to about 1 min; ongoing rooms and playback continue |
| 8 | Sudden spike in large rooms | Gateway CPU, send-queue depth, join rate | Autoscale gateways; admission control and join queue for the hot room; hierarchical fan-out; bounded queues; shed telemetry and presence detail | Scale down; cache snapshot per room version | Some joins wait seconds; no effect on others |
| 9 | Rolling deployment during sessions | Deploy controller; canary SLIs | Drain gateways (stop accepting, `RECONNECT_SOON`, staggered close over 30-60 s); owners hand off shards via graceful lease release and final snapshot; backward-compatible protocol; canary 5% then 25% then 100%, auto-rollback on SLO burn | Resume from snapshots | At most a brief reconnect |
| 10 | Slow or disconnected listener | Send-buffer depth, missed ping/pong | Bounded queue; drop frames for that socket only; mark stale; close after N seconds | Snapshot on reconnect, local catch-up seek | Only that listener resyncs; room unaffected |
| 11 | Duplicate pause or seek | `commandId` in dedup window; version check | Cached ack for duplicates; no-op pause on already-paused; `trackInstanceId` guard for seeks | None needed | None |
| 12 | Partial search-index outage | Cluster health, query errors, shard allocation | Replicas serve; circuit breaker; serve cached popular queries; degrade to prefix-only; Postgres fallback for exact title lookups | Re-allocate shards; reconciler repairs missing docs | Slower or less fuzzy search; playback unaffected |

Cross-cutting: **multi-region** with regional traffic routing via GSLB; **room affinity** (room's `home_region` encoded in `roomId`, listeners use local gateways, owner is in the home region); **controlled room migration** (drain the shard: write snapshot, release the lease, new region claims, publish `OwnerChanged`); **RPO/RTO** as in `design-document.md` section 9; **circuit breakers** on Redis, search, rights and Kafka-produce paths with bounded retries; **rate limiting and load shedding** order: telemetry detail, search suggestions, history writes, then (last) room control; **disaster recovery** drills quarterly and post-recovery reconciliation of history, royalty events and search index.

## 6. Security and abuse prevention

| Area | Design |
|---|---|
| Authentication | OAuth 2.0 / OIDC (authorization code + PKCE) for apps; short-lived access tokens (10-15 min), refresh rotation; secure, HttpOnly, SameSite cookies for web sessions |
| Invites | Opaque or signed tokens scoped to a room, short TTL (default 24 h), optional max uses, revocable by the host; rotation on demand; exchanged via POST so they do not linger in logs |
| WebSocket | 60 s single-use ticket sent in the first frame; origin allowlist; JSON schema validation; max message size (4 KB); max messages per second; unauthenticated sockets closed in 5 s |
| Host authorization | Owner checks `userId` against current host/cohost in the authoritative state; client claims ignored; host transfer is an audited event |
| Queue and vote abuse | Per-user quotas and rate limits; account-age and join-time requirements for votes; duplicate votes idempotent; moderation tools (remove, mute, ban, host-only queue) |
| Room hijacking | Unguessable room IDs (128-bit random), invite scoping, ticket binding to room and user, rate-limited join attempts, host-transfer rules, ownership changes only through the lease (no client path) |
| Audio delivery | Entitlement-checked signed cookies or path tokens; short TTL; DRM-ready packaging |
| Data protection | TLS 1.2+ everywhere, mTLS between services; encryption at rest with KMS-managed keys; secrets in a vault |
| Privacy | Per-user controls: private session, hide listening activity, delete history; pseudonymous listener IDs in analytics; retention limits; data-subject deletion pipeline (Scylla delete by `user_id`; ledger keeps pseudonymous financial records only) |
| Audit | Append-only audit log: room create/delete, host change, bans, token issuance/revocation, admin actions |

## 7. Observability and SLOs

### 7.1 Metrics and collection

| Metric | Source | Collection technique |
|---|---|---|
| Playback start latency (p50/p95/p99) | Client | Timestamp tap-to-first-audio; sampled beacon batched with other telemetry; histogram buckets, not raw events |
| CDN cache-hit ratio | CDN logs | Aggregated per region and PoP |
| Rebuffer ratio and duration | Client | Counters batched per 60 s |
| Playback failure rate | Client and server | Error codes in beacons; manifest/segment 4xx/5xx from CDN logs |
| Command propagation latency | Server and client | Owner commit timestamp vs client `appliedAtServerTime` (sampled) |
| Drift distribution; % within 200 ms | Client, aggregated at gateway | 10% sample, 10 s batches, per-room histogram on the gateway; global histogram to Prometheus |
| Late-join success rate | Client | Time from join to first in-sync report |
| WebSocket connections and reconnection rate | Gateway | Gauges and counters |
| Owner failover duration | Owner fleet, etcd | Lease-gap to first accepted command per shard |
| Queue update latency | Server | Commit-to-publish |
| Kafka consumer lag | Kafka exporter | Per consumer group |
| Duplicate / rejected command rate | Owner | Counters by reason |
| Royalty reconciliation discrepancies | Batch job | Daily result gauge |
| Regional error rates and availability | LB and services | RED metrics per region |

**Keeping telemetry cheap:** sample drift (10%) with an always-on report when drift exceeds 150 ms; batch and compress; aggregate at gateways; Prometheus labels limited to region, client type, gateway and result (never `roomId` or `userId`); per-room detail goes to ClickHouse and traces with tail-based sampling.

**Correlation:** the `traceparent` is propagated in WebSocket envelopes, gRPC calls and Kafka headers; every event carries `roomId` and `stateVersion`; every client report carries `playbackSessionId`. Joining on `(roomId, stateVersion)` links the host command's owner span, the publish, each gateway's fan-out span and the sampled client applies.

### 7.2 SLO targets and validation

SLOs are as in `design-document.md` section 10. They are *initial* and should be adjusted after measurement. Validation: (1) staged load tests with virtual clients that have a simulated audio clock, injected clock skew (+/- 500 ms), RTT 30-200 ms with jitter, and 1-5% loss; (2) a 5,000-listener room, then 50 such rooms plus 200k small rooms with 1M sockets across load generators; (3) chaos: kill owners, gateways, Redis, a Kafka broker, and a region; (4) a 24 h soak for memory and reconnect behavior; (5) a physical-device lab (iOS, Android, desktop, Bluetooth) measuring actual audible offsets using loopback recording and cross-correlation, since simulated clocks cannot capture output latency.

## 8. Roadmap details

### 8.1 Shared integration tasks and dependencies

1. Protocol schema package (`@lt/protocol`) shared by gateway, owner and clients: owned by B, reviewed by A and C (week 1).
2. Auth and join ticket contract: A defines, B enforces in the gateway (weeks 2-3).
3. Metrics contract (names, labels, histogram buckets): C defines, A and B instrument (week 3).
4. HLS sample track end to end through the client: C delivers; B uses it for sync tests (weeks 3-4).
5. Load-test harness (virtual clients with simulated clocks): C builds on B's client library (weeks 6-8).

### 8.2 Testing responsibilities

A: API contract tests, schema migrations, entitlement tests. B: property-based tests of the room reducer (ordering, idempotency, version gaps), failover tests with injected lease loss, protocol compatibility tests. C: load, soak and chaos tests, HLS playback tests across devices, dashboard verification. Joint: end-to-end test that creates a room, plays an HLS track, pauses, and verifies drift metrics.

### 8.3 Postponed capabilities that the architecture still accounts for

| Postponed | What the design preserves now |
|---|---|
| 50M-track catalogue | UUIDv7 IDs, version columns, partition-ready tables, search aliases and external versioning |
| Active-active multi-region rooms | `home_region` in room ID; `epoch` and region fields in every event; async mirror; region lease concept |
| Advanced DRM | CMAF with cbcs packaging; path-scoped signed access; session-based entitlement |
| Recommendations / personalization | Playback events already captured with context; ClickHouse available |
| Full royalty settlement | Deterministic event IDs, immutable ledger, reconciliation job, late-event policy |
