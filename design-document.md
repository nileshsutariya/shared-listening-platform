# Listen Together: Music Streaming Platform Design

## 1. Scope and goals

Design a Spotify-style streaming platform for 20 million monthly users, 2 million peak concurrent listeners, a 50-million-track catalogue, and up to 200,000 open shared rooms. Rooms contain 2–5,000 listeners. Anyone can add tracks and vote to skip; the host can pause and seek. The room service aims to keep 95% of connected listeners within 200 ms of the server-authoritative playback position under healthy network and device conditions.

This is a production architecture proposal, not a claim that the included TypeScript proof of concept scales. Capacity figures are planning estimates that require load testing. Bluetooth latency, device scheduling, network asymmetry, and buffering make a strict 200 ms guarantee impossible on every device.

**Diagrams:** [architecture.mmd](./architecture.mmd) shows the multi-region system and separation of audio delivery from room control. [host-pause-sequence.mmd](./host-pause-sequence.mmd) details a host pause for a room of 5,000 listeners. Capacity assumptions and calculations are in [capacity-estimates.md](./capacity-estimates.md).

## 2. Architecture and key decisions

**Audio delivery.** Store lossless masters in object storage. An ingest pipeline validates rights and metadata, transcodes into adaptive-bitrate HLS with CMAF/fMP4 segments, and publishes manifests and immutable segments to replicated object storage. Serve segments through multiple CDNs using signed, short-lived entitlement tokens. Start with a low-bitrate rendition and a short initial buffer, then adapt to measured bandwidth and device capabilities. Pre-warm popular releases and keep long-tail content cacheable. WebSockets carry playback control only; audio bytes never pass through room servers.

**Catalogue and discovery.** PostgreSQL is the authoritative catalogue for tracks, albums, artists, rights windows, territories, and release schedules. OpenSearch is a derived index for prefix/typeahead, fuzzy matching, ranking, and filters. A transactional outbox publishes catalogue changes for indexing. Release publication is scheduled and idempotent: commit metadata and rights first, then expose the indexed document only when its availability window opens. Search can lag briefly without affecting playback or rights enforcement.

**Room control.** Stateless API and WebSocket gateway fleets authenticate clients and route each room to a single-writer room owner. The owner serializes queue edits, votes, pause, seek, and track transitions. Kafka is the ordered, replicated room-event log; Redis holds rebuildable snapshots and supports regional gateway fan-out. Etcd leases and fencing epochs prevent two owners from committing concurrently. The owner commits a command before acknowledging it; gateways then fan out one versioned event per subscribed gateway, not one broker publish per listener.

**Scale and isolation.** Partition rooms across owners by room ID. Spread large rooms across gateways; a room with 5,000 listeners typically uses multiple gateways, each writing a shared pre-serialized frame to its local sockets in bounded batches. Relay a single room subscription to each region containing listeners. Use bounded queues and load shedding so a large room or analytics backlog cannot starve room commands. Keep search, history, and analytics off the playback critical path.

## 3. Room synchronization and pause flow

The authoritative playback anchor is `{trackId, trackInstanceId, status, positionMs, serverTimestamp, stateVersion, epoch}`. While playing, clients calculate position from the anchor and estimated server time; while paused, the position is fixed. Clients estimate server-clock offset with an NTP-style WebSocket exchange. Resume, seek, and track changes schedule playback at a future server timestamp with a small lead, giving clients time to buffer and start together. Pause includes the exact committed position and applies as soon as the event arrives.

Clients tolerate ordinary clock drift with a small playback-rate correction and hard-seek when drift exceeds a threshold or after buffering. A stalled client never blocks the room: it catches up to the current anchor when media is ready. Late joiners receive a snapshot and server time, then seek to the calculated position. Reconnect sends the last applied version; the gateway returns missed events when available or a fresh snapshot otherwise. Commands have unique IDs for idempotent retries. Clients apply only the next state version, ignore duplicates, and resync on a gap.

For a 5,000-listener pause, the gateway validates the host command and forwards it to the room owner. The owner checks the lease, host authorization, expected version, track instance, and duplicate-command cache; computes the position; and appends a versioned pause event to Kafka. Only after durable commit does it update the snapshot, acknowledge the host, and publish to regional relays and subscribed gateways. Gateways fan out in bounded batches. The owner does not wait for listener acknowledgements. Slow or disconnected clients reconnect and resync; sampled client telemetry measures actual application and drift. The complete message flow and failure branches are shown in the standalone sequence diagram.

Skip votes are idempotent per authenticated participant and track instance. A configurable majority of eligible connected listeners triggers a skip; the host may skip immediately. Queue ordering and playback transitions are strongly consistent at the owner. Presence, displayed vote counts, and telemetry may be eventually consistent.

## 4. Data placement

| Data | Store | Reason |
| --- | --- | --- |
| Track, artist, album, rights, release metadata; playlists | PostgreSQL, with read replicas | Relational integrity, transactional rights checks, and manageable catalogue metadata volume |
| Typeahead and ranked search index | OpenSearch | Prefix/fuzzy search and filtering; rebuildable from the catalogue/outbox |
| Ordered room commands and state transitions | Kafka, partitioned by room | Durable ordered replay and downstream event delivery |
| Hot room snapshots and fan-out | Redis cluster | Fast join/resync and pub/sub; derived and recoverable from the room log |
| Presence and connection state | In-memory gateway state with short-lived Redis coordination as needed | Ephemeral, high-churn data that should not burden durable stores |
| Listening history | ScyllaDB/Cassandra-style wide-column store, partitioned by user and time | High write volume, time-range reads, and user deletion support |
| Royalty/audit events | Append-only event stream plus immutable ledger storage | Idempotent processing, traceability, and reconciliation |
| Product analytics and telemetry | ClickHouse or a managed analytical warehouse | Columnar aggregation without impacting command processing |
| Audio masters and renditions | Replicated object storage behind CDNs | Durable, scalable media storage and global delivery |

Room snapshots, search documents, and analytics tables are rebuildable derived data. Room events and royalty records require durable retention and idempotent consumers. Listening history and telemetry are not treated as authoritative royalty evidence; settlement reconciles playback records with entitlement and delivery evidence.

## 5. Reliability and failure handling

Room-owner failover within a region targets p95 under 15 seconds. Regional recovery is slower (planning target 5–10 minutes) because room logs replicate asynchronously; a few seconds of recent commands may be lost. Rooms restore paused at the last replicated position rather than claiming seamless recovery. Audio already buffered may continue briefly.

| Failure | Handling and user impact |
| --- | --- |
| Room owner crashes | Lease expires; a replacement claims a higher epoch, fences the old owner, restores snapshot and log tail, and republishes state. Audio continues from its anchor; controls are briefly unavailable. |
| WebSocket gateway crashes | Clients reconnect with jittered backoff and resume from their last version. Buffered audio usually continues during the brief control interruption. |
| Redis outage | Owners continue committing to Kafka; gateways use direct event consumption and cached/owner snapshots. Fan-out and late joins may be slower. |
| Kafka unavailable | Reject or bound new durable mutations rather than falsely acknowledging them. Existing playback may continue from the last anchor, but recent commands cannot be guaranteed after an owner failure. |
| Kafka consumer lag | Playback remains available because history, indexing, analytics, and royalty consumers are asynchronous; alert and scale consumers. |
| Regional outage | Global routing sends reconnects to a healthy region; owners restore from mirrored events and snapshots. Rooms reconnect and resume paused; recent commands may be lost. |
| CDN or origin failure | Retry through an alternate CDN/origin and a lower rendition. Users may see a short stall or reduced quality, but room control remains available. |
| Slow listener or fan-out overload | Bound per-socket queues, drop stale frames for that socket, and resync by version. Shed telemetry before control traffic; no listener can block the room. |
| Rolling deployment | Canary first; drain gateways with reconnect notices and staggered closes. Owners hand off leases gracefully; maintain backward-compatible event schemas and auto-rollback on SLO regression. |
| Search or catalogue degradation | Serve cached results where safe and show search as degraded. Rights checks remain authoritative; do not allow stale search data to grant playback entitlement. |

## 6. Observability and SLOs

Instrument command receipt, durable commit, gateway publish, client apply time, playback-session ID, room version, and trace ID. Keep Prometheus labels low-cardinality (region, client type, gateway); use sampled per-room detail in traces or analytical storage. Report aggregate listener counts and drift distributions, not just server-side delivery success.

Initial targets: at least 95% of sampled active listeners within 200 ms of the room anchor; same-region command propagation p99 under 400 ms (p50 under 80 ms); at least 99.5% of late joins synchronized within 3 seconds; owner failover p95 under 15 seconds; playback failure below 0.3% of sessions; and rebuffer ratio below 0.5% of play time. Alert on drift percentiles, command-to-apply latency, commit latency, resync/gap rate, reconnect rate, owner lease changes, Kafka lag, CDN errors, and buffer/rebuffer metrics.

Validate with a 5,000-client room load test, 200,000-room connection test, injected latency/loss/clock skew, owner and gateway kill tests, a 24-hour soak, and real-device audio-output measurements. Targets are acceptance criteria to measure, not assumed outcomes.

## 7. Alternatives considered

| Decision | Selected | Alternative rejected | Reason |
| --- | --- | --- | --- |
| Audio delivery | HLS with CMAF over CDN | Route audio through room servers | CDN scales bytes independently from room control and supports adaptive bitrate. |
| Room transport | WebSockets | Polling or server-sent events | Bidirectional low-latency control and broad client support; polling is wasteful and SSE is one-way. |
| Room consistency | Single-writer owner per room | Shared-database row lock per command or active-active writers | A single writer makes ordering and voting simple; active-active needs conflict resolution or global consensus. |
| Durable room state | Kafka log plus Redis snapshots | Redis as the source of truth | Ordered replay and recovery are required; Redis pub/sub alone can lose messages. |
| Search | OpenSearch derived index | PostgreSQL-only typeahead at full catalogue scale | A search cluster supports ranking and fuzzy matching; PostgreSQL remains the source of truth. |
| Cross-region rooms | Home-region ownership with async replication | Synchronous global commit for every command | Lower command latency and simpler ownership; explicitly accept brief interruption and seconds of RPO on regional failure. |

## 8. Team of three: build first and postpone

**First 12 weeks.** Engineer A owns identity, catalogue/rights schema, APIs, playlists, and release indexing. Engineer B owns the room protocol, single-writer state machine, host controls, queue/votes, snapshots, reconnect, and owner recovery. Engineer C owns sample audio ingest/transcoding, HLS/CDN integration, deployment/metrics, and load/chaos testing. Weeks 1–4 deliver a playable sample track and basic room controls; weeks 5–8 add queue/votes, reconnection, recovery, and a 5,000-client simulation; weeks 9–12 harden deployment, validate SLOs, document operations, and test failure scenarios.

**First release acceptance:** playable CDN audio; room create/join and synchronized controls; queue and voting; late join and reconnect; owner kill recovery without duplicate commands; and dashboards showing measured drift, command propagation, buffering, and recovery.

**Postpone, but keep extension points:** a full 50-million-track ingest, personalization/recommendations, active-active room ownership, sophisticated DRM and device-level latency calibration, and automated royalty settlement. Define stable IDs, rights-aware playback tokens, versioned events, idempotency keys, and append-only royalty evidence early so these can be added without redesigning room control.
