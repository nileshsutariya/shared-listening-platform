# Capacity Estimates

All numbers are **estimates from stated assumptions**, meant to size the design and expose bottlenecks. They must be replaced by measurements from load tests and production telemetry. Rounding is deliberate.

## 1. Assumptions

| #   | Assumption                | Value                                            | Notes                                |
|-----|---------------------------|--------------------------------------------------|--------------------------------------|
| A1  | Monthly active users      | 20M                                              | given                                |
| A2  | Daily active users        | 8M (40% of MAU)                                  | assumed                              |
| A3  | Plays per DAU per day     | 40                                               | about 2.3 h/day listening            |
| A4  | Average track duration    | 210 s                                            | 3.5 min                              |
| A5  | Streaming bitrate mix     | 70% AAC-LC 128, 15% AAC-LC 256, 15% HE-AAC 64    | weighted average 137.6 kbps          |
| A6  | Peak concurrent listeners | 2M                                               | given                                |
| A7  | Catalogue                 | 50M tracks                                       | given                                |
| A8  | Peak rooms                | 200k; room listeners about 0.82M (see section 5) | rooms given, distribution assumed    |
| A9  | Regions                   | 3: NA 40%, EU 35%, APAC 25%                      | assumed                              |
| A10 | Devices                   | 65% mobile, 30% web/desktop, 5% other            | WebSocket and HLS both required      |
| A11 | Network                   | RTT 30-150 ms; 1-3% of sessions on lossy links   | drives drift and rebuffer asumptions |
| A12 | Room activity             | Per room: 1 track change/210 s, 1 host control/120 s, 1 queue or vote action/60 s | about 0.03 events/s/room |
| A13 | Segment length            | 6 s CMAF fMP4                                    | trade-off: fewer objects vs faster ABR switching |

## 2. Traffic and concurrency

```
Plays/day            = 8M DAU x 40                     = 320M
Average plays/s      = 320M / 86,400                   = 3,704/s
Average concurrency  = 320M x 210 s / 86,400           = 777,778 (about 0.78M)
Peak / average       = 2M / 0.78M                      = 2.6x
Peak track starts/s  = 2M / 210 s                      = 9,524/s
```

## 3. Audio bandwidth (data plane)

```
Peak egress   = 2M x 137.6 kbps              = 275 Gbps
With 8% overhead (next-track prefetch, HTTP/TLS headers, segment waste)
              = 275 x 1.08                   = about 300 Gbps (37.5 GB/s)
Per region    = NA 120 Gbps, EU 105 Gbps, APAC 75 Gbps

Per play      = 137.6 kbps = 17.2 KB/s x 210 s = 3.61 MB
Daily volume  = 320M x 3.61 MB                 = 1.16 PB/day (1.25 PB with overhead)
Monthly       = about 37 PB (upper bound; skips reduce it)

Origin egress (CDN miss traffic)
  at 97% byte hit  : 0.03 x 300 Gbps  = 9 Gbps
  at 90% byte hit  : 0.10 x 300 Gbps  = 30 Gbps   <- size origin shield and bucket for this
```

Popularity is heavily skewed. Assume the top 1M tracks (2% of the catalogue) carry about 80% of plays, so an edge working set of about 1M x 11.8 MB = 11.8 TB across all renditions covers most traffic. Long-tail tracks drive the misses.

## 4. Storage

Rendition ladder: **baseline for every track** AAC 256, AAC 128, HE-AAC 64 (448 kbps total); **hot tracks only** (top 10%, 5M) additionally Opus 160 and Opus 96.

Master (FLAC, about 1,000 kbps)   : 1,000 kbps x 210 s / 8 = 26.25 MB x 50M   = 1,312 TB (1.3 PB)
Baseline renditions               : 448 kbps x 210 s / 8   = 11.76 MB x 50M   =   588 TB
Hot Opus renditions (5M tracks)   : 256 kbps x 210 s / 8   =  6.72 MB x 5M     =    34 TB
Total logical                                                                 = about 1.93 PB
Physical (erasure coded about 1.5x, or replicated 3x)                          = about 2.9 to 5.8 PB

Objects per rendition : 210 / 6 = 35 segments + 1 init + 1 playlist = 37
Baseline objects      : 37 x 3 x 50M   = 5.55B
Hot extras            : 37 x 2 x 5M    = 0.37B
Total                 : about 5.9B objects  (average segment 48-192 KB; PUT cost and lifecycle rules matter)


Masters go to an infrequent-access class (rarely read: re-transcoding only). Segments are immutable and keyed by asset version.

**Relational metadata (PostgreSQL)**

| Table                         | Rows                      | Row size | Size   |
|-------------------------------|---------------------------|----------|--------|
| tracks                        | 50M                       | 1.5 KB   | 75 GB  |
| albums / artists              | 4.2M / 3M                 | 1 KB     | 7 GB   |
| rights (territory-group rows) | 150M (3 per track)        | 200 B    | 30 GB  |
| audio_assets                  | 250M (5 per track)        | 250 B    | 62 GB  |
| playlists                     | 100M (5 per user)         | 300 B    | 30 GB  |
| playlist_items                | 4B (40 per playlist)      | 60 B     | 240 GB |
| Data total / with indexes     |                           |          |about 450 GB / about 1 TB |

**Search (OpenSearch):** 50M tracks x about 1.2 KB x 1.5 index overhead = 90 GB, plus artists, albums, public playlists = about 120 GB. 12 primary shards of about 10 GB, 1 replica = 240 GB.

**Search load:** 3M app-active users at peak, one search session per user per 10 min = 5k sessions/s x 6 debounced queries = about 30k queries/s. Prefix caching (Redis, 60 s TTL; plus CDN for popular queries) at about 60% hit leaves about 12k/s on OpenSearch (about 12 data nodes with headroom).

**Ingestion and transcoding compute**
Encode : 3 renditions x 210 s at about 60x real time per core  = 10.5 core-s
Package: about 1.5 core-s
Total  : about 12 core-s per track
New releases (100k tracks/day) : 1.2M core-s = 333 core-h/day = about 14 cores continuously
Initial backfill 50M tracks    : 600M core-s = 167k core-h; on 2,000 cores = about 83 h (3.5 days)


## 5. Room workload

Room-size distribution (sums to 200k rooms and 818k listeners = 41% of the 2M peak):

| Tier        | Rooms       | Avg listeners | Listeners   |
|-------------|-------------|---------------|-------------|
| 2-10        | 195,000     | 2.4           | 468,000     |
| 11-100      | 4,500       | 25            | 112,500     |
| 101-1,000   | 450         | 250           | 112,500     |
| 1,001-5,000 | 50          | 2,500         | 125,000     |
| **Total**   | **200,000** |               | **818,000** |

State events          : 200k rooms x 0.03/s                   = 6,000/s   (burst x3 = 18,000/s)
Fan-out deliveries    : 818k listeners x 0.03/s               = 24.5k msgs/s (burst 73.6k/s)
Fan-out bytes         : 24.5k x 270 B on the wire             = 6.6 MB/s (bandwidth is NOT the bottleneck)
Clock sync            : 818k clients / 30 s                   = 27k small requests/s (handled in gateway, no owner involvement)
Anti-entropy beacon   : 1 version beacon per room per 15 s    = 13k room beacons/s, fanned to 818k sockets = 55k msgs/s
Drift telemetry       : 10% sampled x 818k / 10 s             = 8.2k reports/s in; gateway aggregates to at most 20k histograms/s (about 4 MB/s)
Join rate             : 818k / 2,400 s average stay           = 341 joins/s average (flash crowd of 5,000 in 60 s = 83/s)


**WebSocket gateways**

Connections           : about 0.82M room listeners + hosts + headroom = about 1M
Memory per socket     : about 35 KB (TLS + buffers in Node)  -> 50k sockets = 1.75 GB per pod (2 vCPU, 4 GB)
Pods                  : 60 (20 per region) = 3M socket capacity; nominal load 33%
After losing a region : 1M / 40 pods = 25k per pod = 50% utilization
Ping / pong           : 1M / 25 s = 40k/s total = about 670/s per pod


**Room owners**

Shards                : 4,096 logical shards; 24 owner pods = about 170 shards and about 8,300 rooms per pod
State per room        : about 5 KB average (queue up to 200 items x 150 B = 30 KB max)  -> 200k rooms = 1 GB total
Burst command load    : 18k events/s / 24 pods = 750 events/s per pod (very light)


**Redis hot state:** snapshot 5 KB x 200k = 1 GB, plus per-room ring of last 50 events (15 KB) = 3 GB; about 4 GB, doubled with replicas. Snapshot read bursts after a gateway crash (50k sockets over 10 s = 5k reads/s) are trivial for Redis; the real risk is TLS handshake CPU on gateways.

## 6. Single 5,000-listener room: pause

Payload (JSON)        : about 180 B; wire size with WS + TLS + TCP/IP about 270 B
Total egress          : 5,000 x 270 B = 1.35 MB
Publishes             : 1 (owner to Redis sharded pub/sub), 3 regions: 1 relay hop per remote region
Gateway subscribers   : 5,000 / 500 sockets per gateway = about 10 gateways (20 with regional spread)
Per-gateway writes    : about 250-500 sockets x 30-100 us = 8-50 ms, in 256-socket micro-batches, in parallel across gateways

Serial delivery from a single process would take 5,000 x 30-100 us = 150-500 ms and would break the budget. A hierarchical fan-out of 1 publish to about 10-20 gateways to 250-500 sockets each takes about 10-50 ms of write time per gateway, in parallel.

Hierarchical tier for rooms above about 20k listeners: owner (1) to regional relay (3) to fan-out nodes (up to 100 each) to gateways (up to 100 each) = 30,000 gateways of reach in theory, which is more than needed; two levels are sufficient.

## 7. Event streaming and analytics

Playback events (start, qualified at 30 s, end)  : 3 per play
  Peak    : 9,524 plays/s x 3 = 28.6k events/s
  Average : 3,704 plays/s x 3 = 11.1k events/s
Event size                                       : 350 B -> raw 960M events/day x 350 B = 336 GB/day
Telemetry beacons (batched every 60 s)            : 2M / 60 = 33k requests/s x about 500 B = 16.7 MB/s
room-events topic                                : 18k events/s x 300 B = 5.4 MB/s (x3 replication = 16 MB/s)
playback-events topic                            : 28.6k x 350 B = 10 MB/s peak (30 MB/s replicated)
Kafka retention (7 days, average rate)           : 3.9 MB/s x 604,800 s = 2.4 TB, x3 = 7 TB

ClickHouse   : 336 GB/day / 8.5 compression = about 40 GB/day; 13 months raw = about 16 TB; rollups kept longer
Listening history (ScyllaDB): 320M rows/day x 120 B = 38 GB/day = 14 TB/year; RF=3 = 42 TB/year
Royalty events: qualified streams <= 9.5k/s peak; stored immutably (Parquet in WORM object storage, about 100 B each = 80 GB/day compressed to about 15 GB)


## 8. Major bottlenecks and what they imply

1. **Concurrent sockets and memory per socket** (not bandwidth): drives gateway count and heap sizing. Load test at 1M held sockets.
2. **Reconnect storms**: 50k sockets over 10 s = 5k/s (single gateway), 1M over 60 s = 17k/s (region failure), 100k/s if unjittered. Needs jittered backoff, connection-rate limits, and snapshot caching per room version.
3. **Cold CDN misses for the long tail** and the object count (5.9B). Mitigate by prefetching next track, origin shield, and lazy encoding for tail renditions.
4. **Hot rooms**: write cost per fan-out, handled by hierarchical delivery and bounded queues.
5. **Kafka commit-path latency** (5-15 ms): acceptable inside the 200 ms budget, but the largest single contributor to command latency under load.
6. **Search at 30k QPS**: needs prefix caching and replicas; the index is small.
