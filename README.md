# Listen Together: Music Streaming Platform Design

A system-design deliverable for a Spotify-style streaming service with a synchronized **Listen Together** room feature (2 to 5,000 listeners, about 200 ms target sync). The deliverable is primarily architecture and documentation. The small TypeScript proof of concept only demonstrates the room-synchronization protocol.

## Contents

| File                 | Purpose |
|----------------------|---|
| `design-document.md` | Concise 3-6 page design document. The required diagrams are separate Mermaid deliverables linked from the document. **Start here.** |
| `architecture.mmd`   | Mermaid architecture diagram (control plane vs audio data plane, multi-region) |
| `host-pause-sequence.mmd` | Mermaid sequence diagram: host pauses a room of 5,000 listeners, with failure branches |
| `capacity-estimates.md` | Assumptions and calculations for traffic, bandwidth, storage, event rates, fan-out |
| `docs/detailed-design-appendix.md` | Full detail not limited by page count: audio pipeline, catalogue and search schema, room lifecycle and protocol, storage table and event schema, all 12 failure scenarios, security, observability and SLOs, roadmap details |
| `poc/` | Small TypeScript, Node.js, WebSocket proof of concept with tests and a 5,000-listener load simulation |

## Design in brief

- **Audio** is delivered as HLS (CMAF fMP4) from object storage through a CDN. WebSockets carry only control messages.
- **Rooms** use a server-authoritative playback anchor `{trackId, status, positionMs, serverTimestamp, stateVersion}`. Clients compute the current position from an estimated server clock, correct drift with small playback-rate nudges, and hard-seek above 300 ms.
- **Ownership:** each room has exactly one writer (room owner process) per shard, protected by etcd leases, fencing epochs and Kafka producer fencing. The Kafka room log is the source of truth; Redis is a rebuildable cache and fan-out bus.
- **Fan-out:** one publish per command, delivered to about 10-20 gateways that write a pre-serialized frame to a few hundred sockets each. The host is acked after the durable commit, never after listener acks. Telemetry is sampled and aggregated separately.
- **Events** for history, analytics and royalties flow through Kafka with idempotent IDs and never block playback.

## Key assumptions (details in `capacity-estimates.md`)

20M MAU, 8M DAU, 40 plays/DAU/day, 210 s tracks, 138 kbps average stream, 2M peak concurrent listeners, about 300 Gbps peak egress, 50M tracks (about 1.9 PB logical storage), 200k rooms holding about 0.82M listeners, three regions.

## Honest limits

- **200 ms sync is a target**, not a guarantee. Bluetooth output latency, OS scheduling, network jitter and buffering can exceed it.
- **Active rooms do not survive a regional outage seamlessly.** Room control is interrupted for minutes and the most recent seconds of commands may be lost (asynchronous cross-region replication). Audio already buffered keeps playing briefly.
- **Estimates are not measurements.** All numbers are derived from stated assumptions and must be validated by load tests.
- **The design brief is intentionally concise.** The architecture and pause-sequence diagrams are separate deliverables; supporting detail is in the appendix.
- **The PoC is a single process over loopback.** It proves protocol behavior, not scale. Auth is stubbed (HMAC invite token, user ID supplied by the client at join), there is no real audio, and the "durable log" is an in-memory array.

## Proof of concept

Requirements: Node.js 20+ (developed on Node 22).

```bash
cd poc
npm install
npm start                # open http://localhost:8080 for the browser demo
npm test                 # 18 tests: room reducer + WebSocket integration
ulimit -n 20000          # needed for 5,000 clients in one process (10,000 sockets)
npm run loadtest -- 5000 # 5,000 virtual listeners, host pause, propagation stats
```

The browser demo is served by the same process at `http://localhost:8080`. Enter a name to create a room, use **Invite friends** to share its link, and let participants join with that link. For synchronized audio, add a public direct MP3 URL; every listener clicks **Enable synchronized media** once if their browser requires an autoplay gesture. Clients estimate server-clock offset, schedule play at the room anchor, and correct drift during playback. Direct MP4 is also supported as synchronized video. These are best-effort browser synchronizations, not a hard 200 ms guarantee. Skip-vote acknowledgements show the current vote count and threshold; a passed vote with no queued next track pauses the room and tells the users to add another track. YouTube videos use YouTube's visible official player, and Spotify track/episode links use Spotify's embed; their playback controls are provider-owned and are not synchronized by this PoC. The app does not extract audio from arbitrary websites. Provider, account, region and browser restrictions apply. Set the queue duration to the media length for accurate server-side automatic track advance.

The WebSocket endpoint remains available at `ws://localhost:8080`. What the protocol PoC demonstrates: room creation with a signed invite token; join; authoritative play, pause, resume and seek with scheduled starts; versioned snapshots; late-join sync; duplicate-command protection; reconnect with `lastVersion` catch-up; simulated owner failover (rebuilt from the log with `epoch + 1`, retried commands deduplicated); queue ordering; skip voting; micro-batched fan-out with bounded send buffers; clock-offset estimation under injected skew.

Sample result on the development machine (not a benchmark): 5,000 listeners in one room, host ack 25 ms, all 5,000 listeners received the pause (p50 96 ms, p99 170 ms, measured in the same process as the server, so the clients' own work is included). All converged on the same state version.

## Assignment coverage

| Assignment section | Where |
|---|---|
| 1-2 Overview, scale, assumptions | `design-document.md` s1, `capacity-estimates.md` |
| 3 Technology stack | `design-document.md` s6 |
| 4 Audio delivery | `docs/detailed-design-appendix.md` s1 (flow, HLS vs DASH, WebSocket rationale) |
| 5 Catalogue and search | appendix s2 |
| 6 Listen Together | `design-document.md` s7, appendix s3 |
| 7 Storage | appendix s4 (table, event schema, idempotency, reconciliation) |
| 8 Reliability | `design-document.md` s9, appendix s5 (all 12 scenarios) |
| 9 Security | `design-document.md` s10, appendix s6 |
| 10 Observability and SLOs | `design-document.md` s10, appendix s7 |
| 11 Architecture diagram | `architecture.mmd` |
| 12 Sequence diagram | `host-pause-sequence.mmd` |
| 13 Design document | `design-document.md` |
| 14 Roadmap | `design-document.md` s12, appendix s8 |
| 15 Proof of concept | `poc/` |
