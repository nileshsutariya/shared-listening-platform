// Fan-out simulation: N virtual listeners in ONE room, host pauses, measure propagation to every listener.
// Single process over loopback. This demonstrates the protocol and fan-out loop only. It does NOT prove production scalability.
import { RoomServer } from '../src/server.js';
import { RoomClient } from '../src/client.js';

const N = Number(process.argv[2] ?? process.env.LISTENERS ?? 5000);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const pct = (a: number[], p: number) => a[Math.min(a.length - 1, Math.floor((p / 100) * a.length))];

const server = new RoomServer();
const port = await server.start(0);
const url = `ws://localhost:${port}`;

const host = new RoomClient('host', url); await host.connect(); await host.create();
await host.command({ type: 'QUEUE_ADD', commandId: 'a1', trackId: 'T1', durationMs: 600_000 });
await host.command({ type: 'PLAY', commandId: 'p1' });

console.log(`connecting ${N} listeners...`);
const listeners: RoomClient[] = [];
const t0 = performance.now();
for (let i = 0; i < N; i += 250) {
  const batch = await Promise.all(Array.from({ length: Math.min(250, N - i) }, async (_, k) => {
    const c = new RoomClient(`l${i + k}`, url); c.skew = ((i + k) % 41) * 25 - 500;   // inject +/- 500 ms local clock skew
    await c.connect(); await c.join(host.roomId, host.inviteToken); return c;
  }));
  listeners.push(...batch);
}
console.log(`joined in ${(performance.now() - t0).toFixed(0)} ms; room members=${server.rooms.get(host.roomId)!.members.size}`);

// clock sync for a sample so we can report drift under skew
const sample = listeners.filter((_, i) => i % Math.ceil(N / 200) === 0);
await Promise.all(sample.map((c) => c.syncClock(5)));

await sleep(1000);
const base = host.state!.version;
const sendAt = performance.now();
const arrivals: number[] = [];
const done = new Promise<void>((resolve) => {
  let got = 0;
  for (const c of listeners) {
    const orig = c.ws.listeners('message')[0] as (d: Buffer) => void;
    c.ws.on('message', () => {
      if (c.state && c.state.version > base && !c.appliedAt.has(-1)) {
        c.appliedAt.set(-1, 1); arrivals.push(performance.now() - sendAt);
        if (++got === listeners.length) resolve();
      }
    });
    void orig;
  }
});
const ack = host.command({ type: 'PAUSE', commandId: 'pause-1', expectedVersion: base });
const ackMs = await ack.then(() => performance.now() - sendAt);
await Promise.race([done, sleep(10_000)]);
arrivals.sort((a, b) => a - b);

console.log(`\nHost pause -> ${listeners.length} listeners`);
console.log(`host ack after durable commit (simulated): ${ackMs.toFixed(1)} ms (does not wait for listeners)`);
console.log(`delivered to ${arrivals.length}/${listeners.length}`);
console.log(`propagation ms  p50=${pct(arrivals, 50).toFixed(1)}  p95=${pct(arrivals, 95).toFixed(1)}  p99=${pct(arrivals, 99).toFixed(1)}  max=${arrivals[arrivals.length - 1].toFixed(1)}`);
console.log(`server fan-out loop max=${server.metrics.fanoutMsMax.toFixed(1)} ms, dropped frames=${server.metrics.droppedFrames}`);

const paused = listeners.filter((c) => c.state!.playback.status === 'paused' && c.state!.version === host.state!.version).length;
console.log(`listeners converged on host state: ${paused}/${listeners.length}`);
const drifts = sample.map((c) => Math.abs(c.serverNow() - Date.now()));
drifts.sort((a, b) => a - b);
console.log(`estimated-server-time error after clock sync (sample ${sample.length}, injected skew up to 500 ms): p50=${pct(drifts, 50).toFixed(1)} ms, p95=${pct(drifts, 95).toFixed(1)} ms`);
console.log('\nCaveat: one process, loopback, no real audio. Not evidence of production scale.');
for (const c of listeners) c.close(); host.close(); await server.stop(); process.exit(0);
