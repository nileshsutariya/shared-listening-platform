import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { RoomServer } from '../src/server.js';
import { RoomClient } from '../src/client.js';

let server: RoomServer; let url: string;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
before(async () => { server = new RoomServer(); const port = await server.start(0); url = `ws://localhost:${port}`; });
after(async () => { await server.stop(); });

async function room() {
  const host = new RoomClient('host', url); await host.connect(); await host.create(); await host.syncClock();
  return host;
}
async function listener(h: RoomClient, id: string) {
  const c = new RoomClient(id, url); await c.connect(); await c.join(h.roomId, h.inviteToken); await c.syncClock(); return c;
}

test('create, join, play, pause: all clients converge on the same versioned state', async () => {
  const host = await room(); const l1 = await listener(host, 'l1'); const l2 = await listener(host, 'l2');
  await host.command({ type: 'QUEUE_ADD', commandId: 'a1', trackId: 'T1', durationMs: 120_000 });
  await host.command({ type: 'PLAY', commandId: 'p1' });
  await sleep(700);
  await host.command({ type: 'PAUSE', commandId: 'z1' });
  await sleep(100);
  for (const c of [host, l1, l2]) {
    assert.equal(c.state!.playback.status, 'paused');
    assert.equal(c.state!.version, host.state!.version);
  }
  assert.equal(l1.state!.playback.positionMs, l2.state!.playback.positionMs);
  [host, l1, l2].forEach((c) => c.close());
});

test('late join gets a snapshot and computes the current position', async () => {
  const host = await room();
  await host.command({ type: 'QUEUE_ADD', commandId: 'a1', trackId: 'T1', durationMs: 120_000 });
  await host.command({ type: 'PLAY', commandId: 'p1' });
  await sleep(1000);
  const late = await listener(host, 'late');
  const drift = Math.abs(late.expectedPositionMs() - host.expectedPositionMs());
  assert.ok(drift < 100, `late-join drift ${drift} ms`);
  assert.ok(late.expectedPositionMs() > 400);              // really mid-track
  host.close(); late.close();
});

test('duplicate command over the wire is deduplicated', async () => {
  const host = await room();
  await host.command({ type: 'QUEUE_ADD', commandId: 'a1', trackId: 'T1', durationMs: 120_000 });
  await host.command({ type: 'PLAY', commandId: 'p1' });
  await sleep(400);
  const a1 = await host.command({ type: 'SEEK', commandId: 'seek1', positionMs: 50_000 });
  const a2 = await host.command({ type: 'SEEK', commandId: 'seek1', positionMs: 50_000 });
  assert.equal(a2.duplicate, true); assert.equal(a2.version, a1.version);
  host.close();
});

test('majority skip fans out every committed event without causing a version-gap resync', async () => {
  const host = await room();
  const l1 = await listener(host, 'voter-1');
  const l2 = await listener(host, 'voter-2');
  const clients = [host, l1, l2];

  await host.command({ type: 'QUEUE_ADD', commandId: 'queue-current', trackId: 'T1', durationMs: 120_000 });
  await host.command({ type: 'QUEUE_ADD', commandId: 'queue-next', trackId: 'T2', durationMs: 120_000 });
  await host.command({ type: 'PLAY', commandId: 'play-vote-room' });
  await l1.command({ type: 'SKIP_VOTE', commandId: 'vote-1' });
  const result = await l2.command({ type: 'SKIP_VOTE', commandId: 'vote-2' });

  for (let attempt = 0; attempt < 50 && clients.some((c) => c.state!.version < result.version); attempt++) {
    await sleep(10);
  }

  for (const client of clients) {
    assert.equal(client.state!.version, result.version);
    assert.equal(client.state!.playback.trackId, 'T2');
    assert.deepEqual(client.state!.votes, []);
    assert.equal(client.resyncs, 0);
  }
  clients.forEach((c) => c.close());
});

test('skip-vote acknowledgements show progress and explain when there is no next track', async () => {
  const host = await room();
  const voter = await listener(host, 'voter');
  await host.command({ type: 'QUEUE_ADD', commandId: 'only-track', trackId: 'T1', durationMs: 120_000 });
  await host.command({ type: 'PLAY', commandId: 'play-only-track' });

  const firstVote = await voter.command({ type: 'SKIP_VOTE', commandId: 'first-vote' });
  assert.equal(firstVote.voteCount, 1);
  assert.equal(firstVote.voteThreshold, 2);
  assert.equal(firstVote.skipTriggered, false);

  const finalVote = await host.command({ type: 'SKIP_VOTE', commandId: 'final-vote' });
  assert.equal(finalVote.voteCount, 2);
  assert.equal(finalVote.skipTriggered, true);
  assert.equal(finalVote.nextTrackAvailable, false);
  host.close();
  voter.close();
});

test('reconnect with lastVersion catches up from missed events', async () => {
  const host = await room(); const l = await listener(host, 'flaky');
  await host.command({ type: 'QUEUE_ADD', commandId: 'a1', trackId: 'T1', durationMs: 120_000 });
  const lastSeen = l.state!.version; l.close(); await sleep(50);
  await host.command({ type: 'PLAY', commandId: 'p1' });
  await host.command({ type: 'SEEK', commandId: 's1', positionMs: 10_000 });
  const back = new RoomClient('flaky', url); await back.connect();
  back.state = { ...l.state!, version: lastSeen };          // client kept its last state across the reconnect
  await back.join(host.roomId, host.inviteToken, lastSeen); await sleep(50);
  assert.equal(back.state!.version, host.state!.version);
  assert.deepEqual(back.state!.playback, host.state!.playback);
  host.close(); back.close();
});

test('owner failover: state is recovered from the log and a command retry is not double-applied', async () => {
  const host = await room();
  await host.command({ type: 'QUEUE_ADD', commandId: 'a1', trackId: 'T1', durationMs: 120_000 });
  await host.command({ type: 'PLAY', commandId: 'p1' });
  await sleep(300);
  const first = await host.command({ type: 'PAUSE', commandId: 'pz' });
  server.failoverOwner();                                   // new owner, epoch + 1, rebuilt from log
  const retry = await host.command({ type: 'PAUSE', commandId: 'pz' });
  assert.equal(retry.duplicate, true); assert.equal(retry.version, first.version);
  const after = await host.command({ type: 'RESUME', commandId: 'r1' });
  assert.ok(after.ok);
  await sleep(100);
  assert.equal(host.state!.epoch, 2);                       // events from the new epoch are applied by clients
  assert.equal(host.state!.playback.status, 'playing');
  host.close();
});

test('invite token is required to join', async () => {
  const host = await room();
  const c = new RoomClient('intruder', url); await c.connect();
  c.ws.send(JSON.stringify({ t: 'JOIN', roomId: host.roomId, inviteToken: 'wrong', userId: 'intruder' }));
  const m = await c.until((x) => x.t === 'ERROR');
  assert.equal(m.error, 'BAD_INVITE');
  host.close(); c.close();
});
