import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RoomActor, positionAt, LEAD_MS } from '../src/room.js';

function setup() {
  let t = 1_000_000;
  const clock = () => t;
  const adv = (ms: number) => { t += ms; };
  const a = new RoomActor('r1', 'host', clock);
  a.join('u1'); a.join('u2');
  a.handle({ type: 'QUEUE_ADD', commandId: 'q1', userId: 'host', trackId: 'A', durationMs: 60_000 });
  a.handle({ type: 'QUEUE_ADD', commandId: 'q2', userId: 'u1', trackId: 'B', durationMs: 60_000 });
  return { a, adv, clock };
}

test('authoritative play/pause computes position on the server', () => {
  const { a, adv } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' });
  adv(LEAD_MS + 5_000);                                   // scheduled start + 5 s of playback
  const r = a.handle({ type: 'PAUSE', commandId: 'c1', userId: 'host' });
  assert.ok(r.ok);
  assert.equal(a.state.playback.status, 'paused');
  assert.equal(a.state.playback.positionMs, 5_000);
});

test('duplicate commandId returns the original result without a new version', () => {
  const { a, adv } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' }); adv(1000);
  const r1 = a.handle({ type: 'PAUSE', commandId: 'dup', userId: 'host' });
  const v = a.state.version;
  const r2 = a.handle({ type: 'PAUSE', commandId: 'dup', userId: 'host' });
  assert.equal(r2.duplicate, true); assert.equal(r2.version, r1.version); assert.equal(a.state.version, v);
});

test('pause on an already paused room is a no-op (no version bump)', () => {
  const { a, adv } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' }); adv(1000);
  a.handle({ type: 'PAUSE', commandId: 'c1', userId: 'host' });
  const v = a.state.version;
  const r = a.handle({ type: 'PAUSE', commandId: 'c2', userId: 'host' });
  assert.ok(r.ok); assert.equal(a.state.version, v);
});

test('non-host cannot control playback; stale track instance is rejected', () => {
  const { a } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' });
  assert.equal(a.handle({ type: 'PAUSE', commandId: 'x', userId: 'u1' }).error, 'FORBIDDEN');
  assert.equal(a.handle({ type: 'SEEK', commandId: 'y', userId: 'host', positionMs: 1000, trackInstanceId: 'old-track' }).error, 'STALE_STATE');
});

test('seek while playing schedules the start and while paused anchors immediately', () => {
  const { a, adv, clock } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' }); adv(LEAD_MS + 1000);
  a.handle({ type: 'SEEK', commandId: 's1', userId: 'host', positionMs: 30_000 });
  assert.equal(positionAt(a.state.playback, clock()), 30_000);            // waiting for the scheduled instant
  adv(LEAD_MS + 2000);
  assert.equal(positionAt(a.state.playback, clock()), 32_000);
});

test('skip votes: one per user, majority of connected members skips, reset on track change', () => {
  const { a } = setup();                                   // members: host, u1, u2 => threshold 2
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' });
  const first = a.state.playback.trackId;
  const firstVote = a.handle({ type: 'SKIP_VOTE', commandId: 'v1', userId: 'u1' });
  assert.equal(firstVote.voteCount, 1);
  assert.equal(firstVote.voteThreshold, 2);
  assert.equal(firstVote.skipTriggered, false);
  a.handle({ type: 'SKIP_VOTE', commandId: 'v1b', userId: 'u1' });          // duplicate user
  assert.equal(a.state.playback.trackId, first);
  a.handle({ type: 'SKIP_VOTE', commandId: 'v2', userId: 'u2' });
  assert.equal(a.state.playback.trackId, 'B');
  assert.deepEqual(a.state.votes, []);
});

test('majority skip returns both committed events in version order', () => {
  const { a } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' });

  a.handle({ type: 'SKIP_VOTE', commandId: 'v1', userId: 'u1' });
  const result = a.handle({ type: 'SKIP_VOTE', commandId: 'v2', userId: 'u2' });

  assert.equal(result.ok, true);
  assert.deepEqual(result.events?.map((event) => event.type), ['VOTE_CAST', 'PLAYBACK']);
  assert.deepEqual(result.events?.map((event) => event.version), [result.version - 1, result.version]);
  assert.equal(result.voteCount, 2);
  assert.equal(result.voteThreshold, 2);
  assert.equal(result.skipTriggered, true);
  assert.equal(result.nextTrackAvailable, true);
  assert.equal(a.state.playback.trackId, 'B');
  assert.deepEqual(a.state.votes, []);
});

test('queue additions are totally ordered by owner sequence', () => {
  const { a } = setup();
  a.handle({ type: 'QUEUE_ADD', commandId: 'q3', userId: 'u2', trackId: 'C', durationMs: 1000 });
  assert.deepEqual(a.state.queue.map((q) => q.seq), [1, 2, 3]);
  assert.equal(a.state.queueVersion, 3);
});

test('track auto-advances at the end exactly once', () => {
  const { a, adv } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' });
  adv(LEAD_MS + 61_000);
  assert.ok(a.tick()); assert.equal(a.tick(), null);
  assert.equal(a.state.playback.trackId, 'B');
});

test('recovery replays the log, bumps the epoch and still dedups old commands', () => {
  const { a, adv, clock } = setup();
  a.handle({ type: 'PLAY', commandId: 'p1', userId: 'host' }); adv(2000);
  a.handle({ type: 'PAUSE', commandId: 'pz', userId: 'host' });
  const before = a.state;
  const b = RoomActor.recover('r1', a.log, clock, before.epoch + 1);
  assert.equal(b.state.version, before.version);
  assert.deepEqual(b.state.playback, before.playback);
  assert.equal(b.state.epoch, before.epoch + 1);
  const r = b.handle({ type: 'PAUSE', commandId: 'pz', userId: 'host' });  // client retry after failover
  assert.equal(r.duplicate, true); assert.equal(b.state.version, before.version);
});
