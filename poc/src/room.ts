// Room actor: single-writer, event-sourced, server-authoritative playback state.
// In production this runs inside a shard owner; here it is a plain class so it can be tested in isolation.

export type Status = 'idle' | 'playing' | 'paused';

export interface Playback {
  trackId: string | null;
  trackInstanceId: string | null;
  status: Status;
  positionMs: number;      // position at serverTimestamp
  serverTimestamp: number; // anchor; may be in the future (scheduled start)
}
export interface QueueItem { itemId: string; trackId: string; durationMs: number; addedBy: string; seq: number }

export interface RoomState {
  roomId: string; hostId: string; epoch: number;
  version: number; queueVersion: number; nextSeq: number;
  playback: Playback; queue: QueueItem[]; votes: string[];
}

export type RoomEvent =
  | { type: 'ROOM_CREATED'; version: number; epoch: number; commandId: string; serverTimestamp: number; hostId: string }
  | { type: 'QUEUE_ADDED'; version: number; epoch: number; commandId: string; serverTimestamp: number; item: QueueItem }
  | { type: 'PLAYBACK'; version: number; epoch: number; commandId: string; serverTimestamp: number;
      reason: 'play' | 'pause' | 'resume' | 'seek' | 'skip' | 'ended';
      playback: Playback; popQueue?: boolean; resetVotes?: boolean }
  | { type: 'VOTE_CAST'; version: number; epoch: number; commandId: string; serverTimestamp: number; userId: string };

export type Command =
  | { type: 'PLAY' | 'PAUSE' | 'RESUME' | 'SKIP'; commandId: string; userId: string; expectedVersion?: number; trackInstanceId?: string }
  | { type: 'SEEK'; commandId: string; userId: string; positionMs: number; expectedVersion?: number; trackInstanceId?: string }
  | { type: 'QUEUE_ADD'; commandId: string; userId: string; trackId: string; durationMs: number }
  | { type: 'SKIP_VOTE'; commandId: string; userId: string; trackInstanceId?: string };

export interface Result {
  ok: boolean; version: number; duplicate?: boolean; error?: string;
  event?: RoomEvent; events?: RoomEvent[];
  voteCount?: number; voteThreshold?: number; skipTriggered?: boolean;
  nextTrackAvailable?: boolean; alreadyVoted?: boolean;
}

export const LEAD_MS = 300;          // scheduled-start lead for play/resume/seek
export const DEDUP_WINDOW = 512;     // bounded dedup window
export const VOTE_RATIO = 0.5;       // strictly more than 50% of connected members

export function initialState(roomId: string, hostId: string, epoch = 1): RoomState {
  return {
    roomId, hostId, epoch, version: 0, queueVersion: 0, nextSeq: 1,
    playback: { trackId: null, trackInstanceId: null, status: 'idle', positionMs: 0, serverTimestamp: 0 },
    queue: [], votes: [],
  };
}

export function positionAt(p: Playback, serverNow: number): number {
  if (p.status !== 'playing') return p.positionMs;
  return p.positionMs + Math.max(0, serverNow - p.serverTimestamp); // anchor in the future => waiting to start
}

// Pure reducer: used by the owner, by recovery (replay) and by clients.
export function reduce(s: RoomState, e: RoomEvent): RoomState {
  const base = { ...s, version: e.version, epoch: Math.max(s.epoch, e.epoch) };
  switch (e.type) {
    case 'ROOM_CREATED': return { ...base, hostId: e.hostId };
    case 'QUEUE_ADDED': return { ...base, queue: [...s.queue, e.item], queueVersion: s.queueVersion + 1, nextSeq: e.item.seq + 1 };
    case 'VOTE_CAST': return { ...base, votes: s.votes.includes(e.userId) ? s.votes : [...s.votes, e.userId] };
    case 'PLAYBACK': return {
      ...base, playback: e.playback,
      queue: e.popQueue ? s.queue.slice(1) : s.queue,
      queueVersion: e.popQueue ? s.queueVersion + 1 : s.queueVersion,
      votes: e.resetVotes ? [] : s.votes,
    };
  }
}

export class RoomActor {
  state: RoomState;
  readonly log: RoomEvent[] = [];                       // stands in for the Kafka room log
  private dedup = new Map<string, Result>();            // commandId -> result (bounded, insertion ordered)
  readonly members = new Set<string>();                 // ephemeral presence (not in the log)
  private seqCounter = 0;

  constructor(roomId: string, hostId: string, private clock: () => number = Date.now, epoch = 1) {
    this.state = initialState(roomId, hostId, epoch);
    this.commit({ type: 'ROOM_CREATED', commandId: `create:${roomId}`, hostId });
    this.members.add(hostId);
  }

  // Recovery: rebuild state and the dedup window from a log. A new owner would use epoch + 1.
  static recover(roomId: string, log: RoomEvent[], clock: () => number = Date.now, newEpoch?: number): RoomActor {
    const a = Object.create(RoomActor.prototype) as RoomActor;
    let s = initialState(roomId, '', 0);
    for (const e of log) s = reduce(s, e);
    a.state = newEpoch ? { ...s, epoch: Math.max(newEpoch, s.epoch) } : s;
    (a as any).clock = clock; (a as any).dedup = new Map(); (a as any).members = new Set([s.hostId]);
    (a as any).seqCounter = 0;
    (a as any).log = [...log];
    for (const e of log.slice(-DEDUP_WINDOW)) (a as any).dedup.set(e.commandId, { ok: true, version: e.version, duplicate: true });
    return a;
  }

  private commit(partial: Record<string, unknown> & { type: RoomEvent['type']; commandId: string }): RoomEvent {
    const e = { ...partial, version: this.state.version + 1, epoch: this.state.epoch, serverTimestamp: this.clock() } as RoomEvent;
    this.state = reduce(this.state, e);   // production: append to log (acks=all) BEFORE applying and publishing
    this.log.push(e);
    return e;
  }

  private remember(commandId: string, r: Result) {
    this.dedup.set(commandId, r);
    if (this.dedup.size > DEDUP_WINDOW) this.dedup.delete(this.dedup.keys().next().value as string);
  }

  join(userId: string) { this.members.add(userId); }
  leave(userId: string) { this.members.delete(userId); }
  eventsSince(v: number): RoomEvent[] | null {         // bounded ring in production; null => send a snapshot
    if (v >= this.state.version) return [];
    const first = this.log[0]?.version ?? 1;
    return v + 1 >= first ? this.log.filter((e) => e.version > v) : null;
  }
  snapshot() { return { state: this.state, serverNow: this.clock() }; }
  voteCount() { return this.state.votes.filter((u) => this.members.has(u)).length; }
  voteThreshold() { return Math.floor(this.members.size * VOTE_RATIO) + 1; }

  handle(c: Command): Result {
    const prior = this.dedup.get(c.commandId);
    if (prior) return { ...prior, duplicate: true };
    const r = this.apply(c);
    if (r.ok || r.error === 'STALE_STATE' || r.error === 'FORBIDDEN')
      this.remember(c.commandId, { ...r, event: undefined, events: undefined });
    return r;
  }

  private fail(error: string): Result { return { ok: false, version: this.state.version, error }; }
  private done(e: RoomEvent): Result { return { ok: true, version: e.version, event: e }; }
  private noop(): Result { return { ok: true, version: this.state.version }; }

  private apply(c: Command): Result {
    const s = this.state, now = this.clock(), p = s.playback;
    const hostOnly = c.type === 'PLAY' || c.type === 'PAUSE' || c.type === 'RESUME' || c.type === 'SEEK' || c.type === 'SKIP';
    if (hostOnly && c.userId !== s.hostId) return this.fail('FORBIDDEN');

    // Soft version check: a stale expectedVersion is tolerated if the command still targets the current track instance.
    if ('trackInstanceId' in c && c.trackInstanceId && c.trackInstanceId !== p.trackInstanceId) return this.fail('STALE_STATE');
    if ('expectedVersion' in c && c.expectedVersion !== undefined && c.expectedVersion !== s.version && !('trackInstanceId' in c && c.trackInstanceId))
      return this.fail('STALE_STATE');

    switch (c.type) {
      case 'QUEUE_ADD': {
        if (s.queue.length >= 200) return this.fail('QUEUE_FULL');
        if (s.queue.filter((q) => q.addedBy === c.userId).length >= 5 && c.userId !== s.hostId) return this.fail('QUOTA');
        const item: QueueItem = { itemId: `q${s.nextSeq}`, trackId: c.trackId, durationMs: c.durationMs, addedBy: c.userId, seq: s.nextSeq };
        return this.done(this.commit({ type: 'QUEUE_ADDED', commandId: c.commandId, item }));
      }
      case 'PLAY': {
        if (p.status !== 'idle') return this.noop();
        const head = s.queue[0]; if (!head) return this.fail('EMPTY_QUEUE');
        return this.done(this.startTrack(c.commandId, head.trackId, now, 'play'));
      }
      case 'PAUSE': {
        if (p.status !== 'playing') return this.noop();                       // duplicate or redundant pause: no version bump
        const pos = Math.min(positionAt(p, now), this.currentDuration() ?? Infinity);
        return this.done(this.commit({ type: 'PLAYBACK', commandId: c.commandId, reason: 'pause',
          playback: { ...p, status: 'paused', positionMs: pos, serverTimestamp: now } }));
      }
      case 'RESUME': {
        if (p.status !== 'paused') return this.noop();
        return this.done(this.commit({ type: 'PLAYBACK', commandId: c.commandId, reason: 'resume',
          playback: { ...p, status: 'playing', serverTimestamp: now + LEAD_MS } }));
      }
      case 'SEEK': {
        if (p.status === 'idle') return this.fail('NOT_PLAYING');
        const pos = Math.max(0, Math.min(c.positionMs, this.currentDuration() ?? c.positionMs));
        return this.done(this.commit({ type: 'PLAYBACK', commandId: c.commandId, reason: 'seek',
          playback: { ...p, positionMs: pos, serverTimestamp: p.status === 'playing' ? now + LEAD_MS : now } }));
      }
      case 'SKIP': return this.done(this.advance(c.commandId, 'skip', now));
      case 'SKIP_VOTE': {
        if (!this.members.has(c.userId)) return this.fail('FORBIDDEN');
        if (p.status === 'idle') return this.fail('NOT_PLAYING');
        if (s.votes.includes(c.userId)) {
          return {
            ...this.noop(), voteCount: this.voteCount(),
            voteThreshold: this.voteThreshold(), alreadyVoted: true,
          };
        }
        const e = this.commit({ type: 'VOTE_CAST', commandId: c.commandId, userId: c.userId });
        const voteCount = this.voteCount();
        const voteThreshold = this.voteThreshold();
        if (voteCount >= voteThreshold) {
          const nextTrackAvailable = !!s.queue[1];
          const skipped = this.advance(`${c.commandId}:skip`, 'skip', now);
          return {
            ok: true, version: skipped.version, events: [e, skipped],
            voteCount, voteThreshold, skipTriggered: true, nextTrackAvailable,
          };
        }
        return { ...this.done(e), voteCount, voteThreshold, skipTriggered: false };
      }
    }
  }

  private currentDuration(): number | undefined {
    // duration of the playing track is kept on the queue head item while it plays (head = current track)
    return this.state.queue[0]?.durationMs;
  }

  private startTrack(commandId: string, trackId: string, now: number, reason: 'play' | 'skip' | 'ended'): RoomEvent {
    const ti = `${this.state.roomId}:${++this.seqCounter}:${this.state.version + 1}`;
    return this.commit({ type: 'PLAYBACK', commandId, reason,
      playback: { trackId, trackInstanceId: ti, status: 'playing', positionMs: 0, serverTimestamp: now + LEAD_MS }, resetVotes: true });
  }

  // Move to the next queue item (queue head is the current track; popping it exposes the next).
  private advance(commandId: string, reason: 'skip' | 'ended', now: number): RoomEvent {
    const next = this.state.queue[1];
    if (!next) {
      return this.commit({ type: 'PLAYBACK', commandId, reason, popQueue: true, resetVotes: true,
        playback: { trackId: null, trackInstanceId: null, status: 'idle', positionMs: 0, serverTimestamp: now } });
    }
    const ti = `${this.state.roomId}:${++this.seqCounter}:${this.state.version + 1}`;
    return this.commit({ type: 'PLAYBACK', commandId, reason, popQueue: true, resetVotes: true,
      playback: { trackId: next.trackId, trackInstanceId: ti, status: 'playing', positionMs: 0, serverTimestamp: now + LEAD_MS } });
  }

  // Called periodically by the owner loop: auto-advance at track end (deterministic commandId => idempotent).
  tick(): RoomEvent | null {
    const p = this.state.playback, now = this.clock();
    const dur = this.currentDuration();
    if (p.status === 'playing' && dur !== undefined && positionAt(p, now) >= dur) {
      const cid = `auto:${p.trackInstanceId}`;
      if (this.dedup.has(cid)) return null;
      const e = this.advance(cid, 'ended', now);
      this.remember(cid, { ok: true, version: e.version });
      return e;
    }
    return null;
  }
}
