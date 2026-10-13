// Virtual listener/host client: clock-offset estimation, versioned event application, resync, expected position, drift.
import WebSocket from 'ws';
import { reduce, positionAt, type RoomEvent, type RoomState, type Command } from './room.js';

type DOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

export class RoomClient {
  ws!: WebSocket;
  state?: RoomState;
  roomId = '';
  inviteToken = '';
  offsetMs = 0;                       // serverNow ~= localNow + offsetMs
  private samples: { rtt: number; offset: number }[] = [];
  resyncs = 0;
  appliedAt = new Map<number, number>();   // version -> local arrival time (for propagation measurements)
  private waiters: ((m: any) => boolean)[] = [];
  private acks = new Map<string, (m: any) => void>();
  skew = 0;                           // injected local clock skew for tests

  constructor(public userId: string, private url: string) {}

  now() { return Date.now() + this.skew; }
  serverNow() { return this.now() + this.offsetMs; }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);
      this.ws.on('open', () => resolve());
      this.ws.on('error', reject);
      this.ws.on('message', (d) => this.onMessage(JSON.parse(d.toString())));
    });
  }

  private onMessage(m: any) {
    switch (m.t) {
      case 'SNAPSHOT': this.state = m.state; this.roomId = m.roomId; if (m.inviteToken) this.inviteToken = m.inviteToken; break;
      case 'EVENTS': for (const e of m.events as RoomEvent[]) this.applyEvent(e); break;
      case 'EVENT': this.applyEvent(m.e); break;
      case 'TIME_REPLY': {
        const t3 = this.now();
        const rtt = t3 - m.t0 - (m.t2 - m.t1);
        const offset = ((m.t1 - m.t0) + (m.t2 - t3)) / 2;
        this.samples.push({ rtt, offset }); if (this.samples.length > 8) this.samples.shift();
        const best = this.samples.reduce((a, b) => (b.rtt < a.rtt ? b : a));   // min-RTT sample
        this.offsetMs = best.offset;
        break;
      }
      case 'ACK': this.acks.get(m.commandId)?.(m); this.acks.delete(m.commandId); break;
    }
    this.waiters = this.waiters.filter((w) => !w(m));
  }

  private applyEvent(e: RoomEvent) {
    if (!this.state) return;
    if (e.epoch < this.state.epoch) return;                    // stale owner: drop
    if (e.version <= this.state.version) return;              // duplicate or old
    if (e.version !== this.state.version + 1) {                // gap: resync from last known version
      this.resyncs++;
      this.ws.send(JSON.stringify({ t: 'JOIN', roomId: this.roomId, inviteToken: this.inviteToken, userId: this.userId, lastVersion: this.state.version }));
      return;
    }
    this.state = reduce(this.state, e);
    this.appliedAt.set(e.version, this.now());
  }

  async syncClock(n = 5) { for (let i = 0; i < n; i++) { this.ws.send(JSON.stringify({ t: 'TIME', t0: this.now() })); await this.until((m) => m.t === 'TIME_REPLY'); } }
  until(pred: (m: any) => boolean, ms = 3000): Promise<any> {
    return new Promise((resolve, reject) => {
      const to = setTimeout(() => reject(new Error('timeout')), ms);
      this.waiters.push((m) => { if (pred(m)) { clearTimeout(to); resolve(m); return true; } return false; });
    });
  }
  async create() { this.ws.send(JSON.stringify({ t: 'CREATE', userId: this.userId })); await this.until((m) => m.t === 'SNAPSHOT'); }
  async join(roomId: string, inviteToken: string, lastVersion?: number) {
    this.roomId = roomId; this.inviteToken = inviteToken;
    this.ws.send(JSON.stringify({ t: 'JOIN', roomId, inviteToken, userId: this.userId, lastVersion }));
    await this.until((m) => m.t === 'SNAPSHOT' || m.t === 'EVENTS' || m.t === 'ERROR');
  }
  command(cmd: DOmit<Command, 'userId'>): Promise<any> {
    const p = new Promise<any>((resolve) => this.acks.set(cmd.commandId, resolve));
    this.ws.send(JSON.stringify({ t: 'CMD', cmd }));
    return p;
  }
  expectedPositionMs() { return this.state ? positionAt(this.state.playback, this.serverNow()) : 0; }
  /** drift = where my player is minus where the room says it should be (player position supplied by the caller) */
  driftMs(playerPositionMs: number) { return playerPositionMs - this.expectedPositionMs(); }
  close() { this.ws.close(); }
}
