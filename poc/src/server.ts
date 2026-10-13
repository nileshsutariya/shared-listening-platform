// Single-process WebSocket server: creates rooms, joins, routes commands to the room actor and fans events out.
// Stands in for: gateway fleet + room owner + Redis pub/sub + Kafka log. Auth is stubbed (see signInvite).
import { WebSocketServer, WebSocket } from 'ws';
import { createHmac, randomUUID } from 'node:crypto';
import { createServer, type Server } from 'node:http';
import { readFile } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve as resolvePath } from 'node:path';
import { RoomActor, type Command, type RoomEvent } from './room.js';

const SECRET = 'dev-only-secret';
const BATCH = 256;               // sockets written per micro-batch
const MAX_BUFFERED = 64 * 1024;  // per-socket send-buffer cap (bounded queue)
const PUBLIC_DIR = resolvePath(dirname(fileURLToPath(import.meta.url)), '../public');
const STATIC_FILES: Record<string, { file: string; type: string }> = {
  '/': { file: 'index.html', type: 'text/html; charset=utf-8' },
  '/index.html': { file: 'index.html', type: 'text/html; charset=utf-8' },
  '/styles.css': { file: 'styles.css', type: 'text/css; charset=utf-8' },
  '/app.js': { file: 'app.js', type: 'text/javascript; charset=utf-8' },
};

export const signInvite = (roomId: string) => createHmac('sha256', SECRET).update(roomId).digest('hex').slice(0, 24);

interface Conn { ws: WebSocket; userId?: string; roomId?: string; stale?: boolean }

export class RoomServer {
  rooms = new Map<string, RoomActor>();
  sockets = new Map<string, Set<Conn>>();
  metrics = { commands: 0, duplicates: 0, rejected: 0, droppedFrames: 0, fanoutMsMax: 0 };
  private wss!: WebSocketServer;
  private httpServer?: Server;
  private timer?: NodeJS.Timeout;
  port = 0;

  constructor(private clock: () => number = Date.now) {}

  start(port = 0): Promise<number> {
    return new Promise((resolve, reject) => {
      this.httpServer = createServer((req, res) => {
        const asset = STATIC_FILES[new URL(req.url ?? '/', 'http://localhost').pathname];
        if (!asset) {
          res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Not found');
          return;
        }
        readFile(resolvePath(PUBLIC_DIR, asset.file), (error, content) => {
          if (error) {
            res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Unable to load UI asset');
            return;
          }
          res.writeHead(200, { 'Content-Type': asset.type, 'Cache-Control': 'no-cache' }).end(content);
        });
      });
      this.httpServer.once('error', reject);
      this.httpServer.listen(port, () => {
        this.wss = new WebSocketServer({ server: this.httpServer!, maxPayload: 4096 });
        this.wss.on('connection', (ws) => {
          const conn: Conn = { ws };
          ws.on('message', (data) => this.onMessage(conn, data.toString()));
          ws.on('close', () => this.onClose(conn));
          ws.on('error', () => ws.terminate());
        });
        this.port = (this.httpServer!.address() as { port: number }).port;
        this.timer = setInterval(() => this.tickAll(), 250); // owner loop: auto-advance at track end
        resolve(this.port);
      });
    });
  }

  async stop() {
    clearInterval(this.timer);
    if (!this.httpServer) return;
    for (const c of this.wss.clients) c.terminate();
    await new Promise<void>((resolve) => this.wss.close(() => this.httpServer!.close(() => resolve())));
  }

  // Simulates an owner crash + takeover: drop in-memory actors, rebuild them from the (durable) logs with epoch + 1.
  failoverOwner() {
    for (const [id, a] of this.rooms) this.rooms.set(id, RoomActor.recover(id, a.log, this.clock, a.state.epoch + 1));
  }

  private send(ws: WebSocket, msg: unknown) { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg)); }

  private onMessage(conn: Conn, raw: string) {
    let m: any; try { m = JSON.parse(raw); } catch { return conn.ws.close(4400, 'bad json'); }
    switch (m.t) {
      case 'TIME': return this.send(conn.ws, { t: 'TIME_REPLY', t0: m.t0, t1: this.clock(), t2: this.clock() });
      case 'CREATE': {
        const roomId = `rm_${randomUUID().slice(0, 8)}`;
        const actor = new RoomActor(roomId, m.userId, this.clock);
        this.rooms.set(roomId, actor);
        return this.attach(conn, roomId, m.userId, undefined, signInvite(roomId));
      }
      case 'JOIN': {
        if (m.inviteToken !== signInvite(m.roomId)) return this.send(conn.ws, { t: 'ERROR', error: 'BAD_INVITE' });
        if (!this.rooms.has(m.roomId)) return this.send(conn.ws, { t: 'ERROR', error: 'NO_ROOM' });
        return this.attach(conn, m.roomId, m.userId, m.lastVersion);
      }
      case 'CMD': return this.onCommand(conn, m.cmd);
      case 'DRIFT': return; // production: aggregate into per-room histograms, sampled
    }
  }

  private attach(conn: Conn, roomId: string, userId: string, lastVersion?: number, inviteToken?: string) {
    const actor = this.rooms.get(roomId)!;
    conn.roomId = roomId; conn.userId = userId;
    if (!this.sockets.has(roomId)) this.sockets.set(roomId, new Set());
    this.sockets.get(roomId)!.add(conn);
    actor.join(userId);
    const missed = lastVersion !== undefined ? actor.eventsSince(lastVersion) : null;
    if (missed) this.send(conn.ws, { t: 'EVENTS', events: missed, serverNow: this.clock(), roomId });          // RESUME path
    else this.send(conn.ws, { t: 'SNAPSHOT', ...actor.snapshot(), roomId, inviteToken });                       // late join / big gap
  }

  private onCommand(conn: Conn, cmd: Command) {
    if (!conn.roomId || !conn.userId) return;
    const actor = this.rooms.get(conn.roomId)!;
    this.metrics.commands++;
    const r = actor.handle({ ...cmd, userId: conn.userId } as Command);   // userId comes from the session, never from the client
    if (r.duplicate) this.metrics.duplicates++;
    if (!r.ok) this.metrics.rejected++;
    // Ack the sender right after the (simulated) commit. Never wait for listeners.
    this.send(conn.ws, {
      t: 'ACK', commandId: cmd.commandId, ok: r.ok, version: r.version,
      duplicate: !!r.duplicate, error: r.error,
      voteCount: r.voteCount, voteThreshold: r.voteThreshold,
      skipTriggered: r.skipTriggered, nextTrackAvailable: r.nextTrackAvailable,
      alreadyVoted: r.alreadyVoted,
    });
    if (r.ok) {
      for (const event of r.events ?? (r.event ? [r.event] : [])) this.fanOut(conn.roomId, event);
    }
  }

  private tickAll() {
    for (const [roomId, actor] of this.rooms) { const e = actor.tick(); if (e) this.fanOut(roomId, e); }
  }

  fanOut(roomId: string, event: RoomEvent) {
    const frame = JSON.stringify({ t: 'EVENT', e: event });        // serialize ONCE
    const conns = [...(this.sockets.get(roomId) ?? [])];
    const t0 = performance.now();
    let i = 0;
    const step = () => {                                          // micro-batches so one huge room cannot starve the loop
      const end = Math.min(i + BATCH, conns.length);
      for (; i < end; i++) {
        const c = conns[i];
        if (c.ws.readyState !== c.ws.OPEN) continue;
        if (c.ws.bufferedAmount > MAX_BUFFERED) { c.stale = true; this.metrics.droppedFrames++; continue; } // clients resync by version gap
        c.ws.send(frame);
      }
      if (i < conns.length) setImmediate(step);
      else this.metrics.fanoutMsMax = Math.max(this.metrics.fanoutMsMax, performance.now() - t0);
    };
    step();
  }

  private onClose(conn: Conn) {
    if (!conn.roomId) return;
    this.sockets.get(conn.roomId)?.delete(conn);
    // leave only if the user has no other live socket in this room
    const still = [...(this.sockets.get(conn.roomId) ?? [])].some((c) => c.userId === conn.userId);
    if (!still) this.rooms.get(conn.roomId)?.leave(conn.userId!);
  }
}
