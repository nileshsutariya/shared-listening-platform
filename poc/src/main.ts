import { RoomServer } from './server.js';
const port = Number(process.env.PORT ?? 8080);
const server = new RoomServer();
server.start(port).then((p) => console.log(`Listen Together PoC UI on http://localhost:${p} (WebSocket: ws://localhost:${p})`));
