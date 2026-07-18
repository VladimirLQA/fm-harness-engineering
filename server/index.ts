import './env.ts';
import { DBOS } from '@dbos-inc/dbos-sdk';
import { ensureSchema, subscribe, history, runAgentWorkflow } from 'harness';
import express from 'express';
import { createServer } from 'node:http';
import { WebSocketServer, type WebSocket } from 'ws';
import { type ClientMessage } from '@shared/events';

const PORT = Number(process.env.PORT ?? 8787);

async function main() {
  await ensureSchema();
  DBOS.setConfig({
    name: 'harness',
    systemDatabaseUrl: process.env.DATABASE_URL,
  });
  await DBOS.launch();

  const app = express();
  app.get('/health', (_req, res) => {
    res.json({ ok: true });
  });
  const server = createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });

  subscribe((event) => {
    const data = JSON.stringify(event);
    for (const client of wss.clients) {
      if (client.readyState === client.OPEN) client.send(data);
    }
  });

  wss.on('connection', async (socket: WebSocket) => {
    socket.on('message', async (raw) => {
      let message: ClientMessage;
      try {
        message = JSON.parse(raw.toString());
      } catch {
        return; // ignore anything that isn't valid JSON
      }

      if (message.type === 'submit_task') {
        await DBOS.startWorkflow(runAgentWorkflow)({
          input: message.input,
        });
      }

      for (const event of await history()) socket.send(JSON.stringify(event));
    });
  });

  server.listen(PORT, () => {
    console.log(
      `harness server listening on http://localhost:${PORT}  (ws: /ws)`
    );
  });
}

main().catch(() => {
  console.error('something went wrong...');
  process.exit(1);
});
