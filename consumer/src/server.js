import http from "node:http";

import { Kafka, logLevel } from "kafkajs";
import { WebSocketServer } from "ws";

import { validateReading } from "./validate.js";

const bootstrap = process.env.KAFKA_BOOTSTRAP || "kafka:19092";
const topic = process.env.KAFKA_TOPIC || "market.tickers";
const port = Number(process.env.PORT || 8081);

const clients = new Set();

const server = http.createServer((request, response) => {
  if (request.url === "/health") {
    response.writeHead(200, { "content-type": "text/plain" });
    response.end("ok");
    return;
  }
  response.writeHead(404);
  response.end();
});

const sockets = new WebSocketServer({ server });

sockets.on("connection", (socket) => {
  clients.add(socket);
  socket.on("close", () => {
    clients.delete(socket);
  });
});

function broadcast(reading) {
  const payload = JSON.stringify({ type: "reading", reading });
  for (const socket of clients) {
    if (socket.readyState === socket.OPEN) {
      socket.send(payload);
    }
  }
}

const kafka = new Kafka({
  clientId: "panel-consumer",
  brokers: [bootstrap],
  logLevel: logLevel.WARN,
});

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function consume() {
  let delay = 1000;
  for (;;) {
    const attempt = kafka.consumer({ groupId: "panel" });
    try {
      await attempt.connect();
      await attempt.subscribe({ topic, fromBeginning: false });
      console.log(`[consumer] result=subscribed topic=${topic}`);
      await attempt.run({
        eachMessage: async ({ message }) => {
          if (!message.value) {
            console.warn("[consumer] result=discarded series=- reason=empty");
            return;
          }

          let reading;
          try {
            reading = JSON.parse(message.value.toString());
          } catch {
            console.warn("[consumer] result=discarded series=- reason=json");
            return;
          }

          const result = validateReading(reading);
          if (!result.ok) {
            const series = reading && typeof reading === "object" ? reading.series || "-" : "-";
            console.warn(`[consumer] result=discarded series=${series} reason=${result.reason}`);
            return;
          }

          broadcast(reading);
        },
      });
      return;
    } catch (error) {
      console.warn(`[consumer] result=retry delay=${delay} error=${error.message}`);
      try {
        await attempt.disconnect();
      } catch {
        // El cliente ya estaba cerrado.
      }
      await sleep(delay);
      delay = Math.min(delay * 2, 10000);
    }
  }
}

server.listen(port, "0.0.0.0", () => {
  console.log(`[consumer] result=health port=${port}`);
});

consume().catch((error) => {
  console.error(`[consumer] result=failed error=${error.message}`);
  process.exit(1);
});
