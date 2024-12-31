const amqp = require("amqplib");
const express = require("express");
const bodyParser = require("body-parser");
const { faker } = require("@faker-js/faker");

const app = express();
app.use(bodyParser.json());
const RABBITMQ_URL = "amqp://localhost";
const QUEUE = "messages";

let connection;
let channel;

function generateFakeMessage() {
  return {
    id: faker.string.uuid(),
    name: faker.person.fullName(),
    email: faker.internet.email(),
    content: faker.lorem.sentence(),
    timestamp: new Date().toDateString(),
  };
}

async function initRabbitMQ() {
  try {
    console.log("connecting to Rabit");
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE);
    console.log("connected");
  } catch (error) {
    console.log("error", error);
    process.exit(1);
  }
}

async function sendMessage(message) {
  try {
    if (!channel) {
      throw new Error("channel is not initialized");
    }
    channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(message)));
    console.log(`Message sent:${JSON.stringify(message)}`);
  } catch (error) {
    console.log(error);
  }
}
function startAutoProducer() {
  console.log("Starting auto producer");
  setInterval(async () => {
    const fakeMessage = generateFakeMessage();
    await sendMessage(fakeMessage);
    console.log("1");
  }, 5000);
}

async function closeRabbitMQ() {
  try {
    console.log("Closing RabbitMQ connection ... ");
    if (channel) await channel.close();
    if (connection) await connection.close();
    console.log("RabbitMQ connection closed");
  } catch (error) {
    console.log(error);
  }
}
process.on("SIGINT", async () => {
  await closeRabbitMQ();
  process.exit(0);
});

const PORT = 3000;
app.listen(PORT, async () => {
  console.log(`running on ${PORT}`);
  await initRabbitMQ();
  startAutoProducer();
});