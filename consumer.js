const amqp = require("amqplib");
const mongoose = require("mongoose");

const RABBITMQ_URL = "amqp://localhost";
const QUEUE = "messages";
const MONGO_URI = "mongodb://localhost:27017/rabbitmq_example";

mongoose
  .connect(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => {
    console.log("connected to mongodb");
  })
  .catch((e) => {
    console.log(e);
  });
const messageSchema = new mongoose.Schema({
  id: { type: String, required: true },
  name: { type: String, required: true },
  email: { type: String, required: true },
  content: { type: String, required: true },
  metadata: { type: Object, required: true },
  timestamp: { type: Date, required: true },
});
const Message = mongoose.model("Message", messageSchema);
async function consumeMessages() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  await channel.assertQueue(QUEUE);
  console.log(`Wating for messages in Queue`);
  channel.consume(QUEUE, async (msg) => {
    if (msg != null) {
      const messageContent = JSON.parse(msg.content.toString());
      console.log("Message received:", messageContent);
      try {
        const savedMessage = await Message.create({
          ...messageContent,
          metadata: {
            source: "Rabbit MQ",
            priority: "High",
          },
        });
        console.log(savedMessage);
      } catch (error) {
        console.log(error);
      }
      channel.ack(msg);
    }
  });
}
consumeMessages().catch(console.error);
