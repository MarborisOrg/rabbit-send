import { Core } from "@marboris/coreutils";

const EXCHANGE_NAME = "delayed_exchange";

class RabbitMQManager extends Core {
  Main() {
    this.channel = null;
  }

  async init() {
    if (!this.config.EnvConfig.amqp) {
      throw new Error("[env] amqp is required.");
    }
    try {
      await this.amqpManager.connect(this.config.EnvConfig.amqp);
      this.channel = this.amqpManager.getChannel();
      console.log("RabbitMQ connection and channel created.");
    } catch (err) {
      console.error("Error initializing RabbitMQ:", err);
      throw err;
    }
  }

  async assertQueues(queue) {
    if (!this.channel || !queue) {
      throw new Error("Channel is not initialized.");
    }

    try {
      await this.channel.assertExchange(EXCHANGE_NAME, "x-delayed-message", {
        durable: true,
        arguments: {
          "x-delayed-type": "direct",
        },
      });

      await this.channel.assertQueue(queue, { durable: true });
      await this.channel.bindQueue(queue, EXCHANGE_NAME, "");
      console.log(`Queues asserted and bound for queue: ${queue}`);
    } catch (err) {
      console.error("Error asserting queues:", err);
      throw err;
    }
  }

  async close() {
      await this.amqpManager.close();
      console.log("RabbitMQ connection closed.");
  }
}

class MessageSender {
  constructor(rabbitMQManager) {
    this.rabbitMQManager = rabbitMQManager;
  }

  async sendMessage(message, queue, delayMs = 0, retries = 3) {
    try {
      await this.rabbitMQManager.init();
      await this.rabbitMQManager.assertQueues(queue);

      if (!this.rabbitMQManager.channel) {
        throw new Error("Channel is not initialized.");
      }

      this.rabbitMQManager.channel.publish(
        EXCHANGE_NAME,
        "",
        Buffer.from(JSON.stringify(message)),
        {
          persistent: true,
          headers: {
            "x-delay": delayMs,
          },
        }
      );
      console.log(`Sent message to queue '${queue}_delay' with delay of ${delayMs}ms`);
      await this.rabbitMQManager.close();
    } catch (err) {
      console.warn("Error sending message:", err);
      if (retries > 0) {
        console.log(`Retrying... (${4 - retries})`);
        await this.sendMessage(message, queue, delayMs, retries - 1);
      } else {
        console.error("Failed after retries:", err);
      }
    }
  }
}

(async () => {
  const rabbitMQManager = new RabbitMQManager();
  const messageSender = new MessageSender(rabbitMQManager);

  const text = {
    item_id: "macbook",
    text: "This is a sample message to send receiver to check the ordered Item Availability",
    timestamp: new Date().toISOString(),
    source: "source_name",
    module: "module_name",
  };

  try {
    await messageSender.sendMessage(text, rabbitMQManager.config.Args.queue, 5000);
  } catch (err) {
    console.error("Error occurred during message sending:", err);
  }
  process.exit(0);
})();
