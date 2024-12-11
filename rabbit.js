import { Core } from "@marboris/coreutils";

const EXCHANGE_NAME = "delayed_exchange";

class RabbitMQManager extends Core {
  constructor() {
    super();
  }

  async init() {
    if (!this.config.EnvConfig.amqp) {
      throw new Error("[env] amqp is require.");
    }
    await this.amqpManager.connect(this.config.EnvConfig.amqp);
    console.log("RabbitMQ connection and channel created.");
  }

  async assertQueues(queue) {
    if (!this.amqpManager.getChannel()) {
      throw new Error("Channel is not initialized.");
    }
    await this.amqpManager.getChannel().assertExchange(EXCHANGE_NAME, "x-delayed-message", {
      durable: true,
      arguments: {
        "x-delayed-type": "direct",
      },
    });
    await this.amqpManager.getChannel().assertQueue(queue, { durable: true });
    await this.amqpManager.getChannel().bindQueue(queue, EXCHANGE_NAME, "");
  }

  async close() {
    await this.amqpManager.close()
  }

  free() {}
}

class MessageSender {
  constructor(rabbitMQManager) {
    this.rabbitMQManager = rabbitMQManager;
  }

  async sendMessage(message, queue, delayMs = 0, retries = 3) {
    try {
      await this.rabbitMQManager.init()
      await this.rabbitMQManager.assertQueues(queue);
      if (!this.rabbitMQManager.amqpManager.getChannel()) {
        throw new Error("Channel is not initialized.");
      }
      this.rabbitMQManager.amqpManager.getChannel().publish(
        EXCHANGE_NAME,
        "",
        Buffer.from(JSON.stringify(message)),
        {
          headers: {
            "x-delay": delayMs,
          },
        }
      );
      console.log(
        " [x] Sent '%s' to delay queue '%s'",
        JSON.stringify(message),
        `${queue}_delay`
      );
    await this.rabbitMQManager.close();
    } catch (err) {
      console.warn("Error sending message:", err);
      if (retries > 0) {
        console.log(`Retrying... (${3 - retries + 1})`);
        await this.sendMessage(message, queue, delayMs, retries - 1);
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

  await messageSender.sendMessage(text, rabbitMQManager.config.Args.queue, 40000);
  process.exit(0);
})();
