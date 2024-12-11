import { core } from "@marboris/coreutils";

const EXCHANGE_NAME = "delayed_exchange";

class app extends core {
  async free() {
    await this.closeAmqp();
  }

  constructor() {
    super();
    const text = {
      item_id: "macbook",
      text: "This is a sample message to send receiver to check the ordered Item Availability",
      timestamp: new Date().toISOString(),
      source: "source_name",
      module: "module_name",
    };
    void this.sendMessage(text, this.args.queue, 40000).then(async function () {
      process.exit(0);
    });
  }

  async initRabbitMQ() {
    await this.connectAmqp(this.env_config.amqp);
    console.log("RabbitMQ connection and channel created.");
  }

  async assertQueues(queue) {
    if (!this.channelAmqp) {
      throw new Error("Channel is not initialized.");
    }
    await this.channelAmqp.assertExchange(EXCHANGE_NAME, "x-delayed-message", {
      durable: true,
      arguments: {
        "x-delayed-type": "direct",
      },
    });
    await this.channelAmqp.assertQueue(queue, { durable: true });
    await this.channelAmqp.bindQueue(queue, EXCHANGE_NAME, "");
  }

  async sendMessage(message, queue, delayMs = 0, retries = 3) {
    try {
      await this.initRabbitMQ();
      await this.assertQueues(queue);
      if (!this.channelAmqp) {
        throw new Error("Channel is not initialized.");
      }
      this.channelAmqp.publish(
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
    } catch (err) {
      console.warn("Error sending message:", err);
      if (retries > 0) {
        console.log(`Retrying... (${3 - retries + 1})`);
        await this.sendMessage(message, queue, delayMs, retries - 1);
      }
    }
  }
}

new app();
