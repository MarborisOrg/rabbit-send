import { core } from "@marboris/coreutils";
import { connect } from "amqplib";

const EXCHANGE_NAME = "delayed_exchange";

class app extends core {
  constructor() {
    super()
    const text = {
      item_id: "macbook",
      text: "This is a sample message to send receiver to check the ordered Item Availability",
      timestamp: new Date().toISOString(),
      source: "source_name",
      module: "module_name",
    };
    void this.sendMessage(text, this.args.queue, 40000).then(
      async function () {
        await this.closeConnection();
        process.exit();
      }
    );
  }
  
    free() {

    }

  connection = null;
  channel = null;

  async initRabbitMQ() {
    if (!this.connection) {
      this.connection = await connect(this.env_config.amqp);
      this.channel = await this.connection.createChannel();
      console.log("RabbitMQ connection and channel created.");
    }
  }

  async assertQueues(queue) {
    if (!this.channel) {
      throw new Error("Channel is not initialized.");
    }
    await this.channel.assertExchange(EXCHANGE_NAME, "x-delayed-message", {
      durable: true,
      arguments: {
        "x-delayed-type": "direct",
      },
    });
    await this.channel.assertQueue(queue, { durable: true });
    await this.channel.bindQueue(queue, EXCHANGE_NAME, "");
  }

  async sendMessage(message, queue, delayMs = 0, retries = 3) {
    try {
      await this.initRabbitMQ();
      await this.assertQueues(queue);
      if (!this.channel) {
        throw new Error("Channel is not initialized.");
      }
      this.channel.publish(
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

  async closeConnection() {
    if (this.channel) await this.channel.close();
    if (this.connection) await this.connection.close();
    console.log("RabbitMQ connection closed.");
  }
}

new app()
