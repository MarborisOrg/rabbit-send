import core from "@marboris/coreutils";
import RabbitMQService from "./rabbit.send.service.js";

new (class extends core {
  constructor() {
    const text = {
      item_id: "macbook",
      text: "This is a sample message to send receiver to check the ordered Item Availability",
      timestamp: new Date().toISOString(),
      source: "source_name",
      module: "module_name",
    };
    void RabbitMQService.sendMessage(text, $.config.queue, 40000).then(
      async function () {
        await RabbitMQService.closeConnection();
        process.exit();
      }
    );
  }
})();
