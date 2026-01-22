import amqp from "amqplib";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    console.log("Peril game server connected to RabbitMQ!");
    printServerHelp();

    ["SIGINT", "SIGTERM"].forEach((signal) =>
        process.on(signal, async () => {
            try {
                await conn.close();
                console.log("RabbitMQ connection closed.");
            } catch (err) {
                console.error("Error closing RabbitMQ connection:", err);
            } finally {
                process.exit(0);
            }
        }),
    );

    const publishCh = await conn.createConfirmChannel();

    while (true) {
        const input = await getInput()
        if (!input[0]) {
            continue;
        }
        const cleanedInput = input[0].toLowerCase().trim()

        if (cleanedInput === "pause") {
            console.log("Sending a pause message...");
            try {
                await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
                    isPaused: true,
                });
            } catch (err) {
                console.error("Error publishing message:", err);
            }
        } else if (cleanedInput === "resume") {
            console.log("Sending a resume message...");
            try {
                await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
                    isPaused: false,
                });
            } catch (err) {
                console.error("Error publishing message:", err);
            }
        } else if (cleanedInput === "quit") {
            console.log("Exiting...");
            break;
        } else {
            console.log("Unknown command");
        }
    }
}

main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});

