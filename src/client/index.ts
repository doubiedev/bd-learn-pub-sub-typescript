import amqp from "amqplib";
import { isValidLocation, isValidRank } from "../internal/gamelogic/gamedata.js";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    console.log("Peril game client connected to RabbitMQ!");

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

    const username = await clientWelcome();

    await declareAndBind(
        conn,
        ExchangePerilDirect,
        `${PauseKey}.${username}`,
        PauseKey,
        SimpleQueueType.Transient,
    );

    const gameState = new GameState(username);

    while (true) {
        const words = await getInput();
        if (words.length === 0) continue;

        const command = words[0];
        if (command === "spawn") {
            commandSpawn(gameState, words);
        } else if (command === "move") {
            commandMove(gameState, words);
        } else if (command === "status") {
            commandStatus(gameState);
        } else if (command === "help") {
            printClientHelp()
        } else if (command === "spam") {
            console.log("Spamming not allowed yet!");
        } else if (command === "quit") {
            printQuit()
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

