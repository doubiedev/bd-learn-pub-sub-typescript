import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import { AckType } from "../internal/pubsub/consume.js";

export async function handlerGameLog(data: GameLog): Promise<AckType> {
    await writeLog(data);
    process.stdout.write("> ");
    return AckType.Ack;
}
