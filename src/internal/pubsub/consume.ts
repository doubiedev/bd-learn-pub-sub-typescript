import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const ch = await conn.createChannel();

    const queue = await ch.assertQueue(queueName, {
        durable: queueType === SimpleQueueType.Durable,
        exclusive: queueType !== SimpleQueueType.Durable,
        autoDelete: queueType !== SimpleQueueType.Durable,
    });

    await ch.bindQueue(queue.queue, exchange, key);
    return [ch, queue];
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => AckType,
): Promise<void> {
    const [ch, queue] = await declareAndBind(
        conn,
        exchange,
        queueName,
        key,
        queueType,
    );

    await ch.consume(queue.queue, function(msg: amqp.ConsumeMessage | null) {
        if (!msg) return;

        let data: T;
        try {
            data = JSON.parse(msg.content.toString());
        } catch (err) {
            console.error("Could not unmarshal message:", err);
            return;
        }

        const ackType = handler(data);
        if (ackType === AckType.Ack) {
            ch.ack(msg);
            console.log("Message was ACKed by the broker");
        } else if (ackType === AckType.NackRequeue) {
            ch.nack(msg, false, true);
            console.log("Message was NACKed by the broker, requeueing");
        } else if (ackType === AckType.NackDiscard) {
            ch.nack(msg, false, false);
            console.log("Message was NACKed by the broker, discarding");
        }
    });
}
