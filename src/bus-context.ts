import {Channel, Connection, ConsumeMessage} from "amqplib";
import {MessageHandler} from "./message-handler";
import {Message} from "./message";

export class BusContext {
    handlers: Record<string, MessageHandler[]>
    connection: Connection;
    channel: Channel;
    exchangeName: string

    constructor(connection: Connection, channel: Channel, exchangeName: string) {
        this.connection = connection;
        this.channel = channel;
        this.exchangeName = exchangeName;
        this.handlers = {};
        this.handlers['*'] = [];
    }

    subscribe(messageType: string, handler: MessageHandler) {
        if (!this.handlers[messageType]) {
            this.handlers[messageType] = [];
        }
        this.handlers[messageType].push(handler);
    }

    async publish(message: Message) {
        let queue = await this.channel.assertQueue('', {exclusive: true});
        await this.channel.bindQueue(queue.queue, this.exchangeName, '');
        this.channel.publish(this.exchangeName, '', Buffer.from(JSON.stringify(message)));
    }

    async listen(options?: { maxConcurrentHandlers?: number }) {
        let queue = await this.channel.assertQueue('', {exclusive: true});
        await this.channel.bindQueue(queue.queue, this.exchangeName, '');
        if (options?.maxConcurrentHandlers) {
            await this.channel.prefetch(options?.maxConcurrentHandlers);
        }
        await this.channel.consume(queue.queue, async (msg: ConsumeMessage | null) => {
            if (msg && msg.content) {
                let message = JSON.parse(msg.content.toString()) as Message;
                let handlers = [...this.handlers['*']];
                if (this.handlers[message.type]) {
                    handlers.push(...this.handlers[message.type]);
                }
                for (let handler of handlers) {
                    let events = await handler.handle(message);
                    for (let event of events) {
                        await this.publish(event);
                    }
                }
                this.channel?.ack(msg);
            }
        }, {noAck: false});
    }
}