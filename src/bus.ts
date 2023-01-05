import {connect} from "amqplib";
import {BusContext} from "./bus-context";

export class Bus {
    constructor() {
    }

    async createContext(exchangeName: string, options?: { host?: string, port?: number, username?: string, password?: string }): Promise<BusContext> {
        let connection = await connect({
            hostname: options?.host || 'localhost',
            port: options?.port || 5672,
            username: options?.username || 'guest',
            password: options?.password || 'guest'
        });
        let channel = await connection.createChannel();
        await channel.assertExchange(exchangeName, 'fanout', {durable: false});
        return new BusContext(connection, channel, exchangeName);
    }

    async disposeContext(context: BusContext) {
        await context.connection.close();
    }
}