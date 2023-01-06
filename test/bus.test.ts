import {describe, expect, test, beforeAll, afterAll} from '@jest/globals';
import * as dotenv from 'dotenv'
import {Bus} from "../src/bus";
import {MessageHandler} from "../src/message-handler";
import {Message} from '../src/message';
import {Event} from '../src/event';
import {DateTime} from "luxon";

let messageHandled: DateTime | undefined;
let eventHandled: DateTime | undefined;

function delay(time: number) {
    return new Promise(resolve => setTimeout(resolve, time));
}

class TestMessageHandler implements MessageHandler {
    async handle(message: Message): Promise<Event[]> {
        messageHandled = DateTime.now();
        return [new TestEvent()];
    }
}

class TestEventHandler implements MessageHandler {
    async handle(message: Message): Promise<Event[]> {
        eventHandled = DateTime.now();
        return [];
    }
}

let crumbs: string[] = [];

class HeavyHandler implements MessageHandler {
    async handle(message: Message): Promise<Event[]> {
        crumbs.push('started');
        await delay(1000);
        crumbs.push('finished');
        return [];
    }
}

class DataHandler implements MessageHandler {
    async handle(message: Message): Promise<Event[]> {
        crumbs.push(message.data['text']);
        crumbs.push(...message.data['array']);
        return [];
    }
}

class DataMessage implements Message {
    type: string = 'data-message';
    data: Record<string, any> = {};

    constructor(text: string, array: string[]) {
        this.data['text'] = text;
        this.data['array'] = array;
    }
}


class TestMessage implements Message {
    type: string = 'test-message';
    data: Record<string, any> = {};
}

class TestEvent implements Event {
    type: string = 'test-event';
    data: Record<string, any> = {};
}

describe('rabbitbus tests', () => {
    let bus = new Bus();
    beforeAll(async () => {
        dotenv.config()
        // A rabbitmq instance is needed at localhost.
    });

    afterAll(async () => {
        return;
    });

    test('message and event are handled', async () => {
        let context = await bus.createContext('test-simple-exchange', {
            host: 'localhost',
            port: 5672,
            username: 'guest',
            password: 'guest'
        });
        context.subscribe('test-message', new TestMessageHandler());
        context.subscribe('test-event', new TestEventHandler());
        expect(messageHandled).toBeFalsy();
        expect(eventHandled).toBeFalsy();
        context.listen();
        await context.publish(new TestMessage());
        await new Promise((r) => setTimeout(r, 5000));
        expect(messageHandled).toBeDefined();
        expect(eventHandled).toBeDefined();
        await bus.disposeContext(context);
    }, 30000);

    test('messages are handled sequentially', async () => {
        let context = await bus.createContext('test-sequential-exchange');
        crumbs = [];
        context.subscribe('test-message', new HeavyHandler());
        let promise = context.listen({maxConcurrentHandlers: 1});
        await delay(250);
        await context.publish(new TestMessage());
        await delay(250);
        await context.publish(new TestMessage());
        await delay(5000);
        expect(crumbs).toStrictEqual(['started', 'finished', 'started', 'finished']);
        await bus.disposeContext(context);
        await promise;
    }, 30000);

    test('messages are handled in parallel', async () => {
        let context = await bus.createContext('test-parallel-exchange');
        crumbs = [];

        context.subscribe('test-message', new HeavyHandler());
        let promise = context.listen({maxConcurrentHandlers: 2});
        await delay(250);
        await context.publish(new TestMessage());
        await delay(250);
        await context.publish(new TestMessage());
        await delay(5000);
        expect(crumbs).toStrictEqual(['started', 'started', 'finished', 'finished']);
        await bus.disposeContext(context);
        await promise;
    }, 30000);

    test('handler handles all types', async () => {
        let context = await bus.createContext('test-*-exchange');
        crumbs = [];
        context.subscribe('*', new HeavyHandler());
        let promise = context.listen({maxConcurrentHandlers: 2});
        await delay(250);
        await context.publish(new TestMessage());
        await delay(250);
        await context.publish(new TestEvent());
        await delay(5000);
        expect(crumbs).toStrictEqual(['started', 'started', 'finished', 'finished']);
        await bus.disposeContext(context);
        await promise;
    }, 30000);

    test('handler sees message data', async () => {
        let context = await bus.createContext('test-message-data-exchange');
        crumbs = [];
        context.subscribe('*', new DataHandler());
        let promise = context.listen({maxConcurrentHandlers: 1});
        await delay(250);
        await context.publish(new DataMessage('test-text-1', ['a1', 'a2']));
        await delay(250);
        await context.publish(new DataMessage('test-text-2', ['a3', 'a4']));
        await delay(5000);
        expect(crumbs).toStrictEqual(['test-text-1', 'a1', 'a2', 'test-text-2', 'a3', 'a4']);
        await bus.disposeContext(context);
        await promise;
    }, 30000);
});