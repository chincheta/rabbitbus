import {Message} from "./message";
import {Event} from "./event";

export interface MessageHandler {
    handle(message: Message): Promise<Event[]>;
}