import * as amqp from 'amqplib';
import { Queue } from '../queue';
import { Exchange } from './exchange';

export type ExchangeTypes = Parameters<amqp.Channel['assertExchange']>[1];

export type ExchangeConsumerQueueOptions = Omit<amqp.Options.AssertQueue, 'durable' | 'exclusive'>;

export type BindingArgs = Record<string, string | number | boolean>;

export enum BindingType {
    queue= 'queue',
    exchange = 'exchange',
}

type Pattern = string;
export type Binding = {
    pattern: Pattern;
    args?: BindingArgs;
} & ({
    type: BindingType.queue;
    queue: Queue;
} | {
    type: BindingType.exchange;
    exchange: Exchange;
});

export type ExchangeOptions = amqp.Options.AssertExchange & {
    assert?: boolean;
};
