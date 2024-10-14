import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { MaybePromise } from '../types';

export enum ConsumptionFailedStrategy {
    Drop = 'Drop',
    Requeue = 'Requeue',
    Reject = 'Reject',
}

export type ConsumerCallbackArgs<Message, AdditionalProperties = Record<string, unknown>> = {
    message: Message;
    rabbitMessage: amqp.Message;
    channel: Channel;
} & AdditionalProperties;

export type ConsumerCallbackFn<Message, AdditionalProperties = Record<string, unknown>> = (args: ConsumerCallbackArgs<Message, AdditionalProperties>) => MaybePromise<void>;

export type ConsumerOptions<T> = {
    failureStrategy?: ConsumptionFailedStrategy;
    parseMessageFn?(payload: Buffer): MaybePromise<T>;
    consumeOptions?: Omit<amqp.Options.Consume, 'noAck'>;
    prefetch?: number;
    channel?: Channel | null;
};

export const DEFAULT_CONSUMER_OPTIONS = {
    failureStrategy: ConsumptionFailedStrategy.Drop,
    parseMessageFn: (payload: Buffer) => JSON.parse(payload.toString()),
    consumeOptions: {},
    prefetch: 0,
    channel: null,
} as const satisfies Required<ConsumerOptions<unknown>>;
