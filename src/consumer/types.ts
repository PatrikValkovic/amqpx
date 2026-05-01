import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { MaybePromise } from '../types';

/**
 * How to handle Rabbit message when the processing fails (throws error).
 */
export enum ConsumptionFailureStrategy {
    /**
     * Acknowledges the message as successfully processed even when the handler throws.
     */
    Drop = 'Drop',
    /**
     * Rejects the message and returns it to the queue, so a different consumer can process it again.
     */
    Requeue = 'Requeue',
    /**
     * Message is rejected and moved into dead letter queue if configured on the consuming queue.
     */
    Reject = 'Reject',
}

/**
 * How to handle messages batch when processing fails (throw error).
 */
export enum BatchFailureStrategy  {
    /**
     * Reject the whole batch; the consumer will not try to reprocess the messages.
     * Each message in the batch is processed according to the `failureStrategy` property.
     */
    Fail = 'Fail',
    /**
     * Split the batch into individual messages and try to process each message separately.
     * The listener will be called for each message, where the batch passed into the listener
     * consists only of the single message.
     *
     * If the processing of the message fails again, it is handled according to the `failureStrategy` property.
     *
     * Split strategy is not supported when the batch size is equal to 1.
     */
    Split = 'Split',
}

export enum BatchState {
    WaitingForData = 'WaitingForData',
    Processing = 'Processing',
    Failed = 'Failed',
    Processed = 'Processed',
    Acknowledging = 'Acknowledging',
    Acknowledged = 'Acknowledged',
}

export type ConsumerCallbackArgs<Message, AdditionalProperties = Record<string, unknown>> = {
    channel: Channel;
    message: Message;
    rabbitMessage: amqp.Message;
} & AdditionalProperties;

/**
 * Arguments passed to a batch consumer callback.
 * Contains the channel and an array of message entries, each holding the parsed payload,
 * the raw amqplib message, and any additional properties added by the consumer implementation.
 */
export type BatchConsumerCallbackArgs<Message, AdditionalProperties = Record<string, unknown>> = {
    channel: Channel;
    messages: Array<{
        message: Message;
        rabbitMessage: amqp.Message;
    } & AdditionalProperties>;
};

export type ConsumerCallbackFn<Message, AdditionalProperties = Record<string, unknown>> = (args: ConsumerCallbackArgs<Message, AdditionalProperties>) => MaybePromise<void>;

/**
 * Callback function type for a batch consumer handler.
 * Receives a batch of messages along with their raw amqplib payloads and the channel.
 */
export type BatchConsumerCallbackFn<Message, AdditionalProperties = Record<string, unknown>> = (args: BatchConsumerCallbackArgs<Message, AdditionalProperties>) => MaybePromise<void>;

export type ConsumerOptions<T> = {
    /**
     * How to handle a RabbitMQ message when the handler throws an error.
     * Options are:
     * - Drop: acknowledges the message as successfully processed even when the handler throws.
     * - Requeue: rejects the message and returns it to the queue so a different consumer can process it.
     * - Reject: rejects the message and moves it to the dead letter queue if one is configured.
     */
    failureStrategy?: ConsumptionFailureStrategy;
    /**
     * Function that parses the message received by Rabbit.
     * By default it calls JSON.parse, but you may provide your own function to parse message in XML or Proto format.
     * @param payload content of the rabbit message.
     */
    parseMessageFn?(payload: Buffer): MaybePromise<T>;
    /**
     * Options passed down into `amqplib.channel.consume` function.
     * Auto acknowledgement is controlled by the `failureStrategy` property instead.
     */
    consumeOptions?: Omit<amqp.Options.Consume, 'noAck'>;
    /**
     * How many messages the RabbitMQ broker can send to the consumer in parallel.
     * This effectively controls the concurrency of the consumer.
     * Defaults to 0, which means the broker can send as many messages as it wants.
     *
     * Warning: prefetch is configured on the channel. It is recommended to use a separate channel
     * per consumer so that different prefetch values do not interfere with each other.
     */
    prefetch?: number;
    /**
     * Channel to use for consuming. If `null` (default), a new dedicated channel is created.
     */
    channel?: Channel | null;
};

export type BatchConsumerOptions<T> = ConsumerOptions<T> & {
    /**
     * How many messages are in single batch. Consumer can process multiple batches in parallel,
     * when prefetch is higher than the batch size.
     *
     * The batch can be smaller, for example when not enough messages were received during `maxWaitTimeForBatch`,
     * or when `batchFailureStrategy` is set to `Split`.
     *
     * If not specified, prefetch size will be used as a batch size.
     * If either batch size nor prefetch is specified, the default value is 20.
     */
    batchSize?: number;
    /**
     * Time in milliseconds to wait before process incomplete batch.
     *
     * When consumer doesn't have enough messages in buffer, it will wait
     * specified number of milliseconds for new messages to arrive. If after
     * the time there is still not enough messages, the consumer will invoke
     * the listener with smaller batch.
     *
     * Effectively, this property controls for how long the message can be in buffer
     * before the consumer invokes listener for it.
     *
     * By default is 100ms.
     */
    maxWaitTimeForBatch?: number;
    /**
     * After batch is successfully processed, and `failureStrategy` is not set to `Reject`,
     * consumer needs to confirm messages to the RabbitMQ broker. If there is different batch,
     * that has older messages than the current one, consumer will wait some time before
     * acknowledge the messages (so the batch with older messages have some time to finish).
     *
     * Reasoning behind is that Rabbit allows to either acknowledge each message separately, or
     * acknowledge sequential set of messages using single call (faster). When batch, that is not
     * the oldest one, finish processing, this timeout allows it to wait for the oldest batch
     * to complete, allowing consumer to acknowledge all the messages (both from the older batch,
     * as well for the current) using single acknowledgement call.
     *
     * When the processed job is the oldest, this property doesn't delay the acknowledgement and the
     * consumer acknowledge the whole batch right away using single call.
     *
     * If the older batches are not processed until the specified time, consumer acknowledge
     * each message separately.
     *
     * The property allows to wait for acknowledgements only, rejections are send to the RabbitMQ
     * broker immediately and there is currently no way to control it.
     *
     * This property effectively controls how long the message can wait before is acknowledge,
     * but after it is processed by listener.
     *
     * The value is specified in milliseconds. Default value is 0, meaning the consumer
     * doesn't wait for older batches to finish processing.
     */
    maxWaitTimeForAck?: number;
    /**
     * How to handle a batch of messages when processing fails (throws error).
     * Options are:
     * - Fail: reject the whole batch; the consumer will not try to reprocess the messages.
     * - Split: split the batch into individual messages and try to process each one separately.
     */
    batchFailureStrategy?: BatchFailureStrategy;
};

export const DEFAULT_CONSUMER_OPTIONS = {
    failureStrategy: ConsumptionFailureStrategy.Reject,
    parseMessageFn: (payload: Buffer) => JSON.parse(payload.toString()),
    consumeOptions: {},
    prefetch: 0,
    batchSize: -1,
    maxWaitTimeForBatch: 100,
    maxWaitTimeForAck: 0,
    batchFailureStrategy: BatchFailureStrategy.Fail,
    channel: null,
} as const satisfies Required<BatchConsumerOptions<unknown>> & Required<ConsumerOptions<unknown>>;

export type BatchRecord = {
    messages: Array<{
        rabbitMessage: amqp.Message;
    }>;
    state: BatchState;
    confirmTimer?: NodeJS.Timeout;
};

export type ConsumerWrapper<Callback> = {
    callback: Callback;
    amqpConsumer: amqp.Replies.Consume;
    messagesInFlight: number;
    isConnected: boolean;
};

export type BatchConsumerWrapper<Callback> = ConsumerWrapper<Callback> & {
    batches: BatchRecord[];
};
