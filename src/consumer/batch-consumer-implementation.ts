import { EventEmitter } from 'node:events';
import * as amqp from 'amqplib';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { deepMerge, head, last, sum, sleepPromise } from '../utils';
import {
    BatchConsumerCallbackFn,
    BatchConsumerOptions,
    BatchFailureStrategy,
    BatchRecord,
    BatchState,
    ConsumerWrapper,
    ConsumptionFailureStrategy,
    DEFAULT_CONSUMER_OPTIONS,
} from './types';
import { BatchConsumer, BatchConsumerEventMap } from './batch-consumer';
import { RECONNECT_TIMEOUT } from './consumer';

export class BatchConsumerImplementation<Message> extends EventEmitter<BatchConsumerEventMap> implements BatchConsumer<Message> {
    private static readonly DEFAULT_BATCH_SIZE = 20;

    private readonly options: Required<BatchConsumerOptions<Message>>;
    private consumer: ConsumerWrapper<BatchConsumerCallbackFn<Message>> | null = null;
    private currentlyProcessingMessages = 0;
    private notifyMessageProcessed: (() => void) | undefined = undefined;
    private batches: BatchRecord<Message>[] = [];
    private batchFillingTimer: NodeJS.Timeout | undefined;

    constructor(
        private readonly _channel: Channel,
        private readonly _queue: Queue,
        options: BatchConsumerOptions<Message> = {},
    ) {
        super();
        this.options = deepMerge({}, DEFAULT_CONSUMER_OPTIONS, options) as typeof DEFAULT_CONSUMER_OPTIONS;
        if (this.effectiveBatchSize === 1 && this.options.batchFailureStrategy === BatchFailureStrategy.Split)
            throw new Error('Cannot have split batch failure strategy when batch size is 1');
        this._channel.on('close', this.channelCloseCallback);
    }

    // Must be kept as attribute instead of method, so I don't need to deal with .bind
    // This magic will make sure the implementation tries to reconnect to the channel with
    // some backoff when the connection is lost.
    private readonly channelCloseCallback = () => {
        setTimeout(() => {
            if (this.consumer) {
                const { callback } = this.consumer;
                this.consumer = null;
                this.listen(callback).catch(() => {
                    void this.close();
                });
            }
        }, RECONNECT_TIMEOUT);
    };

    async close(timeout = 30000): Promise<void> {
        if (this.consumer) {
            const channel = await this._channel.native();

            const { consumer } = this;
            await channel.cancel(consumer.amqpConsumer.consumerTag);

            const waitForAllMessagesPromise = new Promise<void>(resolve => {
                this.notifyMessageProcessed = () => {
                    if (this.currentlyProcessingMessages === 0)
                        resolve();
                };
                this.notifyMessageProcessed();
            });

            try {
                await Promise.race([
                    waitForAllMessagesPromise,
                    sleepPromise(timeout).then(() => {
                        throw new Error('Consumer close timeout');
                    }),
                ]);
            } finally {
                this.consumer = null;
            }
        }
    }

    async listen(callback: BatchConsumerCallbackFn<Message>) {
        if (this.consumer)
            throw new Error('Listener is already attached');

        const [channel, queueName] = await Promise.all([
            this._channel.native(),
            this._queue.name(),
        ]);
        await channel.prefetch(this.options.prefetch);
        const amqpConsumer = await channel.consume(
            queueName,
            this.messageReceiver.bind(this, callback, channel),
            {
                ...this.options.consumeOptions,
                noAck: this.shouldAutoAck,
            },
        );
        this.consumer = { amqpConsumer, callback };
        return this;
    }

    get queue(): Queue {
        return this._queue;
    }

    get channel(): Channel {
        return this._channel;
    }

    private get effectiveBatchSize() {
        if (this.options.batchSize >= 0)
            return this.options.batchSize;
        if (this.options.prefetch > 0)
            return this.options.prefetch;
        return BatchConsumerImplementation.DEFAULT_BATCH_SIZE;
    }

    private get shouldAutoAck() {
        return this.options.failureStrategy === ConsumptionFailureStrategy.Drop;
    }

    private get acknowledgeDelay() {
        return Math.max(this.options.maxWaitTimeForAck, 0);
    }

    private get maxProcessingDelay() {
        return Math.max(this.options.maxWaitTimeForBatch, 0);
    }

    private async messageReceiver(
        callback: BatchConsumerCallbackFn<Message>,
        originalChannel: amqp.Channel,
        msg: amqp.ConsumeMessage | null,
    ) {
        // empty message means consumer is cancelled
        if (!msg) {
            await this._channel.close();
            return;
        }

        // it must be an object, so it is passed into other methods as a reference
        const stillConnected = { value: true };
        const handler = () => {
            stillConnected.value = false;
        };
        this._channel.once('close', handler);

        try {
            this.currentlyProcessingMessages++;
            const { content } = msg;
            const parsed = await this.options.parseMessageFn(content);

            if (this.batches.length === 0 || last(this.batches)?.state !== BatchState.WaitingForData) {
                this.batches.push({
                    messages: [],
                    state: BatchState.WaitingForData,
                });
            }
            const lastNotProcessedBatch = last(this.batches);
            if (!lastNotProcessedBatch)
                throw this.processError('Internal error: Cannot get last batch, should never happen');
            lastNotProcessedBatch.messages.push({
                message: parsed,
                rabbitMessage: msg,
            });

            // we have enough messages
            if (lastNotProcessedBatch.messages.length >= this.effectiveBatchSize) {
                clearTimeout(this.batchFillingTimer);
                this.batchFillingTimer = undefined;
                await this.handleBatch(
                    callback,
                    originalChannel,
                    lastNotProcessedBatch,
                    stillConnected,
                );
                return;
            }

            // not enough messages
            if (!this.batchFillingTimer) {
                this.batchFillingTimer = setTimeout(async () => {
                    this.batchFillingTimer = undefined;
                    await this.handleBatch(
                        callback,
                        originalChannel,
                        lastNotProcessedBatch,
                        stillConnected,
                    );
                }, this.maxProcessingDelay);
            }
        } finally {
            this._channel.off('close', handler);
        }
    }

    private async handleBatch(
        callback: BatchConsumerCallbackFn<Message>,
        originalChannel: amqp.Channel,
        batch: BatchRecord<Message>,
        stillConnected: { value: boolean },
    ) {
        try {
            batch.state = BatchState.Processing;
            await callback({
                messages: batch.messages,
                channel: this._channel,
            });
            if (!stillConnected.value) {
                this.removeProcessedBatches(batch);
                return;
            }

            batch.state = BatchState.Processed;
            await this.planMessageAcknowledgment(stillConnected, originalChannel);
        } catch {
            await this.handleBatchError(callback, originalChannel, batch, stillConnected);
        }
    }

    private async handleBatchError(
        callback: BatchConsumerCallbackFn<Message>,
        originalChannel: amqp.Channel,
        batch: BatchRecord<Message>,
        stillConnected: { value: boolean },
    ) {
        batch.state = BatchState.Failed;
        if (!stillConnected.value) {
            this.removeProcessedBatches(batch);
            return;
        }

        const indexOfBatch = this.batches.indexOf(batch);
        if (indexOfBatch < 0)
            throw this.processError('Internal error: Cannot find batch in the list of batches, should never happen');

        const strategy = this.options.batchFailureStrategy;
        if (strategy === BatchFailureStrategy.Reject || batch.messages.length <= 1) {
            await this.handleFailureStrategy(originalChannel, batch, stillConnected, indexOfBatch);
        } else if (strategy === BatchFailureStrategy.Split) {
            await this.splitBatch(originalChannel, batch, stillConnected, indexOfBatch, callback);
        } else {
            this.removeProcessedBatches(batch);
            throw this.processError(`Not supported batch failure strategy: ${strategy}`);
        }
    }

    private async handleFailureStrategy(
        originalChannel: amqp.Channel,
        batch: BatchRecord<Message>,
        stillConnected: { value: boolean },
        indexOfBatch: number,
    ) {
        switch (this.options.failureStrategy) {
        case ConsumptionFailureStrategy.Drop:
            batch.state = BatchState.Processed;
            await this.planMessageAcknowledgment(stillConnected, originalChannel);
            break;

        case ConsumptionFailureStrategy.Requeue:
            this.nackMessages(originalChannel, batch, indexOfBatch, true);
            this.removeProcessedBatches(batch);
            break;

        case ConsumptionFailureStrategy.Reject:
            this.nackMessages(originalChannel, batch, indexOfBatch, false);
            this.removeProcessedBatches(batch);
            break;

        default:
            this.removeProcessedBatches(batch);
            throw this.processError(`Not supported failure strategy: ${this.options.failureStrategy}`);
        }
    }

    private nackMessages(
        originalChannel: amqp.Channel,
        batch: BatchRecord<Message>,
        indexOfBatch: number,
        requeue: boolean,
    ) {
        if (indexOfBatch > 0) {
            batch.messages.forEach(msg => originalChannel.nack(msg.rabbitMessage, false, requeue));
        } else if (indexOfBatch === 0) {
            const lastMessage = last(batch.messages);
            if (!lastMessage)
                throw new Error('Encountered empty batch');
            originalChannel.nack(lastMessage.rabbitMessage, true, requeue);
        }
    }

    private async splitBatch(
        originalChannel: amqp.Channel,
        batch: BatchRecord<Message>,
        stillConnected: { value: boolean },
        indexOfBatch: number,
        callback: BatchConsumerCallbackFn<Message>,
    ) {
        const splitBatches: BatchRecord<Message>[] = batch.messages.map(message => ({
            state: BatchState.Processing,
            messages: [message],
        }));
        this.batches.splice(indexOfBatch, 1, ...splitBatches);
        await Promise.all(splitBatches.map(batch => this.handleBatch(
            callback,
            originalChannel,
            batch,
            stillConnected,
        )));
    }

    private removeProcessedBatches(batch: BatchRecord<Message> | Array<BatchRecord<Message>>) {
        const batchesToRemove = Array.isArray(batch) ? batch : [batch];
        if (batchesToRemove.length === 0)
            return;
        const removeAtIndexes = batchesToRemove
            .map(b => this.batches.indexOf(b))
            .sort((a, b) => a - b);
        if (removeAtIndexes.some(i => i < 0))
            throw this.processError('Internal error: Cannot find batch to remove in the list of batches, should never happen');

        // turn indexes into ranges so number of splice calls is reduced
        const rangesToRemove: Array<[number, number]> = [];
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        let currentStart: number = head(removeAtIndexes)!;
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        let currentEnd: number = head(removeAtIndexes)!;
        removeAtIndexes.slice(1).forEach(batchIndex => {
            if (batchIndex === currentEnd + 1)
                return;
            rangesToRemove.push([currentStart, currentEnd]);
            currentStart = batchIndex;
            currentEnd = batchIndex;
        });
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        rangesToRemove.push([currentStart!, last(removeAtIndexes)!]);

        for (const [from, to] of rangesToRemove)
            this.batches.splice(from, to - from + 1);

        this.currentlyProcessingMessages -= sum(batchesToRemove.map(b => b.messages.length));
        this.notifyMessageProcessed?.();
    }

    private async planMessageAcknowledgment(
        stillConnected: { value: boolean },
        channel: amqp.Channel,
    ) {
        if (!stillConnected.value || this.batches.length === 0)
            return;

        const firstUnprocessedIndex = this.batches.findIndex(({ state }) => state !== BatchState.Processed);
        const processedToIndex = firstUnprocessedIndex < 0 ? this.batches.length : firstUnprocessedIndex;
        const batchesToConfirm = this.batches.slice(0, processedToIndex);

        // for first X batches we can use confirmation with "up to" semantic
        if (batchesToConfirm.length > 0) {
            for (const batch of batchesToConfirm)
                clearTimeout(batch.confirmTimer);
            const lastConfirmBatch = last(batchesToConfirm);
            if (!lastConfirmBatch)
                throw this.processError('Internal Error: Last batch for confirmation not found, should never happen');
            const lastMessageOfLastConfirmBatch = last(lastConfirmBatch.messages);
            if (!lastMessageOfLastConfirmBatch)
                throw this.processError('Internal Error: Last batch or last message for confirmation not found, should never happen');
            if (!this.shouldAutoAck)
                channel.ack(lastMessageOfLastConfirmBatch.rabbitMessage, true);
            this.removeProcessedBatches(batchesToConfirm);
        }

        // for rest wait in case the missing batch will be processed in meantime
        this.batches
            .filter(batch =>
                batch.state === BatchState.Processed && !batch.confirmTimer,
            )
            .forEach(batch => {
                batch.confirmTimer = setTimeout(() => {
                    this.removeProcessedBatches(batch);
                    if (!stillConnected.value)
                        return;
                    if (!this.shouldAutoAck) {
                        batch.messages.forEach(({ rabbitMessage }) =>
                            channel.ack(rabbitMessage, false),
                        );
                    }
                }, this.acknowledgeDelay);
            });
    }

    private processError(message: string) {
        this.emit('error', message);
        return new Error(message);
    }
}
