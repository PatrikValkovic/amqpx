import * as amqp from 'amqplib';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { deepMerge, last } from '../utils';
import {
    BatchConsumerCallbackFn,
    BatchConsumerOptions,
    BatchFailureStrategy,
    BatchRecord,
    BatchState,
    ConsumptionFailureStrategy,
    DEFAULT_CONSUMER_OPTIONS,
} from './types';
import { BatchConsumer, BatchConsumerEventMap } from './batch-consumer';
import { BaseConsumer } from './base-consumer';

export class BatchConsumerImplementation<Message>
    extends BaseConsumer<BatchConsumerCallbackFn<Message>, BatchConsumerEventMap>
    implements BatchConsumer<Message> {
    private static readonly DEFAULT_BATCH_SIZE = 20;

    private readonly options: Required<BatchConsumerOptions<Message>>;
    private batches: BatchRecord<Message>[] = [];
    private batchFillTimer: NodeJS.Timeout | undefined;

    constructor(
        channel: Channel,
        queue: Queue,
        options: BatchConsumerOptions<Message> = {},
    ) {
        super(channel, queue);
        this.options = deepMerge({}, DEFAULT_CONSUMER_OPTIONS, options) as typeof DEFAULT_CONSUMER_OPTIONS;
        if (this.effectiveBatchSize === 1 && this.options.batchFailureStrategy === BatchFailureStrategy.Split)
            throw new Error('Cannot have split batch failure strategy when batch size is 1');
    }

    async listen(callback: BatchConsumerCallbackFn<Message>) {
        if (this.consumer)
            throw new Error('Listener is already attached');

        this.consumer = (async () => {
            const [channel, queueName] = await Promise.all([
                this.channel.native(),
                this.queue.name(),
            ]);

            await channel.prefetch(this.options.prefetch);
            // this must be an object, so it is passed down as reference
            const stillConnected = { value: true };
            this.channel.once('close', () => {
                stillConnected.value = false;
            });

            const amqpConsumer = await channel.consume(
                queueName,
                this.messageReceiver.bind(this, callback, channel, stillConnected),
                {
                    ...this.options.consumeOptions,
                    noAck: this.shouldAutoAck,
                },
            );
            return { amqpConsumer, callback };
        })();

        await this.consumer;
        return this;
    }

    private async messageReceiver(
        callback: BatchConsumerCallbackFn<Message>,
        originalChannel: amqp.Channel,
        stillConnected: { value: boolean },
        msg: amqp.ConsumeMessage | null,
    ) {
        // empty message means consumer is canceled
        if (!msg) {
            await this.channel.close();
            return;
        }

        this.currentlyProcessingMessages++;
        const { content } = msg;
        const parsed = await this.options.parseMessageFn(content);

        // Create new batch if necessary
        if (this.batches.length === 0 || last(this.batches)?.state !== BatchState.WaitingForData) {
            this.batches.push({
                messages: [],
                state: BatchState.WaitingForData,
            });
        }

        // Add message to the last batch
        const lastBatch = last(this.batches);
        if (!lastBatch)
            throw this.processError('Internal error: Cannot get last batch');
        lastBatch.messages.push({
            message: parsed,
            rabbitMessage: msg,
        });

        // Last batch have enough messages, process it
        if (lastBatch.messages.length >= this.effectiveBatchSize) {
            clearTimeout(this.batchFillTimer);
            this.batchFillTimer = undefined;
            await this.handleBatch(
                callback,
                originalChannel,
                lastBatch,
                stillConnected,
            );
            return;
        }

        // Last batch has not enough messages, set max wait time before processing
        if (!this.batchFillTimer) {
            this.batchFillTimer = setTimeout(async () => {
                this.batchFillTimer = undefined;
                await this.handleBatch(
                    callback,
                    originalChannel,
                    lastBatch,
                    stillConnected,
                );
            }, this.maxWaitTimeForBatch);
        }
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

    private get maxWaitTimeForAck() {
        return Math.max(this.options.maxWaitTimeForAck, 0);
    }

    private get maxWaitTimeForBatch() {
        return Math.max(this.options.maxWaitTimeForBatch, 0);
    }

    private processError(message: string) {
        const err =  new Error(message);
        this.emit('error', err);
        return err;
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
                channel: this.channel,
            });
            batch.state = BatchState.Processed;
        } catch {
            await this.handleBatchError(callback, originalChannel, batch, stillConnected);
        } finally {
            await this.planMessageAcknowledgment(stillConnected, originalChannel);
            this.removeProcessedBatches();
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
            batch.state = BatchState.Acknowledged;
            this.batches.filter(batch =>
                batch.state === BatchState.Processed,
            ).forEach(batch => {
                batch.state = BatchState.Acknowledged;
            });
            return;
        }

        const indexOfBatch = this.batches.indexOf(batch);
        if (indexOfBatch < 0)
            throw this.processError('Internal error: Cannot find batch in the list of batches');

        const strategy = this.options.batchFailureStrategy;
        if (strategy === BatchFailureStrategy.Reject || batch.messages.length <= 1)
            await this.handleFailureStrategy(originalChannel, batch, stillConnected, indexOfBatch);
        else if (strategy === BatchFailureStrategy.Split)
            await this.splitBatch(originalChannel, batch, stillConnected, indexOfBatch, callback);
        else
            throw this.processError(`Not supported batch failure strategy: ${strategy}`);
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
            await this.nackMessages(originalChannel, batch, indexOfBatch, true);
            break;

        case ConsumptionFailureStrategy.Reject:
            await this.nackMessages(originalChannel, batch, indexOfBatch, false);
            break;

        default:
            throw this.processError(`Not supported failure strategy: ${this.options.failureStrategy}`);
        }
    }

    private async nackMessages(
        originalChannel: amqp.Channel,
        batch: BatchRecord<Message>,
        indexOfBatch: number,
        requeue: boolean,
    ) {
        if (indexOfBatch > 0) {
            await Promise.all(batch.messages.map(msg =>
                originalChannel.nack(msg.rabbitMessage, false, requeue),
            ));
        } else if (indexOfBatch === 0) {
            const lastMessage = last(batch.messages);
            if (!lastMessage)
                throw this.processError('Internal error: Last message in batch not found during nack');
            // keep await there in case API change in the future
            await originalChannel.nack(lastMessage.rabbitMessage, true, requeue);
        } else {
            throw this.processError('Internal error: Negative batch index during nack');
        }

        batch.state = BatchState.Acknowledged;
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

    private removeProcessedBatches() {
        const indicesOfBatchesToRemove = this.batches
            .flatMap((b, i) => {
                if (b.state === BatchState.Acknowledged)
                    return [i];
                return [];
            });
        if (indicesOfBatchesToRemove.length === 0)
            return;

        for (const index of indicesOfBatchesToRemove.reverse()) {
            const batch = this.batches[index];
            if (!batch)
                throw this.processError('Internal error: Batch for removal not found');
            this.batches.splice(index, 1);
            clearTimeout(batch.confirmTimer);
            batch.confirmTimer = undefined;
            this.currentlyProcessingMessages -= batch.messages.length;
        }

        this.notifyMessageProcessed?.();
    }

    private async planMessageAcknowledgment(
        stillConnected: { value: boolean },
        originalChannel: amqp.Channel,
    ) {
        if (this.batches.length === 0)
            return;

        if (!stillConnected.value || this.shouldAutoAck) {
            this.batches.filter(batch =>
                batch.state === BatchState.Processed,
            ).forEach(batch => {
                batch.state = BatchState.Acknowledged;
            });
            return;
        }

        // for first X batches we can use confirmation with "up to" semantic
        const firstUnprocessedIndex = this.batches.findIndex(
            ({ state }) =>
                state !== BatchState.Processed && state !== BatchState.Acknowledged,
        );
        const processedToIndex = firstUnprocessedIndex < 0 ? this.batches.length : firstUnprocessedIndex;
        const batchesToConfirm = this.batches.slice(0, processedToIndex);
        batchesToConfirm.forEach(batch => {
            clearTimeout(batch.confirmTimer);
            batch.confirmTimer = undefined;
        });
        if (batchesToConfirm.length > 0) {
            const lastConfirmBatch = last(batchesToConfirm);
            if (!lastConfirmBatch)
                throw this.processError('Internal Error: Last batch for confirmation not found');
            const lastMessageOfLastConfirmBatch = last(lastConfirmBatch.messages);
            if (!lastMessageOfLastConfirmBatch)
                throw this.processError('Internal Error: Last batch or last message for confirmation not found');
            // keep await there in case API change in the future
            await originalChannel.ack(lastMessageOfLastConfirmBatch.rabbitMessage, true);
        }
        batchesToConfirm.forEach(batch => {
            batch.state = BatchState.Acknowledged;
        });
        this.removeProcessedBatches();

        // for rest wait up to maxWaitTime to process previous batches
        this.batches
            .filter(batch =>
                batch.state === BatchState.Processed && !batch.confirmTimer,
            )
            .forEach(batch => {
                batch.confirmTimer = setTimeout(async () => {
                    if (!stillConnected.value) {
                        batch.state = BatchState.Acknowledged;
                        return;
                    }
                    await Promise.all(
                        batch.messages.map(({ rabbitMessage }) =>
                            originalChannel.ack(rabbitMessage, false),
                        ),
                    );

                    batch.state = BatchState.Acknowledged;
                    this.removeProcessedBatches();
                }, this.maxWaitTimeForAck);
            });
    }
}
