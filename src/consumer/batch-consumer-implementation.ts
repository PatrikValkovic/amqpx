import { debuglog } from 'util';
import * as amqp from 'amqplib';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { deepMerge, errToMessage, last, LIB_NAME } from '../utils';
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

const debug = debuglog(`${LIB_NAME}:consume:batch`);

export class BatchConsumerImplementation<Message>
    extends BaseConsumer<BatchConsumerCallbackFn<Message>, BatchConsumerEventMap>
    implements BatchConsumer<Message> {
    private static readonly DEFAULT_BATCH_SIZE = 20;

    private readonly options: Required<BatchConsumerOptions<Message>>;
    private batches: BatchRecord[] = [];
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
            this.channel.on('close', this.channelCloseCallback);

            debug('start-listening queue=%s prefetch=%d ack=%s', queueName, this.options.prefetch, !this.shouldAutoAck);
            await channel.prefetch(this.options.prefetch);

            // this must be an object, so it is passed down as reference
            const stillConnected = { value: true };
            this.channel.once('close', () => {
                debug(`channel-closed queue=%s`, queueName);
                stillConnected.value = false;
            });

            const amqpConsumer = await channel.consume(
                queueName,
                this.messageReceiver.bind(this, callback, channel, stillConnected, queueName),
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
        queueName: string,
        msg: amqp.ConsumeMessage | null,
    ) {
        // empty message means consumer is canceled
        if (!msg) {
            debug('receive-empty-message queue=%s', queueName);
            await this.channel.close();
            return;
        }

        this.currentlyProcessingMessages++;
        debug('receive-message queue=%s in-flight=%d', queueName, this.currentlyProcessingMessages);

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
        lastBatch.messages.push({ rabbitMessage: msg });

        // Last batch have enough messages, process it
        if (lastBatch.messages.length >= this.effectiveBatchSize) {
            debug('processing-batch queue=%s reason=%s', queueName, 'full-batch');
            clearTimeout(this.batchFillTimer);
            this.batchFillTimer = undefined;
            await this.handleBatch(
                callback,
                originalChannel,
                lastBatch,
                stillConnected,
                queueName,
            );
            return;
        }

        // Last batch has not enough messages, set max wait time before processing
        if (!this.batchFillTimer) {
            this.batchFillTimer = setTimeout(async () => {
                debug('processing-batch queue=%s reason=%s', queueName, 'timeout');
                this.batchFillTimer = undefined;
                await this.handleBatch(
                    callback,
                    originalChannel,
                    lastBatch,
                    stillConnected,
                    queueName,
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
        return this.options.failureStrategy === ConsumptionFailureStrategy.Drop && this.options.prefetch <= 0;
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
        batch: BatchRecord,
        stillConnected: { value: boolean },
        queueName: string,
    ) {
        // guard in case processing is called twice on the same batch
        if (batch.state !== BatchState.WaitingForData)
            return;

        if (!stillConnected.value) {
            debug('batch-acknowledged queue=%s reason=%s', queueName, 'disconnected');
            batch.state = BatchState.Acknowledged;
            return;
        }

        try {
            debug('processing-batch-started queue=%s batch-size=%d', queueName, batch.messages.length);
            batch.state = BatchState.Processing;
            const parsedMessages = await Promise.all(
                batch.messages.map(async ({ rabbitMessage }) => ({
                    rabbitMessage,
                    message: await this.options.parseMessageFn(rabbitMessage.content),
                })),
            );
            await callback({
                messages: parsedMessages,
                channel: this.channel,
            });
            debug('batch-processed queue=%s reason=%s', queueName, 'succeess');
            batch.state = BatchState.Processed;
        } catch (error) {
            debug('batch-processing-failed queue=%s error=%s strategy=%s', queueName, errToMessage(error), this.options.batchFailureStrategy);
            this.emit('handlingFailed', error);
            await this.handleBatchError(
                callback,
                originalChannel,
                batch,
                stillConnected,
                queueName,
            );
        } finally {
            await this.planMessageAcknowledgment(stillConnected, originalChannel, queueName);
            this.removeProcessedBatches(stillConnected, queueName);
            debug('processing-batch-finished queue=%s batch-size=%d', queueName, batch.messages.length);
        }
    }

    private async handleBatchError(
        callback: BatchConsumerCallbackFn<Message>,
        originalChannel: amqp.Channel,
        batch: BatchRecord,
        stillConnected: { value: boolean },
        queueName: string,
    ) {
        batch.state = BatchState.Failed;

        if (!stillConnected.value) {
            debug('batch-acknowledged queue=%s reason=%s', queueName, 'disconnected');
            batch.state = BatchState.Acknowledged;
            return;
        }

        const indexOfBatch = this.batches.indexOf(batch);
        if (indexOfBatch < 0)
            throw this.processError('Internal error: Cannot find batch in the list of batches');

        const strategy = this.options.batchFailureStrategy;
        if (strategy === BatchFailureStrategy.Reject || batch.messages.length <= 1) {
            await this.handleFailureStrategy(originalChannel, batch, stillConnected, indexOfBatch, queueName);
        } else if (strategy === BatchFailureStrategy.Split) {
            await this.splitBatch(
                originalChannel,
                batch,
                stillConnected,
                indexOfBatch,
                callback,
                queueName,
            );
        } else {
            throw this.processError(`Not supported batch failure strategy: ${strategy}`);
        }
    }

    private async handleFailureStrategy(
        originalChannel: amqp.Channel,
        batch: BatchRecord,
        stillConnected: { value: boolean },
        indexOfBatch: number,
        queueName: string,
    ) {
        switch (this.options.failureStrategy) {
        case ConsumptionFailureStrategy.Drop:
            batch.state = BatchState.Processed;
            debug('batch-processed queue=%s reason=%s', queueName, 'drop-strategy');
            await this.planMessageAcknowledgment(stillConnected, originalChannel, queueName);
            break;

        case ConsumptionFailureStrategy.Requeue:
            await this.nackMessages(originalChannel, batch, indexOfBatch, true, queueName);
            break;

        case ConsumptionFailureStrategy.Reject:
            await this.nackMessages(originalChannel, batch, indexOfBatch, false, queueName);
            break;

        default:
            throw this.processError(`Not supported failure strategy: ${this.options.failureStrategy}`);
        }
    }

    private async nackMessages(
        originalChannel: amqp.Channel,
        batch: BatchRecord,
        indexOfBatch: number,
        requeue: boolean,
        queueName: string,
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

        debug('batch-acknowledged queue=%s reason=%s requeue=%s', queueName, 'nacked', requeue);
        batch.state = BatchState.Acknowledged;
    }

    private async splitBatch(
        originalChannel: amqp.Channel,
        batch: BatchRecord,
        stillConnected: { value: boolean },
        indexOfBatch: number,
        callback: BatchConsumerCallbackFn<Message>,
        queueName: string,
    ) {
        debug('batch-split queue=%s messages=%d', queueName, batch.messages.length);
        const splitBatches: BatchRecord[] = batch.messages.map(message => ({
            state: BatchState.WaitingForData,
            messages: [message],
        }));
        this.batches.splice(indexOfBatch, 1, ...splitBatches);
        await Promise.all(splitBatches.map(batch => this.handleBatch(
            callback,
            originalChannel,
            batch,
            stillConnected,
            queueName,
        )));
    }

    private removeProcessedBatches(stillConnected: { value: boolean }, queueName: string) {
        if (!stillConnected.value || this.shouldAutoAck) {
            this.batches
                .filter(batch =>
                    batch.state === BatchState.Processed || batch.state === BatchState.Acknowledging,
                ).forEach(batch => {
                    debug('batch-acknowledged queue=%s reason=%s', queueName, 'disconnected');
                    batch.state = BatchState.Acknowledged;
                });
        }

        const indicesOfBatchesToRemove = this.batches
            .flatMap((b, i) => {
                if (b.state === BatchState.Acknowledged)
                    return [i];
                return [];
            });
        if (indicesOfBatchesToRemove.length === 0)
            return;

        debug('acknowledged-cleanup queue=%s batches=%d', queueName, indicesOfBatchesToRemove.length);
        for (const index of indicesOfBatchesToRemove.reverse()) {
            const batch = this.batches[index];
            if (!batch)
                throw this.processError('Internal error: Batch for removal not found');
            this.batches.splice(index, 1);
            clearTimeout(batch.confirmTimer);
            batch.confirmTimer = undefined;
            this.currentlyProcessingMessages -= batch.messages.length;
        }

        debug('acknowledged-cleaned queue=%s batches=%d in-flight=%d', queueName, indicesOfBatchesToRemove.length, this.currentlyProcessingMessages);
        this.notifyMessageProcessed?.();
    }

    private async planMessageAcknowledgment(
        stillConnected: { value: boolean },
        originalChannel: amqp.Channel,
        queueName: string,
    ) {
        if (this.batches.length === 0)
            return;

        if (!stillConnected.value || this.shouldAutoAck)
            return;

        // for first X batches we can use confirmation with "up to" semantic
        const firstUnprocessedIndex = this.batches.findIndex(
            ({ state }) =>
                [BatchState.WaitingForData, BatchState.Processing, BatchState.Failed].includes(state),
        );
        const processedToIndex = firstUnprocessedIndex < 0 ? this.batches.length : firstUnprocessedIndex;
        // batches only in states Processed, Acknowledging, and Acknowledged
        const batchesToConfirm = this.batches.slice(0, processedToIndex);
        batchesToConfirm.forEach(batch => {
            clearTimeout(batch.confirmTimer);
            batch.confirmTimer = undefined;
        });

        if (batchesToConfirm.length > 0 && last(batchesToConfirm)?.state === BatchState.Processed) {
            debug('acknowledging-from-head queue=%s batches=%d', queueName, batchesToConfirm.length);
            const lastConfirmBatch = last(batchesToConfirm);
            if (!lastConfirmBatch)
                throw this.processError('Internal Error: Last batch for confirmation not found');
            const lastMessageOfLastConfirmBatch = last(lastConfirmBatch.messages);
            if (!lastMessageOfLastConfirmBatch)
                throw this.processError('Internal Error: Last batch or last message for confirmation not found');
            batchesToConfirm.forEach(batch => {
                if (batch.state === BatchState.Processed)
                    batch.state = BatchState.Acknowledging;
            });
            // keep await there in case API change in the future
            await originalChannel.ack(lastMessageOfLastConfirmBatch.rabbitMessage, true);
            batchesToConfirm.forEach(batch => {
                debug('batch-acknowledged queue=%s reason=%s', queueName, 'success');
                batch.state = BatchState.Acknowledged;
            });
        }

        // No bulk-ack possible (blocking batch before them): install confirmTimers for
        // Processed batches that are waiting on an earlier unfinished batch.
        this.batches
            .filter(batch =>
                batch.state === BatchState.Processed && !batch.confirmTimer,
            )
            .forEach(batch => {
                batch.confirmTimer = setTimeout(async () => {
                    if (!stillConnected.value) {
                        debug('batch-acknowledged queue=%s reason=%s', queueName, 'disconnect');
                        batch.state = BatchState.Acknowledged;
                    }
                    if (batch.state !== BatchState.Processed)
                        return;
                    debug('acknowledging-out-of-order queue=%s', queueName);
                    batch.state = BatchState.Acknowledging;
                    await Promise.all(
                        batch.messages.map(({ rabbitMessage }) =>
                            originalChannel.ack(rabbitMessage, false),
                        ),
                    );
                    debug('batch-acknowledged queue=%s reason=%s', queueName, 'success');
                    batch.state = BatchState.Acknowledged;
                    this.removeProcessedBatches(stillConnected, queueName);
                }, this.maxWaitTimeForAck);
            });
    }
}
