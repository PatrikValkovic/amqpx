import { TestChannel } from '../extensions/vitest';
import { ConsumerImplementation, BatchConsumerImplementation } from '../consumer';
import { QueueImplementation } from './queue-implementation';

describe('QueueImplementation', () => {
    let channel: TestChannel;
    let queue: QueueImplementation;

    beforeEach(() => {
        channel = new TestChannel();
        queue = new QueueImplementation(channel, 'test-queue');
    });

    describe('createConsumer', () => {
        it('returns ConsumerImplementation using queue channel when no channel in options', async () => {
            const consumer = await queue.createConsumer();
            expect(consumer).toBeInstanceOf(ConsumerImplementation);
            expect(consumer.channel).toBe(channel);
        });

        it('returns ConsumerImplementation using channel from options when provided', async () => {
            const otherChannel = new TestChannel();
            const consumer = await queue.createConsumer({ channel: otherChannel });
            expect(consumer).toBeInstanceOf(ConsumerImplementation);
            expect(consumer.channel).toBe(otherChannel);
        });
    });

    describe('createBatchConsumer', () => {
        it('returns BatchConsumerImplementation using queue channel when no channel in options', async () => {
            const consumer = await queue.createBatchConsumer();
            expect(consumer).toBeInstanceOf(BatchConsumerImplementation);
            expect(consumer.channel).toBe(channel);
        });

        it('returns BatchConsumerImplementation using channel from options when provided', async () => {
            const otherChannel = new TestChannel();
            const consumer = await queue.createBatchConsumer({ channel: otherChannel });
            expect(consumer).toBeInstanceOf(BatchConsumerImplementation);
            expect(consumer.channel).toBe(otherChannel);
        });
    });
});
