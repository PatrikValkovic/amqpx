import { debuglog } from 'util';
import { Connection } from '../connection';
import { BatchConsumer, Consumer } from '../consumer';
import { Producer } from '../producer';
import { LIB_NAME } from '../utils';

const debug = debuglog(`${LIB_NAME}:shutdown`);

/**
 * Orchestrates an orderly shutdown of all registered producers, consumers, channels, and connections.
 *
 * Shutdown order: producers → consumers → channels → connections.
 */
export class RabbitCloser {
    /**
     * @param connections - Connections to close after all producers and consumers have stopped.
     * @param consumers - Consumers to stop before closing channels and connections.
     * @param producers - Producers to stop first, waiting for all pending publishes to settle.
     */
    constructor(
        private readonly connections: Connection[],
        private readonly consumers: Array<Consumer<unknown> | BatchConsumer<unknown>>,
        private readonly producers: Producer<unknown>[],
    ) {
    }

    /**
     * Performs the orderly shutdown sequence with an optional overall timeout budget.
     * Each stage receives the remaining milliseconds at the moment it starts, so later
     * stages (channels, connections) get progressively less time than earlier ones.
     * Waits for each stage to complete before proceeding to the next.
     * Follows sequence: producers → consumers → channels → connections.
     * @param timeout - Total budget in milliseconds for the entire shutdown sequence.
     *   When omitted, each stage waits indefinitely.
     */
    async close(timeout = 60_000): Promise<void> {
        const start = performance.now();
        const remaining = () => Math.max(0, timeout - (performance.now() - start));

        debug('shutdown-started producers=%d consumers=%d connections=%d', this.producers.length, this.consumers.length, this.connections.length);
        debug('shutdown-producers-started');
        await Promise.all(
            this.producers.map(producer => producer.close(remaining())),
        );

        debug('shutdown-consumers-started');
        await Promise.all(
            this.consumers.map(consumer => consumer.close(remaining())),
        );

        debug('shutdown-channels-started');
        await Promise.all([
            ...this.producers.map(producer => producer.channel.close(remaining())),
            ...this.consumers.map(consumer => consumer.channel.close(remaining())),
        ]);

        debug('shutdown-connections-started');
        await Promise.all(
            this.connections.map(connection => connection.close(remaining())),
        );

        debug('shutdown-complete');
    }
}
