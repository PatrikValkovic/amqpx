import { debuglog } from 'util';
import { Connection } from '../connection';
import { BatchConsumer, Consumer } from '../consumer';
import { Producer } from '../producer';
import { LIB_NAME } from '../utils';

const debug = debuglog(`${LIB_NAME}:shutdown`);

export class RabbitCloser {
    constructor(
        private readonly connections: Connection[],
        private readonly consumers: Array<Consumer<unknown> | BatchConsumer<unknown>>,
        private readonly producers: Producer<unknown>[],
    ) {
    }

    async close(): Promise<void> {
        debug('shutdown-started producers=%d consumers=%d connections=%d', this.producers.length, this.consumers.length, this.connections.length);
        debug('shutdown-producers-started');
        await Promise.all(
            this.producers.map(async producer => {
                await producer.close();
            }),
        );
        debug('shutdown-consumers-started');
        await Promise.all(
            this.consumers.map(async consumer => {
                await consumer.close();
            }),
        );
        debug('shutdown-channels-started');
        await Promise.all([
            ...this.producers.map(producer => producer.channel.close()),
            ...this.consumers.map(consumer => consumer.channel.close()),
        ]);
        debug('shutdown-connections-started');
        await Promise.all(
            this.connections.map(connection => connection.close()),
        );
        debug('shutdown-complete');
    }
}
