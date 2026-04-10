import { Connection } from '../connection';
import { Consumer } from '../consumer';
import { Producer } from '../producer';

export class RabbitCloser {
    constructor(
        private readonly connections: Connection[],
        private readonly consumers: Consumer<unknown>[],
        private readonly producers: Producer<unknown>[],
    ) {
    }

    async close(): Promise<void> {
        await Promise.all(
            this.producers.map(async producer => {
                await producer.close();
                await producer.getChannel().close();
            }),
        );
        await Promise.all(
            this.consumers.map(async consumer => {
                await consumer.close();
                await consumer.getChannel().close();
            }),
        );
        await Promise.all(
            this.connections.map(connection => connection.close()),
        );
    }
}
