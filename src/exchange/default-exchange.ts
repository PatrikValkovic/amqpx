import { Channel } from '../channel';
import { ExchangeImplementation } from './exchange-implementation';

/**
 * Class representing default, direct exchange created by RabbitMQ broker.
 * The exchange can't be asserted, it is just checked for existence.
 * Every queue is bound to this exchange under routing key equal to queue name.
 */
export class DefaultExchange extends ExchangeImplementation {
    constructor(channel: Channel) {
        super(channel, '', 'direct', {
            durable: true,
        });
    }

    /**
     * Will just check if the exchange exists on the server.
     * Default exchange should be always present, and can't be asserted.
     *
     * @inheritDoc
     */
    override async assert(): Promise<this> {
        const nativeChannel = await this.channel.native();
        await nativeChannel.checkExchange('');
        return this;
    }
}
