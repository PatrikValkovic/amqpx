import { Channel } from '../channel';
import { ExchangeImplementation } from './exchange-implementation';

export class DefaultExchange extends ExchangeImplementation {
    constructor(channel: Channel) {
        super(channel, '', 'direct', {
            durable: true,
        });
    }

    override async assert(): Promise<this> {
        await this.channel.connect();
        return this;
    }
}
