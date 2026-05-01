import { Channel } from '../channel';
import { AssertionMode } from '../types';
import { ExchangeImplementation } from './exchange-implementation';

/**
 * Class representing the default, direct exchange created by the RabbitMQ broker.
 * Every queue is automatically bound to this exchange under a routing key equal to the queue name.
 * The exchange cannot be declared or removed; broker interaction is skipped entirely.
 */
export class DefaultExchange extends ExchangeImplementation {
    constructor(channel: Channel) {
        super(channel, '', 'direct', {
            durable: true,
            assertionMode: AssertionMode.Passive,
        });
    }
}
