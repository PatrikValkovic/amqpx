import { Queue } from '../queue';
import { ConsumerOptions, Consumer } from '../consumer';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { ExchangeConsumerQueueOptions, BindingArgs } from './types';

export interface Exchange {
    assert(): Promise<Exchange>;

    name(): Promise<string>;
    bind(queueOrExchange: Queue | Exchange, pattern: string, args?: BindingArgs): Promise<Exchange>;

    /**
     * Create consumer for current exchange.
     * It will create exclusive queue and bind it to the exchange.
     * Unless specified otherwise, uses the same channel as was used to declare the exchange.
     * This is generally not recommended, as each producer/consumer should have separate channels.
     * Consider using `connection.createConsumerForExchange` method.
     */
    createConsumer<T>(options?: ConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;

    /**
     * Create producer for current exchange.
     * Unless specified otherwise, uses the same channel as was used to declare the exchange.
     * This is generally not recommended, as each producer/consumer should have separate channels.
     * Consider using `connection.createProducerForExchange` method.
     */
    createProducer<T>(options?: ProducerOptions<T>): Promise<Producer<T>>;
}
