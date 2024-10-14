import { Exchange } from '../exchange';
import { ConsumerOptions, Consumer } from '../consumer';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { BindingArgs } from './types';

export interface Queue {
    assert(): Promise<Queue>;

    name(): Promise<string>;
    bind(exchange: Exchange, pattern: string, args?: BindingArgs): Promise<Queue>;

    /**
     * Create consumer for current queue.
     * Unless specified otherwise, uses the same channel as was used to declare the queue.
     * This is generally not recommended, as each producer/consumer should have separate channels.
     * Consider using `connection.createConsumerForQueue` method.
     */
    createConsumer<T>(options?: ConsumerOptions<T>): Promise<Consumer<T>>;

    /**
     * Create producer for current queue.
     * Unless specified otherwise, uses the same channel as was used to declare the queue.
     * This is generally not recommended, as each producer/consumer should have separate channels.
     * Consider using `connection.createProducerForQueue` method.
     */
    createProducer<T>(options?: ProducerOptions<T>): Promise<Producer<T>>;
}
