import { Exchange } from '../exchange';
import { ConsumerOptions, Consumer, BatchConsumerOptions, BatchConsumer } from '../consumer';
import { ProducerOptions, Producer } from '../producer';
import { BindingArgs } from './types';

export interface Queue {
    /**
     * Verify the queue against the broker, according to the configured `assertionMode`.
     *
     * - `Assert` (default) — declare the queue; creates it if absent, throws on config mismatch.
     * - `Check` — verify the queue exists without creating it; throws if absent.
     * - `Passive` — no-op; no network call is made.
     *
     * Subsequent calls return immediately once the first call completes successfully.
     * Returns `this` to allow chaining.
     */
    assert(): Promise<Queue>;

    /**
     * Returns the name of the queue.
     * Asserts the queue first if it has not been asserted yet.
     */
    name(): Promise<string>;

    /**
     * Bind this queue to an exchange.
     * Asserts the queue first if it has not been asserted yet.
     * @param exchange - Exchange to bind this queue to.
     * @param pattern - Routing key pattern under which to bind.
     * @param args - Optional AMQP headers or x-arguments for the binding.
     */
    bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs): Promise<Queue>;

    /**
     * Create consumer for current queue.
     * By default, creates a new dedicated channel for the consumer.
     * Pass a channel in `options` to use a specific one instead.
     */
    createConsumer<T>(options?: ConsumerOptions<T>): Promise<Consumer<T>>;

    /**
     * Create batch consumer for current queue.
     * By default, creates a new dedicated channel for the consumer.
     * Pass a channel in `options` to use a specific one instead.
     */
    createBatchConsumer<T>(options?: BatchConsumerOptions<T>): Promise<BatchConsumer<T>>;

    /**
     * Create producer for current queue.
     * Publishes via the default RabbitMQ exchange, routing messages directly to this queue by name.
     * By default, creates a new dedicated channel for the producer.
     * Pass a channel in `options` to use a specific one instead.
     */
    createProducer<T>(options?: ProducerOptions<T>): Promise<Producer<T>>;
}
