import { Exchange } from '../exchange';
import { ConsumerOptions, Consumer } from '../consumer';
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
     * Return name of the queue.
     * This call will also assert the queue if not asserted yet before it returns.
     */
    name(): Promise<string>;

    /**
     * Bind this queue to an exchange.
     * This call may assert the current queue if it's not asserted yet.
     * @param exchange Exchange to bind this queue to.
     * @param pattern Routing key under which to bind.
     * @param args
     */
    bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs): Promise<Queue>;

    /**
     * Create consumer for current queue.
     * Unless specified otherwise, uses the same channel as was used to declare the queue.
     * This is generally not recommended, as each producer/consumer should have separate channels.
     * Consider using `connection.createConsumerForQueue` method.
     */
    createConsumer<T>(options?: ConsumerOptions<T>): Promise<Consumer<T>>;

    /**
     * Create producer for current queue.
     * Publishes via the default RabbitMQ exchange, routing messages directly to this queue by name.
     * Unless specified otherwise, uses the same channel as was used to declare the queue.
     * This is generally not recommended, as each producer/consumer should have separate channels.
     * Consider using `connection.createProducerForQueue` method.
     */
    createProducer<T>(options?: ProducerOptions<T>): Promise<Producer<T>>;
}
