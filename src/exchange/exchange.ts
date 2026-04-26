import { Queue } from '../queue';
import { Consumer, BatchConsumer } from '../consumer';
import { ProducerOptions, Producer } from '../producer';
import { ExchangeConsumerQueueOptions, ExchangeConsumerOptions, ExchangeBatchConsumerOptions, BindingArgs } from './types';

export interface Exchange {
    /**
     * Verify the exchange against the broker, according to the configured `assertionMode`.
     *
     * - `Assert` (default) — declare the exchange; creates it if absent, throws on config mismatch.
     * - `Check` — verify the exchange exists without creating it; throws if absent.
     * - `Passive` — no-op; no network call is made.
     *
     * Subsequent calls return immediately once the first call completes successfully.
     * Returns `this` to allow chaining.
     */
    assert(): Promise<Exchange>;

    /**
     * Returns the name of the exchange.
     * Asserts the exchange first if it has not been asserted yet.
     */
    name(): Promise<string>;

    /**
     * Bind a queue to this exchange.
     * Asserts the exchange first if it has not been asserted yet.
     * @param queue - Queue to bind to this exchange.
     * @param pattern - Routing key pattern under which to bind.
     * @param args - Optional AMQP headers or x-arguments for the binding.
     */
    bindQueue(queue: Queue, pattern: string, args?: BindingArgs): Promise<Exchange>;

    /**
     * Bind another exchange to this exchange.
     * Asserts the exchange first if it has not been asserted yet.
     * @param exchange - Exchange to bind to this exchange.
     * @param pattern - Routing key pattern under which to bind.
     * @param args - Optional AMQP headers or x-arguments for the binding.
     */
    bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs): Promise<Exchange>;

    /**
     * Creates a consumer for the current exchange.
     * Internally asserts an exclusive queue and binds it to the exchange.
     * Unless specified otherwise, uses the same channel as was used to declare the exchange.
     * This is generally not recommended, as each consumer should have its own dedicated channel.
     * Consider using `connection.createConsumerForExchange` instead.
     */
    createConsumer<T>(options: ExchangeConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;

    /**
     * Creates a batch consumer for the current exchange.
     * Internally asserts an exclusive queue and binds it to the exchange.
     * Unless specified otherwise, uses the same channel as was used to declare the exchange.
     * This is generally not recommended, as each consumer should have its own dedicated channel.
     * Consider using `connection.createBatchConsumerForExchange` instead.
     */
    createBatchConsumer<T>(options: ExchangeBatchConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<BatchConsumer<T>>;

    /**
     * Creates a producer for the current exchange.
     * Unless specified otherwise, uses the same channel as was used to declare the exchange.
     * This is generally not recommended, as each producer should have its own dedicated channel.
     * Consider using `connection.createProducerForExchange` instead.
     */
    createProducer<T>(options?: ProducerOptions<T>): Promise<Producer<T>>;
}
