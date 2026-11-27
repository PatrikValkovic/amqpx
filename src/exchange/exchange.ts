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
     * Asserts the queue and the exchange first if they have not been asserted yet.
     * Equivalent to `queue.bindExchange(exchange, pattern, args)`.
     * The binding is restored when the queue is asserted again after a reconnect.
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
     * The consumer reads from a temporary exclusive queue bound to the exchange under `options.pattern`
     * and keeps receiving messages after a reconnect.
     * By default, creates a new dedicated channel for the consumer.
     * Pass a channel in `options` to use a specific one instead.
     */
    createConsumer<T>(options: ExchangeConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<Consumer<T>>;

    /**
     * Creates a batch consumer for the current exchange.
     * The consumer reads from a temporary exclusive queue bound to the exchange under `options.pattern`
     * and keeps receiving messages after a reconnect.
     * By default, creates a new dedicated channel for the consumer.
     * Pass a channel in `options` to use a specific one instead.
     */
    createBatchConsumer<T>(options: ExchangeBatchConsumerOptions<T>, queueOptions?: ExchangeConsumerQueueOptions): Promise<BatchConsumer<T>>;

    /**
     * Creates a producer for the current exchange.
     * By default, creates a new dedicated channel for the producer.
     * Pass a channel in `options` to use a specific one instead.
     *
     * @param options - Optional producer options (serialization, routing key, hooks, …).
     *   Set `options.isConfirmed` to `true` to open the dedicated channel in confirm mode.
     *   Has no effect when `options.channel` is supplied.
     */
    createProducer<T>(options?: ProducerOptions<T> & { isConfirmed?: boolean }): Promise<Producer<T>>;
}
