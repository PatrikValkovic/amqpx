import { Queue } from '../queue';
import { ConsumerOptions, Consumer } from '../consumer';
import { ProducerOptions } from '../producer/types';
import { Producer } from '../producer';
import { ExchangeConsumerQueueOptions, BindingArgs } from './types';

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
     * Return name of the exchange.
     * This call will also assert the exchange if not asserted yet before it returns.
     */
    name(): Promise<string>;

    /**
     * Bind queue or exchange to this exchange.
     * This call may assert current exchange if it's not asserted yet.
     * @param queueOrExchange Queue or exchange to bind to the current exchange.
     * @param pattern Routing key under which to bind.
     * @param args
     */
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
