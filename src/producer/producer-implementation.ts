import { EventEmitter } from 'events';
import { Channel } from '../channel';
import { Exchange } from '../exchange';
import { deepMerge } from '../utils';
import { Producer, ProducerEventMap } from './producer';
import {
    DEFAULT_PRODUCER_OPTIONS,
    InFlightEntry,
    ProducerEvents,
    ProducerOptions,
    ProducerPublishOptions,
    RoutingKeyGenerator,
} from './types';

export class ProducerImplementation<T> extends EventEmitter<ProducerEventMap<T>> implements Producer<T> {
    private readonly options: Required<ProducerOptions<T>>;
    private readonly inFlight = new Set<InFlightEntry<T>>();
    private readonly interval: ReturnType<typeof setInterval>;
    private closed = false;

    constructor(
        private readonly channel: Channel,
        private readonly exchange: Exchange,
        options: ProducerOptions<T> = {},
    ) {
        super();
        this.options = deepMerge({}, DEFAULT_PRODUCER_OPTIONS, options) as typeof DEFAULT_PRODUCER_OPTIONS;

        this.channel.on('error', this.handleChannelError);

        this.interval = setInterval(() => {
            const now = performance.now();
            for (const entry of this.inFlight) {
                if (entry.expiresAt <= now)
                    this.inFlight.delete(entry);
            }
        }, Math.max(100, this.options.errorWindow));
    }

    async close(): Promise<void> {
        this.closed = true;
        clearInterval(this.interval);
        this.channel.off('error', this.handleChannelError);
    }

    private readonly handleChannelError = () => {
        const now = performance.now();
        for (const entry of this.inFlight) {
            if (entry.expiresAt >= now) {
                // Remove from in-flight before retrying; if the republish fails it will be
                // re-added to inFlight by the recursive publish() call.
                this.inFlight.delete(entry);
                this.publish(entry.message, entry.routingKey, entry.options)
                    .then(() => { /* ignore */ })
                    .catch(err => this.emit(ProducerEvents.republishFailed, entry.message, err));
            }
        }
    };

    async publish(
        message: T,
        routingKey?: RoutingKeyGenerator<T>,
        options?: ProducerPublishOptions,
    ) {
        if (this.closed)
            throw new Error('Producer is closed');

        const finalOptions = deepMerge({}, this.options, {
            ...(routingKey ? { routingKey } : {}),
            ...(options ? { options } : {}),
        }) as typeof this.options;
        const actualKeyPromise = typeof finalOptions.routingKey === 'string' ? finalOptions.routingKey : finalOptions.routingKey(message);
        const [actualKey, buffer, exchangeName] = await Promise.all([
            actualKeyPromise,
            this.options.stringifyMessage(message),
            this.exchange.name(),
        ]);

        this.emit(ProducerEvents.beforeSend, message, buffer);
        await this.channel.publish(
            exchangeName,
            actualKey,
            buffer,
            {
                retryStrategy: this.options.retryStrategy,
                drainTimeout: this.options.drainTimeout,
                ...finalOptions.options,
            },
        );

        // Global interval will remove this entry once the window expires.
        this.inFlight.add({
            message,
            routingKey,
            options,
            expiresAt: performance.now() + this.options.errorWindow,
        });

        this.emit(ProducerEvents.afterSend, message, buffer);

        return message;
    }

    getChannel() {
        return this.channel;
    }
}
