import { debuglog } from 'util';
import { EventEmitter } from 'events';
import { Channel } from '../channel';
import { Exchange } from '../exchange';
import { deepMerge, errToMessage, LIB_NAME } from '../utils';
import { Producer, ProducerEventMap } from './producer';
import {
    DEFAULT_PRODUCER_OPTIONS,
    InFlightEntry,
    ProducerOptions,
    ProducerPublishOptions,
    RoutingKeyGenerator,
} from './types';

const debug = debuglog(`${LIB_NAME}:publish`);

export class ProducerImplementation<T> extends EventEmitter<ProducerEventMap<T>> implements Producer<T> {
    private readonly options: Required<ProducerOptions<T>>;
    private readonly inFlight = new Set<InFlightEntry<T>>();
    private readonly pendingPublishes = new Set<Promise<boolean>>();
    private readonly interval: ReturnType<typeof setInterval>;
    private closed = false;

    constructor(
        public readonly channel: Channel,
        private readonly exchange: Exchange,
        options: ProducerOptions<T> = {},
    ) {
        super();
        this.options = deepMerge({}, DEFAULT_PRODUCER_OPTIONS, options) as typeof DEFAULT_PRODUCER_OPTIONS;

        this.channel.on('error', this.handleChannelError);

        this.interval = setInterval(() => {
            let deleted = 0;
            const now = performance.now();
            for (const entry of this.inFlight) {
                if (entry.expiresAt <= now) {
                    this.inFlight.delete(entry);
                    deleted++;
                }
            }
            if (deleted > 0)
                debug('deleted-in-flight deleted=%d remaining=%d', deleted, this.inFlight.size);
        }, Math.max(100, this.options.errorWindow));
        this.interval.unref();
    }

    async close(): Promise<void> {
        debug('closing');
        this.closed = true;
        await Promise.allSettled(this.pendingPublishes);
        clearInterval(this.interval);
        this.channel.off('error', this.handleChannelError);
    }

    private readonly handleChannelError = () => {
        const now = performance.now();
        debug('channel-error in-flight=%d', this.inFlight.size);
        for (const entry of this.inFlight) {
            if (entry.expiresAt > now) {
                // Remove from in-flight before retrying; if the republish fails it will be
                // re-added to inFlight by the recursive publish() call.
                this.inFlight.delete(entry);
                this.publish(entry.message, entry.routingKey, entry.options)
                    .then(() => { /* ignore */ })
                    .catch(err => {
                        debug('republish-failed error=%s', errToMessage(err));
                        this.emit('republishFailed', entry.message, err);
                    });
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

        debug('publish exchange=%s routing-key=%s', exchangeName, actualKey);
        this.emit('beforeSend', message, buffer);

        const publishPromise = this.channel.publish(
            exchangeName,
            actualKey,
            buffer,
            {
                retryStrategy: this.options.retryStrategy,
                drainTimeout: this.options.drainTimeout,
                ...finalOptions.options,
            },
        );
        this.pendingPublishes.add(publishPromise);
        try {
            await publishPromise;
        } finally {
            this.pendingPublishes.delete(publishPromise);
        }
        const isConfirmed = await publishPromise;
        debug('published exchange=%s routing-key=%s', exchangeName, actualKey);

        if (!isConfirmed) {
            // Global interval will remove this entry once the window expires.
            this.inFlight.add({
                message,
                routingKey,
                options,
                expiresAt: performance.now() + this.options.errorWindow,
            });
        }

        this.emit('afterSend', message, buffer);

        return message;
    }
}
