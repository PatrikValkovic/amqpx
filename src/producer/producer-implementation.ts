import { EventEmitter } from 'events';
import { Channel } from '../channel';
import { Exchange } from '../exchange';
import { deepMerge } from '../utils';
import { Producer, ProducerEventMap } from './producer';
import {
    DEFAULT_PRODUCER_OPTIONS,
    ProducerEvents,
    ProducerOptions,
    ProducerPublishOptions,
    RoutingKeyGenerator,
} from './types';

export class ProducerImplementation<T> extends EventEmitter<ProducerEventMap<T>> implements Producer<T> {
    private readonly options: Required<ProducerOptions<T>>;

    constructor(
        private readonly channel: Channel,
        private readonly exchange: Exchange,
        options: ProducerOptions<T> = {},
    ) {
        super();
        this.options = deepMerge({}, DEFAULT_PRODUCER_OPTIONS, options) as typeof DEFAULT_PRODUCER_OPTIONS;
    }

    async publish(
        message: T,
        routingKey?: RoutingKeyGenerator<T>,
        options?: ProducerPublishOptions,
    ) {
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

        // Following block tries to resend the message if there is error on the channel.
        // This may result in more-than-once delivery, but in distributed system it should be OK.
        // Because there is no direct feedback from Rabbit (for non-confirmed channels),
        // we setup 5s window within which all the messages are considered failed and app will try to resend them.
        let publishWithError = false;
        let publishErrorTimer: NodeJS.Timeout;
        const publishErrorHandler = () => {
            publishWithError = true;
            if (publishErrorTimer)
                clearTimeout(publishErrorTimer);
            this.channel.off('error', publishErrorHandler);
            this.publish(message, routingKey, options)
                .then(() => { /* ignore */ })
                .catch(() => { /* ignore */ });
        };
        publishErrorTimer = setTimeout(() => {
            if (!publishWithError)
                this.channel.off('error', publishErrorHandler);
        }, this.options.errorWindow);
        this.channel.once('error', publishErrorHandler);

        await this.channel.publish(
            exchangeName,
            actualKey,
            buffer,
            {
                ...finalOptions.options,
                drainTimeout: this.options.drainTimeout,
            },
        );

        return message;
    }

    getChannel() {
        return this.channel;
    }
}
