import { Channel } from '../channel';
import { Exchange } from '../exchange';
import { ConsumerImplementation, ConsumerOptions, Consumer } from '../consumer';
import { ProducerOptions } from '../producer/types';
import { ProducerImplementation } from '../producer';
import { predefined } from '../index';
import { deepMerge } from '../utils';
import { AssertionMode } from '../types';
import { Queue } from './queue';
import { BindingArgs, QueueOptions } from './types';

const DEFAULT_QUEUE_OPTIONS = {
    assertionMode: AssertionMode.Assert,
} as const satisfies QueueOptions;

export class QueueImplementation implements Queue {
    private readonly options?: QueueOptions;
    private assertPromise: Promise<void> | null = null;

    constructor(
        private readonly channel: Channel,
        private readonly queueName: string,
        options?: QueueOptions,
    ) {
        this.options = deepMerge({}, DEFAULT_QUEUE_OPTIONS, options ?? {});
        this.channel.on('close', () => {
            this.assertPromise = null;
        });
    }

    async assert() {
        if (this.assertPromise) {
            await this.assertPromise;
            return this;
        }

        this.assertPromise = (async () => {
            const channel = await this.channel.native();
            switch (this.options?.assertionMode) {
            case AssertionMode.Check:
                await channel.checkQueue(this.queueName);
                break;
            case AssertionMode.Assert:
                await channel.assertQueue(this.queueName, this.options);
                break;
            case AssertionMode.Passive:
                break;
            default:
                throw new Error(`Unknown assertion mode: ${this.options?.assertionMode}`);
            }
        })();

        await this.assertPromise;
        return this;
    }

    async name() {
        return this.assert().then(self => self.queueName);
    }

    async bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs) {
        await exchange.bindQueue(this, pattern, args);
        return this;
    }

    createConsumer<T>(options: ConsumerOptions<T> = {}): Promise<Consumer<T>> {
        return Promise.resolve(
            new ConsumerImplementation(options.channel ?? this.channel, this, options),
        );
    }

    createProducer<T>(options: ProducerOptions<T> = {}) {
        const exchange = predefined.defaultExchange(this.channel);
        return Promise.resolve(
            new ProducerImplementation(options.channel ?? this.channel, exchange, {
                ...options,
                routingKey: () => this.name(),
            }),
        );
    }
}
