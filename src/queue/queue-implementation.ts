import { debuglog } from 'util';
import { Channel } from '../channel';
import { Exchange } from '../exchange';
import { ConsumerImplementation, ConsumerOptions, Consumer, BatchConsumerImplementation, BatchConsumerOptions, BatchConsumer } from '../consumer';
import { ProducerOptions, ProducerImplementation } from '../producer';
import { predefined } from '../index';
import { deepMerge, LIB_NAME } from '../utils';
import { AssertionMode } from '../types';
import { Queue } from './queue';
import { BindingArgs, QueueOptions } from './types';

const debug = debuglog(`${LIB_NAME}:queue`);

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
            debug('native-close');
            this.assertPromise = null;
        });
    }

    async assert() {
        if (this.assertPromise) {
            await this.assertPromise;
            return this;
        }

        this.assertPromise = (async () => {
            debug('assert name=%s mode=%s', this.queueName, this.options?.assertionMode);
            const channel = await this.channel.native();
            switch (this.options?.assertionMode) {
            case AssertionMode.Check:
                await channel.checkQueue(this.queueName);
                debug('native-check-done name=%s', this.queueName);
                break;
            case AssertionMode.Assert:
                await channel.assertQueue(this.queueName, this.options);
                debug('native-assert-done name=%s', this.queueName);
                break;
            case AssertionMode.Passive:
                debug('no-assert name=%s mode=%s', this.queueName, this.options.assertionMode);
                break;
            default:
                throw new Error(`Unknown assertion mode: ${this.options?.assertionMode}`);
            }
        })();

        await this.assertPromise;
        debug('assert-complete name=%s', this.queueName);
        return this;
    }

    async name() {
        return this.assert().then(self => self.queueName);
    }

    async bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs) {
        debug('bind-exchange queue=%s pattern=%s', this.queueName, pattern);
        await exchange.bindQueue(this, pattern, args);
        return this;
    }

    createConsumer<T>(options: ConsumerOptions<T> = {}): Promise<Consumer<T>> {
        return Promise.resolve(
            new ConsumerImplementation(options.channel ?? this.channel, this, options),
        );
    }

    createBatchConsumer<T>(options: BatchConsumerOptions<T> = {}): Promise<BatchConsumer<T>> {
        return Promise.resolve(
            new BatchConsumerImplementation(options.channel ?? this.channel, this, options),
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
