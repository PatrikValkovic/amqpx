import { debuglog } from 'util';
import stableJsonStringify from 'safe-stable-stringify';
import { Channel } from '../channel';
import { Exchange } from '../exchange';
import { ConsumerImplementation, ConsumerOptions, Consumer, BatchConsumerImplementation, BatchConsumerOptions, BatchConsumer } from '../consumer';
import { ProducerOptions, ProducerImplementation } from '../producer';
import { predefined } from '../index';
import { deepMerge, errToMessage, LIB_NAME } from '../utils';
import { AssertionMode } from '../types';
import { Queue } from './queue';
import { BindingArgs, QueueBinding, QueueOptions } from './types';

const debug = debuglog(`${LIB_NAME}:queue`);

const DEFAULT_QUEUE_OPTIONS = {
    assertionMode: AssertionMode.Assert,
} as const satisfies QueueOptions;

export class QueueImplementation implements Queue {
    private readonly options?: QueueOptions;
    private assertPromise: Promise<void> | null = null;
    /** Queue name resolved by the last assert; null until then and after the channel closes. */
    private declaredName: string | null = null;
    private bindings: QueueBinding[] = [];

    constructor(
        private readonly channel: Channel,
        private readonly queueName: string,
        options?: QueueOptions,
    ) {
        this.options = deepMerge({}, DEFAULT_QUEUE_OPTIONS, options ?? {});
        this.channel.on('close', () => {
            debug('native-close');
            this.assertPromise = null;
            this.declaredName = null;
        });
    }

    async assert() {
        if (this.assertPromise) {
            await this.assertPromise;
            return this;
        }

        const promise = (async () => {
            debug('assert name=%s mode=%s', this.queueName, this.options?.assertionMode);
            const channel = await this.channel.native();
            switch (this.options?.assertionMode) {
            case AssertionMode.Check:
                await channel.checkQueue(this.queueName);
                this.declaredName = this.queueName;
                debug('native-check-done name=%s', this.queueName);
                break;
            case AssertionMode.Assert: {
                const reply = await channel.assertQueue(this.queueName, this.options);
                this.declaredName = reply.queue;
                debug('native-assert-done name=%s declared-name=%s', this.queueName, this.declaredName);
                break;
            }
            case AssertionMode.Passive:
                this.declaredName = this.queueName;
                debug('no-assert name=%s mode=%s', this.queueName, this.options.assertionMode);
                break;
            default:
                throw new Error(`Unknown assertion mode: ${this.options?.assertionMode}`);
            }
            await this.rebind();
            debug('assert-complete name=%s', this.currentName());
        })().catch((error: unknown) => {
            // Let the next assert() retry, unless a newer assert already replaced this one.
            if (this.assertPromise === promise) {
                this.assertPromise = null;
                this.declaredName = null;
            }
            throw error;
        });
        this.assertPromise = promise;

        await promise;
        return this;
    }

    async name() {
        return this.assert().then(self => self.currentName());
    }

    async bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs) {
        await this.assert();
        await this.nativeBind({ exchange, pattern, args });

        const isSameBinding = (binding: QueueBinding) =>
            binding.exchange === exchange &&
            binding.pattern === pattern &&
            stableJsonStringify(binding.args) === stableJsonStringify(args);
        if (this.bindings.every(binding => !isSameBinding(binding)))
            this.bindings.push({ exchange, pattern, args });

        return this;
    }

    /** Creates the binding on the broker; the queue must already be asserted. */
    private async nativeBind({ exchange, pattern, args }: QueueBinding) {
        const exchangeName = await exchange.name();
        const queueName = this.currentName();
        debug('bind-exchange queue=%s exchange=%s pattern=%s', queueName, exchangeName, pattern);
        const channel = await this.channel.native();
        await channel.bindQueue(queueName, exchangeName, pattern, args);
        debug('bind-exchange-done queue=%s exchange=%s pattern=%s', queueName, exchangeName, pattern);
    }

    private currentName() {
        return this.declaredName ?? this.queueName;
    }

    /** Re-applies the remembered bindings after the queue is asserted. */
    private async rebind() {
        if (this.bindings.length === 0)
            return;
        debug('rebind name=%s bindings=%d', this.currentName(), this.bindings.length);
        const results = await Promise.allSettled(
            this.bindings.map(binding => this.nativeBind(binding)),
        );
        const failed = results.find(result => result.status === 'rejected');
        if (failed) {
            debug('rebind-failed error=%s', errToMessage(failed.reason));
            throw failed.reason;
        }
        debug('rebind-complete name=%s', this.currentName());
    }

    createConsumer<T>(options: ConsumerOptions<T> = {}): Promise<Consumer<T>> {
        return Promise.resolve(
            new ConsumerImplementation(options.channel ?? this.channel.connection.createChannel(), this, options),
        );
    }

    createBatchConsumer<T>(options: BatchConsumerOptions<T> = {}): Promise<BatchConsumer<T>> {
        return Promise.resolve(
            new BatchConsumerImplementation(options.channel ?? this.channel.connection.createChannel(), this, options),
        );
    }

    createProducer<T>(options: ProducerOptions<T> & { isConfirmed?: boolean } = {}) {
        const exchange = predefined.defaultExchange(this.channel);
        return Promise.resolve(
            new ProducerImplementation(options.channel ?? this.channel.connection.createChannel(options.isConfirmed), exchange, {
                ...options,
                routingKey: () => this.name(),
            }),
        );
    }
}
