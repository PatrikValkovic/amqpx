import { debuglog } from 'util';
import { Channel } from '../channel';
import { Queue } from '../queue';
import { ConsumerOptions, BatchConsumerOptions } from '../consumer';
import { ProducerOptions, Producer, ProducerImplementation } from '../producer';
import { deepMerge, errToMessage, LIB_NAME } from '../utils';
import { AssertionMode } from '../types';
import { BindingArgs, Binding, BindingType, ExchangeConsumerQueueOptions, ExchangeTypes, ExchangeOptions } from './types';
import { Exchange } from './exchange';

const debug = debuglog(`${LIB_NAME}:exchange`);

const DEFAULT_EXCHANGE_OPTIONS = {
    assertionMode: AssertionMode.Assert,
} as const satisfies ExchangeOptions;

export class ExchangeImplementation implements Exchange {
    private readonly options?: ExchangeOptions;
    private assertPromise: Promise<void> | null = null;
    private bindings: Binding[] = [];

    constructor(
        protected readonly channel: Channel,
        private readonly exchangeName: string,
        private readonly exchangeType: ExchangeTypes,
        options?: ExchangeOptions,
    ) {
        this.options = deepMerge({}, DEFAULT_EXCHANGE_OPTIONS, options ?? {});
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
            debug('assert name=%s type=%s mode=%s', this.exchangeName, this.exchangeType, this.options?.assertionMode);
            const channel = await this.channel.native();
            switch (this.options?.assertionMode) {
            case AssertionMode.Check:
                await channel.checkExchange(this.exchangeName);
                debug('native-check-done name=%s', this.exchangeName);
                break;
            case AssertionMode.Assert:
                await channel.assertExchange(this.exchangeName, this.exchangeType, this.options);
                debug('native-assert-done name=%s type=%s', this.exchangeName, this.exchangeType);
                break;
            case AssertionMode.Passive:
                debug('no-assert name=%s mode=%s', this.exchangeName, this.options.assertionMode);
                break;
            default:
                throw new Error(`Unknown assertion mode: ${this.options?.assertionMode}`);
            }
        })();

        await this.assertPromise;
        await this.rebind();
        debug('assert-complete name=%s', this.exchangeName);
        return this;
    }

    async name() {
        return this.assert().then(self => self.exchangeName);
    }

    async bindQueue(queue: Queue, pattern: string, args?: BindingArgs) {
        const [thisName, queueName] = await Promise.all([
            this.name(),
            queue.name(),
        ]);
        debug('bind-queue exchange=%s queue=%s pattern=%s', thisName, queueName, pattern);

        const doesBindingMatch = (binding: Binding) =>
            binding.type === BindingType.queue &&
            binding.pattern === pattern &&
            binding.queue === queue;

        if (this.bindings.every(binding => !doesBindingMatch(binding))) {
            this.bindings.push({
                args,
                pattern,
                type: BindingType.queue,
                queue,
            });
        }

        const channel = await this.channel.native();
        await channel.bindQueue(queueName, thisName, pattern, args);
        debug('bind-queue-done exchange=%s queue=%s pattern=%s', thisName, queueName, pattern);
        return this;
    }

    async bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs) {
        const [thisName, exchangeName] = await Promise.all([
            this.name(),
            exchange.name(),
        ]);
        debug('bind-exchange dest=%s source=%s pattern=%s', thisName, exchangeName, pattern);

        const doesBindingMatch = (binding: Binding) =>
            binding.type === BindingType.exchange &&
            binding.pattern === pattern &&
            binding.exchange === exchange;

        if (this.bindings.every(binding => !doesBindingMatch(binding))) {
            this.bindings.push({
                args,
                pattern,
                type: BindingType.exchange,
                exchange,
            });
        }

        const channel = await this.channel.native();
        await channel.bindExchange(exchangeName, thisName, pattern, args);
        debug('bind-exchange-done dest=%s source=%s pattern=%s', thisName, exchangeName, pattern);
        return this;
    }

    private async rebind() {
        debug('rebind name=%s bindings=%d', this.exchangeName, this.bindings.length);
        const promises = await Promise.allSettled(
            this.bindings.map(
                binding => (binding.type === BindingType.queue
                    ? this.bindQueue(binding.queue, binding.pattern, binding.args)
                    : this.bindExchange(binding.exchange, binding.pattern, binding.args)),
            ),
        );

        const failedPromise = promises.find(p => p.status === 'rejected');
        if (failedPromise) {
            debug('rebind-failed error=%s', errToMessage(failedPromise.reason));
            throw failedPromise.reason;
        }
        debug('rebind-complete name=%s', this.exchangeName);
    }

    async createConsumer<T>(options: ConsumerOptions<T> = {}, queueOptions: ExchangeConsumerQueueOptions = {}) {
        const queue = await this.channel.createQueue('', {
            ...queueOptions,
            durable: false,
            exclusive: true,
        });
        return queue.createConsumer(options);
    }

    async createBatchConsumer<T>(options: BatchConsumerOptions<T> = {}, queueOptions: ExchangeConsumerQueueOptions = {}) {
        const queue = await this.channel.createQueue('', {
            ...queueOptions,
            durable: false,
            exclusive: true,
        });
        return queue.createBatchConsumer(options);
    }

    createProducer<T>(options: ProducerOptions<T> = {}): Promise<Producer<T>> {
        return Promise.resolve(
            new ProducerImplementation(options.channel ?? this.channel, this, options),
        );
    }
}
