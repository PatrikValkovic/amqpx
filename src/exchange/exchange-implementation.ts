import { Channel } from '../channel';
import { Queue } from '../queue';
import { ConsumerOptions } from '../consumer';
import { ProducerOptions } from '../producer/types';
import { Producer, ProducerImplementation } from '../producer';
import { deepMerge } from '../utils';
import { BindingArgs, Binding, BindingType, ExchangeConsumerQueueOptions, ExchangeTypes, ExchangeOptions, ExchangeAssertionMode } from './types';
import { Exchange } from './exchange';

const DEFAULT_EXCHANGE_OPTIONS = {
    assertionMode: ExchangeAssertionMode.Assert,
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
            case ExchangeAssertionMode.Check:
                await channel.checkExchange(this.exchangeName);
                break;
            case ExchangeAssertionMode.Assert:
                await channel.assertExchange(this.exchangeName, this.exchangeType, this.options);
                break;
            case ExchangeAssertionMode.Passive:
                break;
            default:
                throw new Error(`Unknown assertion mode: ${this.options?.assertionMode}`);
            }
        })();

        await this.assertPromise;
        await this.rebind();
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
        return this;
    }

    async bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs) {
        const [thisName, exchangeName] = await Promise.all([
            this.name(),
            exchange.name(),
        ]);

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
        return this;
    }

    private async rebind() {
        const promises = await Promise.allSettled(
            this.bindings.map(
                binding => (binding.type === BindingType.queue
                    ? this.bindQueue(binding.queue, binding.pattern, binding.args)
                    : this.bindExchange(binding.exchange, binding.pattern, binding.args)),
            ),
        );
        const failedPromise = promises.find(p => p.status === 'rejected');
        if (failedPromise)
            throw failedPromise.reason;
    }

    async createConsumer<T>(options: ConsumerOptions<T> = {}, queueOptions: ExchangeConsumerQueueOptions = {}) {
        const queue = await this.channel.createQueue('', {
            ...queueOptions,
            durable: false,
            exclusive: true,
        });
        return queue.createConsumer(options);
    }

    createProducer<T>(options: ProducerOptions<T> = {}): Promise<Producer<T>> {
        return Promise.resolve(
            new ProducerImplementation(options.channel ?? this.channel, this, options),
        );
    }
}
