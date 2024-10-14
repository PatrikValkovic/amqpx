import { Channel } from '../channel';
import { Queue, QueueImplementation } from '../queue';
import { ConsumerOptions } from '../consumer';
import { ProducerOptions } from '../producer/types';
import { Producer, ProducerImplementation } from '../producer';
import { deepMerge } from '../utils';
import { BindingArgs, Binding, BindingType, ExchangeConsumerQueueOptions, ExchangeTypes, ExchangeOptions } from './types';
import { Exchange } from './exchange';

const DEFAULT_EXCHANGE_OPTIONS = {
    assert: true,
} as const satisfies ExchangeOptions;

export class ExchangeImplementation implements Exchange {
    private readonly options?: ExchangeOptions;
    private asserted = false;
    private bindings: Binding[] = [];

    constructor(
        protected readonly channel: Channel,
        private readonly exchangeName: string,
        private readonly exchangeType: ExchangeTypes,
        options?: ExchangeOptions,
    ) {
        this.options = deepMerge({}, DEFAULT_EXCHANGE_OPTIONS, options ?? {});
        this.channel.on('close', () => {
            this.asserted = false;
        });
    }

    async assert() {
        if (this.asserted)
            return this;
        const channel = await this.channel.native();
        if (this.options?.assert)
            await channel.assertExchange(this.exchangeName, this.exchangeType, this.options);
        else
            await channel.checkExchange(this.exchangeName);
        this.asserted = true;
        await this.rebind();
        return this;
    }

    async name() {
        return this.assert().then(self => self.exchangeName);
    }

    async bind(queueOrExchange: Queue | Exchange, pattern: string, args?: BindingArgs) {
        if (queueOrExchange instanceof QueueImplementation)
            return this.bindQueue(queueOrExchange, pattern, args);
        return this.bindExchange(queueOrExchange, pattern, args);
    }

    private async bindQueue(queue: Queue, pattern: string, args?: BindingArgs) {
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

    private async bindExchange(exchange: Exchange, pattern: string, args?: BindingArgs) {
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
        await Promise.all(
            this.bindings.map(
                binding => this.bind(
                    binding.type === BindingType.queue ? binding.queue : binding.exchange,
                    binding.pattern,
                    binding.args,
                ),
            ),
        );
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
