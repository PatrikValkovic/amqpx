import * as amqp from 'amqplib';
import { Channel } from '../channel';
import { Exchange } from '../exchange';
import { ConsumerImplementation, ConsumerOptions, Consumer } from '../consumer';
import { ProducerOptions } from '../producer/types';
import { ProducerImplementation } from '../producer';
import { predefined } from '../../index';
import { Queue } from './queue';
import { BindingArgs } from './types';

export class QueueImplementation implements Queue {
    private asserted = false;

    constructor(
        private readonly channel: Channel,
        private readonly queueName: string,
        private readonly options?: amqp.Options.AssertQueue,
    ) {
        this.channel.on('close', () => {
            this.asserted = false;
        });
    }

    async assert() {
        if (this.asserted)
            return this;
        const channel = await this.channel.native();
        await channel.assertQueue(this.queueName, this.options);
        this.asserted = true;
        return this;
    }

    async name() {
        return this.assert().then(self => self.queueName);
    }

    async bind(exchange: Exchange, pattern: string, args?: BindingArgs) {
        await exchange.bind(this, pattern, args);
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
                routingKey: async () => await this.name(),
            }),
        );
    }

}
