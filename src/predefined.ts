import { ExchangeImplementation, Exchange } from './exchange';
import { Channel } from './channel';
import { DefaultExchange } from './exchange/default-exchange';

export const directExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.direct',
    'direct',
    {
        durable: true,
    },
);

export const fanoutExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.fanout',
    'fanout',
    {
        durable: true,
    },
);

export const headersExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.headers',
    'headers',
    {
        durable: true,
    },
);

export const matchExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.match',
    'match',
    {
        durable: true,
    },
);

export const topicExchange = (channel: Channel): Exchange => new ExchangeImplementation(
    channel,
    'amqp.topic',
    'topic',
    {
        durable: true,
    },
);

export const defaultExchange = (channel: Channel): Exchange => new DefaultExchange(channel);
