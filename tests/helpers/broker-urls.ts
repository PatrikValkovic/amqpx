import type * as amqp from 'amqplib';

export const DIRECT_URL = 'amqp://guest:guest@localhost:5672';
export const PROXIED_URL = 'amqp://guest:guest@localhost:25672';

export const DIRECT_OPTIONS: amqp.Options.Connect = {
    protocol: 'amqp',
    hostname: 'localhost',
    port: 5672,
    username: 'guest',
    password: 'guest',
};

export const PROXIED_OPTIONS: amqp.Options.Connect = {
    protocol: 'amqp',
    hostname: 'localhost',
    port: 25672,
    username: 'guest',
    password: 'guest',
};

