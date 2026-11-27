import { TestChannel } from './extensions/vitest';
import { ExchangeImplementation } from './exchange';
import { QueueImplementation } from './queue';
import { DefaultExchange } from './exchange/default-exchange';
import { AssertionMode } from './types';
import * as predefined from './predefined';

describe('predefined', () => {
    let channel: TestChannel;

    beforeEach(() => {
        channel = new TestChannel();
    });

    describe.each([
        ['directExchange', predefined.directExchange, 'amq.direct', 'direct'],
        ['fanoutExchange', predefined.fanoutExchange, 'amq.fanout', 'fanout'],
        ['headersExchange', predefined.headersExchange, 'amq.headers', 'headers'],
        ['matchExchange', predefined.matchExchange, 'amq.match', 'headers'],
        ['topicExchange', predefined.topicExchange, 'amq.topic', 'topic'],
    ] as const)('%s', (_name, factory, expectedName, expectedType) => {
        it(`should be an exchange named ${expectedName}`, async () => {
            const exchange = factory(channel);

            expect(exchange).toBeInstanceOf(ExchangeImplementation);
            expect(await exchange.name()).toBe(expectedName);
        });

        it(`should have type ${expectedType}, be durable and use Passive assertion mode`, () => {
            const exchange = factory(channel);

            // @ts-expect-error private fields
            expect(exchange.exchangeType).toBe(expectedType);
            // @ts-expect-error private fields
            expect(exchange.options).toEqual(expect.objectContaining({
                durable: true,
                assertionMode: AssertionMode.Passive,
            }));
        });

        it('should not declare or check the exchange on the broker', async () => {
            const exchange = factory(channel);

            await exchange.assert();
            await exchange.name();

            expect(channel.nativeChannel.assertExchange).not.toHaveBeenCalled();
            expect(channel.nativeChannel.checkExchange).not.toHaveBeenCalled();
        });

        it('should bind a queue under its own name', async () => {
            const exchange = factory(channel);

            await exchange.bindQueue(new QueueImplementation(channel, 'test-queue'), 'routing.key', { type: 'a' });

            expect(channel.nativeChannel.bindQueue).toHaveBeenCalledWith('test-queue', expectedName, 'routing.key', { type: 'a' });
            expect(channel.nativeChannel.assertExchange).not.toHaveBeenCalled();
        });
    });

    describe('defaultExchange', () => {
        it('should be the passive default exchange with an empty name', async () => {
            const exchange = predefined.defaultExchange(channel);

            expect(exchange).toBeInstanceOf(DefaultExchange);
            expect(await exchange.name()).toBe('');
            expect(channel.nativeChannel.assertExchange).not.toHaveBeenCalled();
        });
    });
});
