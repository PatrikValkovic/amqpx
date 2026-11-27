import { TestConsumer } from '.';

type Order = { orderId: string; total: number };

const firstCallArg = (handler: ReturnType<typeof vi.fn>) => {
    const [call] = handler.mock.calls;
    if (!call)
        throw new Error('the listener was not called');
    return call[0];
};

describe('TestConsumer', () => {
    let consumer: TestConsumer<Order>;
    const order: Order = { orderId: 'abc', total: 49.99 };

    beforeEach(() => {
        consumer = new TestConsumer<Order>();
    });

    describe('deliverMessage', () => {
        it('should call the listener with the payload and default message fields when no options are given', async () => {
            const handler = vi.fn();
            await consumer.listen(handler);

            await consumer.deliverMessage(order);

            expect(handler).toHaveBeenCalledTimes(1);
            const { message, rabbitMessage } = firstCallArg(handler);
            expect(message).toEqual(order);
            expect(rabbitMessage.content).toEqual(Buffer.from(JSON.stringify(order)));
            expect(rabbitMessage.fields).toEqual({
                deliveryTag: 1,
                redelivered: false,
                exchange: '',
                routingKey: '',
                consumerTag: '',
            });
            expect(rabbitMessage.properties).toEqual({ headers: {} });
        });

        it('should override only the given message fields and properties when partial ones are passed', async () => {
            const handler = vi.fn();
            await consumer.listen(handler);

            // the example from the testing guide
            await consumer.deliverMessage(order, {
                fields: { routingKey: 'orders.priority', redelivered: false },
                properties: { headers: { source: 'checkout' } },
            });

            const { rabbitMessage } = firstCallArg(handler);
            expect(rabbitMessage.fields).toEqual({
                deliveryTag: 1,
                redelivered: false,
                exchange: '',
                routingKey: 'orders.priority',
                consumerTag: '',
            });
            expect(rabbitMessage.properties).toEqual({ headers: { source: 'checkout' } });
        });

        it('should serialize the payload with the given function when serialize is passed', async () => {
            const handler = vi.fn();
            await consumer.listen(handler);

            await consumer.deliverMessage(order, { serialize: msg => Buffer.from(msg.orderId) });

            expect(firstCallArg(handler).rabbitMessage.content).toEqual(Buffer.from('abc'));
        });

        it('should reject when no listener is registered', async () => {
            await expect(consumer.deliverMessage(order)).rejects.toThrow('No listener registered');
        });
    });
});
