import { TestBatchConsumer } from '.';

type Order = { orderId: string; total: number };

const firstCallArg = (handler: ReturnType<typeof vi.fn>) => {
    const [call] = handler.mock.calls;
    if (!call)
        throw new Error('the listener was not called');
    return call[0];
};

describe('TestBatchConsumer', () => {
    let consumer: TestBatchConsumer<Order>;
    const orders: Order[] = [
        { orderId: '1', total: 10 },
        { orderId: '2', total: 20 },
    ];

    beforeEach(() => {
        consumer = new TestBatchConsumer<Order>();
    });

    describe('deliverMessages', () => {
        it('should call the listener once with all payloads and default message fields when no options are given', async () => {
            const handler = vi.fn();
            await consumer.listen(handler);

            await consumer.deliverMessages(orders);

            expect(handler).toHaveBeenCalledTimes(1);
            const { messages } = firstCallArg(handler);
            expect(messages.map((entry: { message: Order }) => entry.message)).toEqual(orders);
            expect(messages[0].rabbitMessage.fields).toEqual({
                deliveryTag: 1,
                redelivered: false,
                exchange: '',
                routingKey: '',
                consumerTag: '',
            });
        });

        it('should apply partial message fields and properties to every message when they are passed', async () => {
            const handler = vi.fn();
            await consumer.listen(handler);

            await consumer.deliverMessages(orders, {
                fields: { routingKey: 'orders.priority' },
                properties: { headers: { source: 'checkout' } },
            });

            const { messages } = firstCallArg(handler);
            for (const entry of messages) {
                expect(entry.rabbitMessage.fields.routingKey).toBe('orders.priority');
                expect(entry.rabbitMessage.fields.deliveryTag).toBe(1);
                expect(entry.rabbitMessage.properties).toEqual({ headers: { source: 'checkout' } });
            }
        });

        it('should reject when no listener is registered', async () => {
            await expect(consumer.deliverMessages(orders)).rejects.toThrow('No listener registered');
        });
    });
});
