import { TestConnection, TestExchange, TestQueue } from '.';

describe('TestConnection', () => {
    let connection: TestConnection;

    beforeEach(() => {
        connection = new TestConnection();
    });

    describe('createExchange', () => {
        it('should resolve a TestExchange and record the call when called', async () => {
            const exchange = await connection.createExchange('orders', 'topic', { durable: true });

            expect(exchange).toBeInstanceOf(TestExchange);
            expect(connection.createExchange).toHaveBeenCalledWith('orders', 'topic', { durable: true });
        });
    });

    describe('createQueue', () => {
        it('should resolve a TestQueue and record the call when called', async () => {
            const queue = await connection.createQueue('jobs', { durable: true });

            expect(queue).toBeInstanceOf(TestQueue);
            expect(connection.createQueue).toHaveBeenCalledWith('jobs', { durable: true });
        });
    });

    describe('checkQueue', () => {
        it('should resolve empty queue metadata for the given name when called', async () => {
            const reply = await connection.checkQueue('jobs');

            expect(reply).toEqual({ queue: 'jobs', messageCount: 0, consumerCount: 0 });
            expect(connection.checkQueue).toHaveBeenCalledWith('jobs');
        });

        it('should resolve the overridden value when the mock is overridden', async () => {
            connection.checkQueue.mockResolvedValueOnce({ queue: 'jobs', messageCount: 5, consumerCount: 2 });

            const reply = await connection.checkQueue('jobs');

            expect(reply.messageCount).toBe(5);
        });
    });
});
