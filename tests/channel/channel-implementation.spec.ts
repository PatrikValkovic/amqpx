import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConnectionImplementation } from '../../src';
import { DIRECT_OPTIONS } from '../helpers/broker-urls';
import { uniqueName } from '../helpers/names';
import * as rabbitApi from '../helpers/rabbit-api';
import { sleepPromise } from '../../src/utils';

const PROPAGATION_DELAY = 10_000;

describe('Channel integration', () => {
    let connection: ConnectionImplementation;

    beforeEach(() => {
        connection = new ConnectionImplementation(DIRECT_OPTIONS);
    });

    afterEach(async () => {
        await connection?.close();
    });

    describe('connect', () => {
        it('should provide a native amqplib channel after connecting', async () => {
            const channel = connection.createChannel();
            const native = await channel.native();
            expect(native).toBeTruthy();
        });

        it('should coalesce concurrent connect() calls', async () => {
            const channel = connection.createChannel();
            const [r1, r2, r3] = await Promise.all([
                channel.connect(),
                channel.connect(),
                channel.connect(),
            ]);
            expect(r1).toBe(channel);
            expect(r2).toBe(channel);
            expect(r3).toBe(channel);
        });
    });

    describe('close', () => {
        it('should close an open channel without error', async () => {
            const channel = connection.createChannel();
            await channel.connect();
            await expect(channel.close()).resolves.toBeUndefined();
        });

        it('should be a no-op when channel was never connected', async () => {
            const channel = connection.createChannel();
            await expect(channel.close()).resolves.toBeUndefined();
        });

        it('should coalesce concurrent close() calls', async () => {
            const channel = connection.createChannel();
            await channel.connect();
            await expect(Promise.all([channel.close(), channel.close()])).resolves.toBeDefined();
        });

        it('should emit close event when the connection is closed', async () => {
            const channel = connection.createChannel();
            await channel.connect();
            const closeHandler = vi.fn();
            channel.on('close', closeHandler);
            await connection.close();
            await vi.waitFor(() => expect(closeHandler).toHaveBeenCalledTimes(1));
        });
    });

    describe('checkQueue', () => {
        it('should return queue details for an existing queue', async () => {
            const queueName = uniqueName('ch-check');
            const channel = connection.createChannel();
            await channel.createQueue(queueName, { durable: true, autoDelete: false });
            const result = await channel.checkQueue(queueName);
            expect(result.queue).toBe(queueName);
        });

        it('should throw for a non-existent queue', async () => {
            const channel = connection.createChannel();
            await channel.connect();
            channel.on('error', () => {});
            await expect(channel.checkQueue('does-not-exist-xyz')).rejects.toThrow();
        });
    });

    describe('publish — regular channel', () => {
        it('should deliver a message to the broker', async () => {
            const queueName = uniqueName('ch-pub');
            const channel = connection.createChannel();
            await channel.createQueue(queueName, { durable: true, autoDelete: false });

            await channel.publish(
                '',
                queueName,
                Buffer.from(JSON.stringify({ value: 1 })),
                { drainTimeout: 5000 },
            );

            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(1);
        });
    });

    describe('publish — confirm channel', () => {
        it('should deliver a message to the broker with broker confirmation', async () => {
            const queueName = uniqueName('ch-pub-confirm');
            const channel = connection.createChannel(true);
            await channel.createQueue(queueName, { durable: true, autoDelete: false });

            await channel.publish(
                '',
                queueName,
                Buffer.from(JSON.stringify({ value: 2 })),
                { drainTimeout: 5000 },
            );

            await sleepPromise(PROPAGATION_DELAY);
            const detail = await rabbitApi.queueDetail(queueName);
            expect(detail.messages).toBe(1);
        });
    });
});
