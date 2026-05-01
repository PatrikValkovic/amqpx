import { describe, expect, afterEach, vi } from 'vitest';
import * as amqp from 'amqplib';
import { ConnectionImplementation, ConnectionState } from '../../src';
import { DIRECT_OPTIONS, PROXIED_OPTIONS } from '../helpers/broker-urls';
import { RABBIT_CONTAINER, restartContainer, stopContainer } from '../helpers/docker';
import { withToxic } from '../helpers/toxiproxy';
import { sleepPromise } from '../../src/utils';

const connectRef = vi.hoisted(() => ({
    fn: null as ((...args: Parameters<typeof import('amqplib').connect>) => ReturnType<typeof import('amqplib').connect>) | null,
}));

vi.mock('amqplib', async importOriginal => {
    const actual = await importOriginal<typeof import('amqplib')>();
    connectRef.fn = (...args) => actual.connect(...args);
    return {
        ...actual,
        connect: vi.fn((...args: Parameters<typeof actual.connect>) => connectRef.fn!(...args)),
    };
});

describe('Connection integration', () => {
    let conn: ConnectionImplementation;

    afterEach(async () => {
        vi.mocked(amqp.connect).mockImplementation((...args) => connectRef.fn!(...args));
        await Promise.race([
            conn?.close().catch(() => { /* already closed or never opened */ }),
            sleepPromise(100),
        ]);
    });

    describe('connect', () => {
        it('transitions to connected state', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.connect();
            expect(conn.state()).toBe(ConnectionState.connected);
            const native = await conn.native();
            expect(native).toBeDefined();
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });

        it('concurrent connect() calls coalesce to single attempt', async () => {
            vi.mocked(amqp.connect).mockClear();
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await Promise.all([conn.connect(), conn.connect()]);
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(amqp.connect).toHaveBeenCalledTimes(1);
        });

        it('calling native() should establish the connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.native();
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });

        it('calling connect() after close() should establish new connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.close();
            await conn.connect();
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });
    });

    describe('close', () => {
        it('transitions to closed state', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const closePromise = conn.close();
            expect(conn.state()).toBe(ConnectionState.closing);
            await closePromise;
            expect(conn.state()).toBe(ConnectionState.closed);
        });

        it('emits close event', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const closeHandlerMock = vi.fn();
            conn.on('close', closeHandlerMock);
            await conn.close();
            expect(closeHandlerMock).toHaveBeenCalledTimes(1);
        });

        it('close() on preconnect state is a no-op', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.close();
            expect(conn.state()).toBe(ConnectionState.preconnect);
        });

        it('concurrent close() calls coalesce to single teardown', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const native = await conn.native();
            const closeSpy = vi.spyOn(native, 'close');
            await Promise.all([conn.close(), conn.close()]);
            expect(conn.state()).toBe(ConnectionState.closed);
            expect(closeSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('reconnect', () => {
        it('emits reconnecting event when server drops connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectingMock = vi.fn();
            conn.once('reconnecting', reconnectingMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
        });

        it('re-establishes connection and emits connected event after drop', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectedMock = vi.fn();
            conn.once('connected', reconnectedMock);
            await restartContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectedMock).toHaveBeenCalledTimes(1), { timeout: 60_000 });
            expect(conn.state()).toBe(ConnectionState.connected);
        });

        it('close during reconnection stops retry loop', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectingMock = vi.fn();
            conn.once('reconnecting', reconnectingMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
            await conn.close();
            expect(conn.state()).toBe(ConnectionState.closed);
        });

        it('emits connectionRetryExhausted when all retries fail', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS, { maxRetries: 1, reconnectionTimeoutMs: 0 });
            await conn.connect();
            const exhaustedMock = vi.fn();
            conn.once('connectionRetryExhausted', exhaustedMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(exhaustedMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
            expect(conn.state()).toBe(ConnectionState.closed);
        });
    });

    describe('networking', () => {
        it('connects successfully with 50ms upstream and 50ms downstream latency', async () => {
            conn = new ConnectionImplementation(PROXIED_OPTIONS);
            await withToxic('rabbit', { type: 'latency', stream: 'upstream', toxicity: 1, attributes: { latency: 50 } }, () =>
                withToxic('rabbit', { type: 'latency', stream: 'downstream', toxicity: 1, attributes: { latency: 50 } }, async () => {
                    await conn.connect();
                    expect(conn.state()).toBe(ConnectionState.connected);
                }),
            );
        });
    });
});
