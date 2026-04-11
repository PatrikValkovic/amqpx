import { describe, test, expect, afterEach, vi } from 'vitest';
import * as amqp from 'amqplib';
import { ConnectionImplementation, ConnectionState } from '../../src';
import { DIRECT_OPTIONS } from '../helpers/broker-urls';
import { RABBIT_CONTAINER, startContainer, stopContainer } from '../helpers/docker';

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
        await conn?.close().catch(() => { /* already closed or never opened */ });
    });

    describe('connect', () => {
        test('transitions to connected state', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.connect();
            expect(conn.state()).toBe(ConnectionState.connected);
            const native = await conn.native();
            expect(native).toBeDefined();
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });

        test('concurrent connect() calls coalesce to single attempt', async () => {
            vi.mocked(amqp.connect).mockClear();
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await Promise.all([conn.connect(), conn.connect()]);
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(amqp.connect).toHaveBeenCalledTimes(1);
        });

        test('calling native() should establish the connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            const connectedHandler = vi.fn();
            conn.on('connected', connectedHandler);
            await conn.native();
            expect(conn.state()).toBe(ConnectionState.connected);
            expect(connectedHandler).toHaveBeenCalledTimes(1);
        });

        test('calling connect() after close() should establish new connection', async () => {
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
        test('transitions to closed state', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const closePromise = conn.close();
            expect(conn.state()).toBe(ConnectionState.closing);
            await closePromise;
            expect(conn.state()).toBe(ConnectionState.closed);
        });

        test('emits close event', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const closeHandlerMock = vi.fn();
            conn.on('close', closeHandlerMock);
            await conn.close();
            expect(closeHandlerMock).toHaveBeenCalledTimes(1);
        });

        test('close() on preconnect state is a no-op', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.close();
            expect(conn.state()).toBe(ConnectionState.preconnect);
        });

        test('concurrent close() calls coalesce to single teardown', async () => {
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
        test('emits reconnecting event when server drops connection', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectingMock = vi.fn();
            conn.once('reconnecting', reconnectingMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
        });

        test('re-establishes connection and emits connected event after drop', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectedMock = vi.fn();
            conn.once('connected', reconnectedMock);
            await stopContainer(RABBIT_CONTAINER);
            await startContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectedMock).toHaveBeenCalledTimes(1), { timeout: 60_000 });
            expect(conn.state()).toBe(ConnectionState.connected);
        }, 120_000);

        test('close during reconnection stops retry loop', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS);
            await conn.connect();
            const reconnectingMock = vi.fn();
            conn.once('reconnecting', reconnectingMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(reconnectingMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
            await conn.close();
            expect(conn.state()).toBe(ConnectionState.closed);
        });

        test('emits connectionRetryExhausted when all retries fail', async () => {
            conn = new ConnectionImplementation(DIRECT_OPTIONS, { maxRetries: 1, reconnectionTimeoutMs: 0 });
            await conn.connect();
            const exhaustedMock = vi.fn();
            conn.once('connectionRetryExhausted', exhaustedMock);
            await stopContainer(RABBIT_CONTAINER);
            await vi.waitFor(() => expect(exhaustedMock).toHaveBeenCalledTimes(1), { timeout: 20_000 });
            expect(conn.state()).toBe(ConnectionState.closed);
        });
    });
});
