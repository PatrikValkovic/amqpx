import { execFile as execFileCb } from 'node:child_process';
import { promisify } from 'node:util';
import * as path from 'node:path';
import { sleepPromise } from '../../src/utils';

const execFile = promisify(execFileCb);

const COMPOSE_FILE = path.join(__dirname, '..', '..', 'docker-compose.yml');

export const RABBIT_CONTAINER = 'amqp-integ-rabbit';
export const TOXI_CONTAINER = 'amqp-integ-toxi';

export async function containerStatus(container: string): Promise<string> {
    const { stdout } = await execFile('docker', ['inspect', '--format', '{{.State.Status}}', container]);
    return stdout.trim();
}

export async function startContainer(name: string): Promise<void> {
    await execFile('docker', ['start', name]);
}

export async function stopContainer(name: string): Promise<void> {
    await execFile('docker', ['stop', name]);
}

export async function killContainer(name: string, signal = 'SIGKILL'): Promise<void> {
    await execFile('docker', ['kill', '--signal', signal, name]);
}

export async function restartContainer(name: string): Promise<void> {
    await execFile('docker', ['restart', name]);
}

export async function destroyContainer(name: string): Promise<void> {
    await execFile('docker', ['stop', name]);
    await execFile('docker', ['rm', name]);
}

export async function waitForHealthy(name: string, timeoutMs = 30_000): Promise<void> {
    const start = performance.now();
    while (performance.now() - start < timeoutMs) {
        const { stdout } = await execFile('docker', [
            'inspect', '--format', '{{.State.Health.Status}}', name,
        ]).catch(() => ({ stdout: 'none', stderr: '' }));
        const status = stdout.trim();
        // Container has no healthcheck configured — treat as healthy if running
        if (status === '' || status === 'none') {
            const runStatus = await containerStatus(name).catch(() => '');
            if (runStatus === 'running')
                return;
        }
        if (status === 'healthy')
            return;
        await sleepPromise(500);
    }
    throw new Error(`Container ${name} did not become healthy within ${timeoutMs}ms`);
}

export async function compose(args: string[]): Promise<{ stdout: string; stderr: string }> {
    return execFile('docker', ['compose', '-f', COMPOSE_FILE, ...args]);
}
