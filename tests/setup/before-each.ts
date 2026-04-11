import { beforeEach } from 'vitest';
import { containerStatus, startContainer, waitForHealthy, RABBIT_CONTAINER, TOXI_CONTAINER } from '../helpers/docker';
import { resetAll } from '../helpers/toxiproxy';
import { sleepPromise } from '../../src/utils';

async function ensureContainerRunning(container: string): Promise<void> {
    const status = await containerStatus(container).catch(() => 'missing');
    if (status !== 'running') {
        await startContainer(container);
        await waitForHealthy(container, 30_000);
    }
}

async function ensureToxiReachable(): Promise<void> {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
        const ok = await fetch('http://localhost:8474/version')
            .then(r => r.ok)
            .catch(() => false);
        if (ok)
            return;
        await sleepPromise(300);
    }
    throw new Error('Toxiproxy HTTP API did not become reachable within 15s');
}

beforeEach(async () => {
    await ensureContainerRunning(RABBIT_CONTAINER);
    await ensureContainerRunning(TOXI_CONTAINER);
    await ensureToxiReachable();
    await resetAll();
});
