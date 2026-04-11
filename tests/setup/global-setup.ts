import { compose, waitForHealthy, RABBIT_CONTAINER } from '../helpers/docker';
import { createProxy } from '../helpers/toxiproxy';

export default async function setup() {
    await compose(['up', '-d', '--wait']);
    await waitForHealthy(RABBIT_CONTAINER, 60_000);

    // Provision default toxiproxy route: host:25672 → rabbitmq:5672 (via docker network)
    // Idempotent — createProxy ignores 409 if proxy already exists from a prior run.
    await createProxy({ name: 'rabbit', listen: '0.0.0.0:25672', upstream: 'rabbitmq:5672' });

    return async function teardown() {
        await compose(['down', '-v']);
    };
}
