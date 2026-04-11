const TOXI_BASE = 'http://localhost:8474';

export interface Toxic {
    name: string;
    type: string;
    stream: 'upstream' | 'downstream';
    toxicity: number;
    attributes: Record<string, unknown>;
}

export async function createProxy(opts: { name: string; listen: string; upstream: string }): Promise<void> {
    const res = await fetch(`${TOXI_BASE}/proxies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...opts, enabled: true }),
    });
    // 409 = already exists, treat as success
    if (!res.ok && res.status !== 409)
        throw new Error(`Failed to create proxy: ${res.status} ${await res.text()}`);

}

export async function resetAll(): Promise<void> {
    const res = await fetch(`${TOXI_BASE}/reset`, { method: 'POST' });
    if (!res.ok)
        throw new Error(`Failed to reset toxiproxy: ${res.status}`);
}

export async function addToxic(proxyName: string, toxic: Omit<Toxic, 'name'> & { name?: string }): Promise<Toxic> {
    const res = await fetch(`${TOXI_BASE}/proxies/${proxyName}/toxics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(toxic),
    });
    if (!res.ok)
        throw new Error(`Failed to add toxic: ${res.status} ${await res.text()}`);
    return res.json() as Promise<Toxic>;
}

export async function removeToxic(proxyName: string, toxicName: string): Promise<void> {
    const res = await fetch(`${TOXI_BASE}/proxies/${proxyName}/toxics/${toxicName}`, { method: 'DELETE' });
    if (!res.ok)
        throw new Error(`Failed to remove toxic: ${res.status}`);
}

export async function withToxic<T>(
    proxyName: string,
    toxic: Omit<Toxic, 'name'> & { name?: string },
    fn: () => Promise<T>,
): Promise<T> {
    const created = await addToxic(proxyName, toxic);
    try {
        return await fn();
    } finally {
        await removeToxic(proxyName, created.name);
    }
}
