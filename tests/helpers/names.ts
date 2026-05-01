import { randomUUID } from 'node:crypto';

export function uniqueName(prefix: string): string {
    return `${prefix}-${randomUUID().slice(0, 8)}`;
}
