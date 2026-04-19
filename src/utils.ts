export const LIB_NAME = 'amqpx';

export const sleepPromise = (ms: number) =>
    new Promise<void>(resolve => setTimeout(resolve, ms));

const isObject = (item: unknown): item is object => (!!item && typeof item === 'object' && !Array.isArray(item));

export const deepMerge = <T extends object>(target: T, ...sources: Array<Partial<T>>): T => {
    if (sources.length === 0)
        return target;
    const [source, ...restSources] = sources;

    if (isObject(target) && isObject(source)) {
        for (const key in source) {
            if (Object.prototype.hasOwnProperty.call(source, key)) {
                const typedKey = key as keyof T;
                const sourceValue = source[typedKey];
                const targetValue = target[typedKey];
                if (isObject(sourceValue) && isObject(targetValue)) {
                    target[typedKey] = deepMerge(
                        targetValue as T[keyof T] & object,
                        sourceValue as Partial<T[keyof T] & object>,
                    ) as T[keyof T];
                } else {
                    target[typedKey] = sourceValue as T[keyof T];
                }
            }
        }
    }

    return deepMerge(target, ...restSources);
};

export const last = <T>(arr: T[]) => {
    const index = arr.length - 1;
    if (index < 0)
        return undefined;
    return arr[index];
};

export const zip = <T, U>(arr1: T[], arr2: U[]) => {
    const length = Math.min(arr1.length, arr2.length);
    const result: Array<[T, U]> = [];
    for (let i = 0; i < length; i++)
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        result.push([arr1[i]!, arr2[i]!]);
    return result;
};

export function swallowError<T>(callback: () => Promise<T>): Promise<T | null>;
export function swallowError<T>(callback: () => T): T | null;
export function swallowError<T>(promise: Promise<T>): Promise<T | null>;
export function swallowError<T>(callback: ((() => Promise<T>) | (() => T) | Promise<T>)): Promise<T | null> | T | null {
    try {
        const result = typeof callback === 'function' ? callback() : callback;
        if (result instanceof Promise)
            return result.catch(() => null);
        return result;
    } catch {
        return null;
    }
}

export function maskAmqpUrl(options: import('amqplib').Options.Connect | string): string {
    if (typeof options === 'string')
        return options.replace(/:\/\/([^:@]+):([^@]+)@/, '://$1:***@');
    const host = options.hostname ?? 'localhost';
    const port = options.port ?? 5672;
    const vhost = options.vhost ? `/${options.vhost}` : '';
    const user = options.username ?? 'guest';
    return `amqp://${user}:***@${host}:${port}${vhost}`;
}

export const errToMessage = (err: unknown) => {
    if (err instanceof Error)
        return err.message;
    if (err !== null && typeof err === 'object' && 'message' in err)
        return `${err.message}`;
    return String(err);
};
