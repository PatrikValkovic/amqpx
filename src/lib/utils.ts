export const externallyResolvedPromise = () => {
    let resolve: () => void;
    const promise = new Promise<void>(r => {
        resolve = r;
    });
    // @ts-expect-error Resolve is assigned immediately
    return [promise, resolve] as const;
};

export const sleepPromise = (ms: number) =>
    new Promise<void>(resolve => setTimeout(resolve, ms));

const isObject = (item: unknown): item is object => (!!item && typeof item === 'object' && !Array.isArray(item));

export const deepMerge = <T extends object>(target: T, ...sources: Array<Partial<T>>): T => {
    if (!sources.length)
        return target;
    const source = sources.shift();

    if (isObject(target) && isObject(source)) {
        for (const key in source) {
            if (Object.prototype.hasOwnProperty.call(source, key)) {
                const sourceValue = source[key];
                if (isObject(sourceValue) && isObject(target[key]))
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    target[key] = deepMerge(target[key] as any, sourceValue as any);
                else
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    (target as any)[key] = sourceValue;

            }
        }
    }

    return deepMerge(target, ...sources);
};
