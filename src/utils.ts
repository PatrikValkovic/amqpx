import { MaybePromise } from './types';

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


export function tryCatchExpression<T>(_try: () => Promise<T>, _catch: (err: unknown) => MaybePromise<T>): Promise<T>;
export function tryCatchExpression<T>(_try: () => T, _catch: (err: unknown) => T): T;
export function tryCatchExpression<T>(_try: () => T, _catch: (err: unknown) => T) {
    try {
        const result = _try();
        if (result instanceof Promise || (typeof result === 'object' && result !== null && 'catch' in result && typeof result.catch === 'function'))
            return (result as unknown as Promise<T>).catch(err => _catch(err));
        return result;
    } catch (err) {
        return _catch(err);
    }
}
