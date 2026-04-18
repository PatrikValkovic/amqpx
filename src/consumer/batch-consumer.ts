import { EventEmitter } from 'node:events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { BatchConsumerCallbackFn } from './types';
import { BaseConsumerEventMap } from './base-consumer';

/**
 * Events emitted by a {@link BatchConsumer}.
 */
export type BatchConsumerEventMap = BaseConsumerEventMap & {
    /**
     * Emitted when an internal processing error occurs.
     */
    error: [error: Error];
    /**
     * Emitted when the message handler or message parsing throws an error.
     */
    handlingFailed: [error: unknown];
};

export interface BatchConsumer<
    Message,
    AdditionalProperties = Record<string, unknown>,
> extends EventEmitter<BatchConsumerEventMap> {
    close(timeout?: number): Promise<void>;

    listen(callback: BatchConsumerCallbackFn<Message, AdditionalProperties>): Promise<BatchConsumer<Message, AdditionalProperties>>;

    get queue(): Queue;

    get channel(): Channel;
}
