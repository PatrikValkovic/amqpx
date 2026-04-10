import { EventEmitter } from 'node:events';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { BatchConsumerCallbackFn } from './types';

/**
 * Events emitted by a {@link BatchConsumer}.
 */
export interface BatchConsumerEventMap {
    /**
     * Emitted when an internal processing error occurs.
     */
    error: [message: string];
    /**
     * Emitted when the message handler throws an error.
     * TODO: not yet emitted by the implementation
     */
    handlingFailed: [error: unknown];
}

export interface BatchConsumer<Message, AdditionalProperties = Record<string, unknown>> extends EventEmitter<BatchConsumerEventMap> {
    close(timeout?: number): Promise<void>;

    listen(callback: BatchConsumerCallbackFn<Message, AdditionalProperties>): Promise<BatchConsumer<Message, AdditionalProperties>>;

    get queue(): Queue;

    get channel(): Channel;
}
