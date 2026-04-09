import { Queue } from '../queue';
import { Channel } from '../channel';
import { BatchConsumerCallbackFn } from './types';

export interface BatchConsumer<Message, AdditionalProperties = Record<string, unknown>> {
    close(timeout?: number): Promise<void>;

    listen(callback: BatchConsumerCallbackFn<Message, AdditionalProperties>): Promise<BatchConsumer<Message, AdditionalProperties>>;

    on(eventName: 'handlingFailed', callback: (error: unknown) => void): BatchConsumer<Message, AdditionalProperties>;

    get queue(): Queue;

    get channel(): Channel;
}
