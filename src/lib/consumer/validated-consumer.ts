import { z } from 'zod';
import { Queue } from '../queue';
import { Channel } from '../channel';
import { Consumer } from './consumer';
import { ConsumerCallbackFn } from './types';

export class ValidatedConsumer<InputMessage, AdditionalProperties, OutputMessage = InputMessage> implements Consumer<OutputMessage, AdditionalProperties> {
    constructor(
        private readonly consumer: Consumer<InputMessage, AdditionalProperties>,
        private readonly validator: z.ZodType<OutputMessage, z.ZodTypeDef, InputMessage>,
    ) {}

    close(timeout?: number): Promise<void> {
        return this.consumer.close(timeout);
    }

    async listen(callback: ConsumerCallbackFn<OutputMessage, AdditionalProperties>) {
        await this.consumer.listen(args => {
            const parsed = this.validator.safeParse(args.message);
            if (!parsed.success) {
                const errorsInString = Object.entries(parsed.error.flatten().fieldErrors).map(
                    error => `- ${error[0]}: ${error[1]}`,
                ).join('\n');
                throw new Error(`Message validation failed with errors\n${errorsInString}`);
            }
            return callback({
                ...args,
                message: parsed.data,
            });
        });
        return this;
    }

    on(eventName: 'handlingFailed', callback: (error: unknown) => void) {
        this.consumer.on(eventName, callback);
        return this;
    }

    async setPrefetch(prefetch: number): Promise<void> {
        await this.consumer.setPrefetch(prefetch);
    }

    getQueue(): Queue {
        return this.consumer.getQueue();
    }

    getChannel(): Channel {
        return this.consumer.getChannel();
    }
}
