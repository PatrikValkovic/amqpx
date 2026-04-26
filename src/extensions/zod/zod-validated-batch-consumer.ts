import { EventEmitter } from 'events';
import { type z } from 'zod';
import { BatchConsumerCallbackFn, BatchConsumer } from '../../index';
import { BatchConsumerEventMap } from '../../consumer/batch-consumer';

/**
 * BatchConsumer wrapper that validates received messages using Zod.
 *
 * Note that the schema may transform the message into different type
 * (using e.g. `refine`), and this type will be reflected in generic types.
 *
 * @example
 * ```ts
 * import { ZodValidatedBatchConsumer } from 'amqpx/zod';
 *
 * const consumer = new ZodValidatedBatchConsumer(
 *     new TestBatchConsumer(),
 *     z.object({
 *         foo: z.string(),
 *     }),
 * );
 *
 * await consumer.listen((args) => {
 *     for (const { message } of args.messages) {
 *         console.log(message.foo);
 *     }
 * });
 *
 * consumer.on('handlingFailed', error => {
 *     if(error instanceof ZodError) {
 *         // do something
 *     }
 * });
 * ```
 */
export class ZodValidatedBatchConsumer<InputMessage, AdditionalProperties, OutputMessage = InputMessage> extends EventEmitter<BatchConsumerEventMap> implements BatchConsumer<OutputMessage, AdditionalProperties> {
    /**
     * Creates a new instance of ZodValidatedBatchConsumer.
     * @param consumer BatchConsumer to wrap, implementation will reuse downstream implementation except the message validation.
     * @param validator Zod schema to validate each message with. May transform the message into different type.
     */
    constructor(
        private readonly consumer: BatchConsumer<InputMessage, AdditionalProperties>,
        private readonly validator: z.ZodType<OutputMessage, InputMessage>,
    ) {
        super();
        this.consumer.on('handlingFailed', error => this.emit('handlingFailed', error));
        this.consumer.on('error', error => this.emit('error', error));
    }

    /**
     * @inheritDoc
     */
    close(timeout?: number): Promise<void> {
        return this.consumer.close(timeout);
    }

    /**
     * Receives messages from downstream implementation, validates each using Zod schema,
     * and calls the callback with the validated messages.
     *
     * @param callback Callback to call when all messages in the batch are validated.
     */
    async listen(callback: BatchConsumerCallbackFn<OutputMessage, AdditionalProperties>) {
        await this.consumer.listen(args => {
            const messages = args.messages.map(item => {
                const parsed = this.validator.safeParse(item.message);
                if (!parsed.success)
                    throw parsed.error;
                return { ...item, message: parsed.data };
            });
            return callback({ ...args, messages });
        });
        return this;
    }

    get queue() {
        return this.consumer.queue;
    }

    get channel() {
        return this.consumer.channel;
    }
}
