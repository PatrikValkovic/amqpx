import { EventEmitter } from 'events';
import { type z } from 'zod';
import { ConsumerCallbackFn, Consumer } from '../../index';
import { ConsumerEventMap } from '../../consumer/consumer';
import { ZodValidationError } from './zod-validation-error';

/**
 * Consumer wrapper that validates received messages using Zod.
 *
 * Note that the schema may transform the message into different type
 * (using e.g. `refine`), and this type will be reflected in generic types.
 *
 * @example
 * ```ts
 * import { ZodValidatedConsumer } from 'amqpx/zod';
 *
 * const consumer = new ZodValidatedConsumer(
 *     new TestConsumer(),
 *     z.object({
 *         foo: z.string(),
 *     }),
 * );
 *
 * await consumer.listen((args) => {
 *     console.log(args.message.foo);
 * });
 *
 * consumer.on('handlingFailed', error => {
 *     if(error instanceof ZodValidationError) {
 *         // do something
 *     }
 * });
 * ```
 */
export class ZodValidatedConsumer<InputMessage, AdditionalProperties, OutputMessage = InputMessage> extends EventEmitter<ConsumerEventMap> implements Consumer<OutputMessage, AdditionalProperties> {
    /**
     * Creates a new instance of ZodValidatedConsumer.
     * @param consumer Consumer to wrap, implementation will reuse downstream implementation except the message validation.
     * @param validator Zod schema to validate the message with. May transform the message into different type.
     */
    constructor(
        private readonly consumer: Consumer<InputMessage, AdditionalProperties>,
        private readonly validator: z.ZodType<OutputMessage, InputMessage>,
    ) {
        super();
        this.consumer.on('handlingFailed', error => this.emit('handlingFailed', error));
    }

    /**
     * @inheritDoc
     */
    close(timeout?: number): Promise<void> {
        return this.consumer.close(timeout);
    }

    /**
     * Receives the message from downstream implementation, validates it using Zod schema,
     * and calls the callback with the validated message.
     *
     * @param callback Callback to call when the message is validated.
     * @throws ZodValidationError if the message is not valid.
     */
    async listen(callback: ConsumerCallbackFn<OutputMessage, AdditionalProperties>) {
        await this.consumer.listen(args => {
            const parsed = this.validator.safeParse(args.message);
            if (!parsed.success)
                throw new ZodValidationError(`Message validation failed with errors`, parsed.error);
            return callback({
                ...args,
                message: parsed.data,
            });
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
