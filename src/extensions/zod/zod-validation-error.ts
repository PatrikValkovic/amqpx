import { ZodError } from 'zod';

/**
 * Error indicating that the message validation failed.
 *
 * Contains the original ZodError.
 */
export class ZodValidationError extends Error {
    /**
     * @param message Message describing the error.
     * @param zodError Original ZodError that caused the validation to fail.
     */
    constructor(
        message: string,
        public readonly zodError: ZodError,
    ) {
        super(message);
    }
}
