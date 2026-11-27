import * as amqp from 'amqplib';
import { AssertionMode } from '../types';
import { Exchange } from '../exchange';

export type BindingArgs = Record<string, string | number | boolean>;

/**
 * Options for asserting a queue on the broker.
 * Extends the amqplib `AssertQueue` options with an additional `assertionMode` field.
 */
export type QueueOptions = amqp.Options.AssertQueue & {
    /**
     * Controls how the queue is verified against the broker when `assert()` is called.
     * Defaults to `AssertionMode.Assert`.
     *
     * @see AssertionMode
     */
    assertionMode?: AssertionMode;
};

type Pattern = string;
/** A binding of a queue to an exchange. */
export type QueueBinding = {
    exchange: Exchange;
    pattern: Pattern;
    args?: BindingArgs;
};
