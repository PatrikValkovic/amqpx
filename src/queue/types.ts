import * as amqp from 'amqplib';
import { AssertionMode } from '../types';
import { Exchange } from '../exchange';

export type BindingArgs = Record<string, string | number | boolean>;

export type QueueOptions = amqp.Options.AssertQueue & {
    /**
     * Controls how the queue is verified against the broker when `assert()` is called.
     * Defaults to `AssertionMode.Assert`.
     *
     * @see AssertionMode
     */
    assertionMode?: AssertionMode;
};

type ExchangeName = string;
type Pattern = string;
export type Bindings = Record<ExchangeName, Record<Pattern, {
    exchange: Exchange;
    pattern: Pattern;
    args?: BindingArgs;
}>>;
