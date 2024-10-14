import { Exchange } from '../exchange';

export type BindingArgs = Record<string, string | number | boolean>;

type ExchangeName = string;
type Pattern = string;
export type Bindings = Record<ExchangeName, Record<Pattern, {
    exchange: Exchange;
    pattern: Pattern;
    args?: BindingArgs;
}>>;
