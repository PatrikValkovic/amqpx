import * as amqp from 'amqplib';
import { ProducerPublishOptions } from '../producer/types';

export type ChannelPublishOptions = ProducerPublishOptions & {
    drainTimeout: number;
    isRecursion?: boolean;
};

export type ChannelWrapper = { awaiting?: Promise<void> } & ({
    isConfirmed: true;
    channel: amqp.ConfirmChannel | null;
} | {
    isConfirmed: false;
    channel: amqp.Channel | null;
});
