import * as amqp from 'amqplib';
import { ProducerPublishOptions } from '../producer/types';

export type ChannelPublishOptions = ProducerPublishOptions & {
    drainTimeout: number;
};

export type ChannelWrapper = ({
    isConfirmed: true;
    channel: Promise<amqp.ConfirmChannel> | null;
} | {
    isConfirmed: false;
    channel: Promise<amqp.Channel> | null;
});
