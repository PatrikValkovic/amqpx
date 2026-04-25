import { TestChannel } from '../extensions/vitest';

export const processGeneratedMessages = (channel: TestChannel, messages: number | object[]) => {
    const messagesContent = Array.isArray(messages)
        ? messages
        : Array.from({ length: messages }, (_, i) => ({ value: i }));
    const rabbitMessages = messagesContent.map((content, i) => ({
        content: Buffer.from(JSON.stringify(content)),
        __index: i,
    }));

    const consumerHandler = channel.nativeChannel.consume.mock.lastCall?.[1];
    if (!consumerHandler)
        throw new Error('No consumer handler found');

    const consumePromise = Promise.all(rabbitMessages.map(message => consumerHandler(message)));

    return {
        messagesContent,
        rabbitMessages,
        consumePromise,
    };
};
