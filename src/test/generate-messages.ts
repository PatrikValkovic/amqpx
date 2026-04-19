import { TestChannel } from '../extensions/vitest';

export const processGeneratedMessages = (channel: TestChannel, numOfMessages: number) => {
    const messagesContent = Array.from({ length: numOfMessages }, (_, i) => i).map(
        value => ({ value }),
    );
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
