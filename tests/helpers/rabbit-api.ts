import { ADMIN_URL } from './broker-urls';
import { HTTPBindingDetail, HTTPExchangeDetail, HTTPQueueDetail, HTTPQueueGet } from './types';

/*
 Source: https://www.rabbitmq.com/docs/http-api-reference
*/

const SHARED_OPTS = {
    method: 'GET',
    headers: {
        Authorization: `Basic ${Buffer.from('guest:guest').toString('base64')}`,
    },
};

export const queueDetail = async (queueName: string, vhost = '/'): Promise<HTTPQueueDetail> => {
    const response = await fetch(`${ADMIN_URL}/api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queueName)}?lengths_age=60&msg_rates_age=60&data_rates_age=60`, {
        ...SHARED_OPTS,
    });
    return response.json();
};

export const queueDelete = async (queueName: string, vhost = '/'): Promise<void> => {
    const response = await fetch(`${ADMIN_URL}/api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queueName)}`, {
        ...SHARED_OPTS,
        method: 'DELETE',
    });
    await response.text();
};

export const queueGet = async (queueName: string, vhost = '/'): Promise<[HTTPQueueGet]> => {
    const response = await fetch(`${ADMIN_URL}/api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queueName)}/get`, {
        ...SHARED_OPTS,
        method: 'POST',
        body: Buffer.from(JSON.stringify({
            count: 1,
            ackmode: 'reject_requeue_true',
            encoding: 'auto',
        })),
    });
    return response.json();
};

export const exchangeDetail = async (exchangeName: string, vhost = '/'): Promise<HTTPExchangeDetail> => {
    const response = await fetch(`${ADMIN_URL}/api/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchangeName)}?lengths_age=60&msg_rates_age=60&data_rates_age=60`, {
        ...SHARED_OPTS,
    });
    return response.json();
};

export const exchangeBindings = async (exchangeName: string, vhost = '/'): Promise<Array<HTTPBindingDetail>> => {
    const response = await fetch(`${ADMIN_URL}/api/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchangeName)}/bindings/source`, {
        ...SHARED_OPTS,
    });
    return response.json();
};

export const publish = async (message: string, routing_key: string, exchange = '', vhost = '/'): Promise<{  routed: boolean }> => {
    const response = await fetch(`${ADMIN_URL}/api/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/publish`, {
        ...SHARED_OPTS,
        method: 'POST',
        body: JSON.stringify({
            properties: {
                delivery_mode: 2,
            },
            routing_key: routing_key,
            payload: message,
            payload_encoding: 'string',
        }),
    });
    return response.json();
};
