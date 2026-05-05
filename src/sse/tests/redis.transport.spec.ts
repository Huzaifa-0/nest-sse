import { describe, expect, it, jest } from '@jest/globals';
import { RedisSseTransport } from '../transports/redis.transport';

interface MockRedisClient {
  publish: (channel: string, message: string) => Promise<number>;
  subscribe: (channel: string) => Promise<number>;
  on: (event: 'message', listener: (channel: string, message: string) => void) => void;
  off: (event: 'message', listener: (channel: string, message: string) => void) => void;
  quit: () => Promise<'OK'>;
  emitMessage: (channel: string, message: string) => void;
}

function createMockRedisClient(): MockRedisClient {
  let messageListener: ((channel: string, message: string) => void) | undefined;

  const publish = jest.fn(async (_channel: string, _message: string) => 1);
  const subscribe = jest.fn(async (_channel: string) => 1);
  const on = jest.fn(
    (_event: 'message', listener: (channel: string, message: string) => void) => {
      messageListener = listener;
    },
  );
  const off = jest.fn(
    (_event: 'message', listener: (channel: string, message: string) => void) => {
      if (messageListener === listener) {
        messageListener = undefined;
      }
    },
  );
  const quit = jest.fn(async () => 'OK' as const);

  return {
    publish,
    subscribe,
    on,
    off,
    quit,
    emitMessage: (channel: string, message: string) => {
      messageListener?.(channel, message);
    },
  };
}

describe('RedisSseTransport', () => {
  it('publishes serialized transport messages to the default channel', async () => {
    const client = createMockRedisClient();
    const transport = new RedisSseTransport({ host: '127.0.0.1', port: 6379 });

    (transport as any).createClient = jest
      .fn()
      .mockImplementation(async () => client as any);

    const message = {
      topic: 'news.public',
      target: 'all' as const,
      envelope: {
        event: 'news.updated',
        data: { id: 1 },
      },
    };

    await transport.publish(message);
    await transport.publish(message);

    expect((transport as any).createClient).toHaveBeenCalledTimes(1);
    expect(client.publish).toHaveBeenCalledTimes(2);
    expect(client.publish).toHaveBeenCalledWith(
      'nest-sse:broadcast',
      JSON.stringify(message),
    );
  });

  it('uses a custom configured redis channel for publish', async () => {
    const client = createMockRedisClient();
    const transport = new RedisSseTransport({
      host: '127.0.0.1',
      port: 6379,
      channel: 'custom:sse:channel',
    });

    (transport as any).createClient = jest
      .fn()
      .mockImplementation(async () => client as any);

    await transport.publish({
      topic: 'topic.custom',
      target: 'authenticated',
      envelope: {
        event: 'custom.event',
        data: { ok: true },
      },
    });

    expect(client.publish).toHaveBeenCalledWith(
      'custom:sse:channel',
      expect.any(String),
    );
  });

  it('subscribes and forwards valid JSON messages while ignoring malformed payloads', async () => {
    const client = createMockRedisClient();
    const transport = new RedisSseTransport({ host: '127.0.0.1', port: 6379 });

    (transport as any).createClient = jest
      .fn()
      .mockImplementation(async () => client as any);

    const handler = jest.fn();
    await transport.subscribe(handler);

    expect(client.subscribe).toHaveBeenCalledWith('nest-sse:broadcast');
    expect(client.on).toHaveBeenCalledWith('message', expect.any(Function));

    const validMessage = {
      topic: 'topic.1',
      target: 'public' as const,
      envelope: {
        event: 'event.1',
        data: { id: 10 },
      },
    };

    client.emitMessage('nest-sse:broadcast', JSON.stringify(validMessage));
    client.emitMessage('nest-sse:broadcast', '{bad-json');

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(validMessage);
  });

  it('cleans up listeners and closes both pub/sub clients', async () => {
    const pubClient = createMockRedisClient();
    const subClient = createMockRedisClient();
    const transport = new RedisSseTransport({ host: '127.0.0.1', port: 6379 });

    (transport as any).createClient = jest
      .fn()
      .mockImplementationOnce(async () => pubClient as any)
      .mockImplementationOnce(async () => subClient as any);

    await transport.publish({
      topic: 'cleanup.topic',
      target: 'all',
      envelope: {
        event: 'cleanup.event',
        data: {},
      },
    });

    await transport.subscribe(() => undefined);
    await transport.close();

    expect(subClient.off).toHaveBeenCalledWith('message', expect.any(Function));
    expect(pubClient.quit).toHaveBeenCalledTimes(1);
    expect(subClient.quit).toHaveBeenCalledTimes(1);
  });
});