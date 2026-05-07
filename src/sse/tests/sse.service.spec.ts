import { ChannelRegistryService } from '../services/channel-registry.service';
import { ConnectionPoolService } from '../services/connection-pool.service';
import { SseEventBusService } from '../services/sse-event-bus.service';
import { SseService } from '../services/sse.service';
import type { SseTransportAdapter } from '../interfaces/transport.interface';

describe('SseService', () => {
  let service: SseService;
  let transport: SseTransportAdapter;

  beforeEach(async () => {
    jest.useFakeTimers();
    transport = {
      publish: jest.fn(async () => undefined),
      subscribe: jest.fn(async () => undefined),
      close: jest.fn(async () => undefined),
    };

    service = new SseService(
      new ConnectionPoolService(),
      new SseEventBusService(),
      {
        heartbeatIntervalMs: 100,
        deadConnectionMs: 300,
        bufferMaxEvents: 2,
        bufferFlushMs: 20,
        channels: [
          { pattern: 'news.public', audience: 'public' },
          { pattern: 'news.private', audience: 'authenticated' },
          {
            pattern: 'orders.{orderId}',
            audience: 'authenticated',
            authorize: ({ params, requestMetadata }) => {
              const allowedOrderId = (requestMetadata as { orderId: string }).orderId;
              return allowedOrderId === (params as { orderId: string }).orderId;
            },
          },
        ],
      },
      transport,
      new ChannelRegistryService([
        { pattern: 'news.public', audience: 'public' },
        { pattern: 'news.private', audience: 'authenticated' },
        {
          pattern: 'orders.{orderId}',
          audience: 'authenticated',
          authorize: ({ params, requestMetadata }) => {
            const allowedOrderId = (requestMetadata as { orderId: string }).orderId;
            return allowedOrderId === (params as { orderId: string }).orderId;
          },
        },
      ]),
    );

    await service.onModuleInit();
  });

  afterEach(async () => {
    await service.onModuleDestroy();
    jest.useRealTimers();
  });

  function mockConnection(clientId: string, metadata?: Record<string, unknown>) {
    const reqHandlers = new Map<string, (...args: any[]) => void>();
    const req = {
      on: (event: string, cb: (...args: any[]) => void) => {
        reqHandlers.set(event, cb);
      },
    } as any;

    const res = {
      setHeader: jest.fn(),
      flushHeaders: jest.fn(),
      write: jest.fn(),
      end: jest.fn(),
    } as any;

    service.openStream({
      clientId,
      request: req,
      response: res,
      metadata,
    });

    return {
      req,
      res,
      abort: () => {
        reqHandlers.get('close')?.();
      },
    };
  }

  it('subscribes and broadcasts events', async () => {
    const { res } = mockConnection('c1');

    expect(service.subscribe({ clientId: 'c1', channel: 'news.public' })).toEqual({ ok: true });

    const delivered = await service.broadcast('news.public', {
      event: 'news.updated',
      data: { id: 1 },
    });

    jest.advanceTimersByTime(25);

    expect(delivered).toBe(1);
    expect(res.write).toHaveBeenCalled();
  });

  it('cancels stream and closes connection', () => {
    const { res } = mockConnection('c2');

    const ok = service.cancelStream('c2', 'done');

    expect(ok).toBe(true);
    expect(res.end).toHaveBeenCalled();
    expect(service.connectionCount()).toBe(0);
  });

  it('handles dead connections by heartbeat timeout', () => {
    const { res } = mockConnection('c3');

    jest.advanceTimersByTime(500);

    expect(res.end).toHaveBeenCalled();
  });

  it('pipes iterable data to client stream', async () => {
    const { res } = mockConnection('c4');

    const sent = await service.pipeIterable({
      clientId: 'c4',
      iterable: [1, 2, 3],
      event: 'numbers',
    });

    jest.advanceTimersByTime(25);

    expect(sent).toBe(3);
    expect(res.write).toHaveBeenCalled();
  });

  it('rejects subscribe for unauthenticated client on authenticated channel', () => {
    mockConnection('c5');

    const result = service.subscribe({
      clientId: 'c5',
      channel: 'news.private',
    });

    expect(result).toEqual({ ok: false, reason: 'unauthorized' });
  });

  it('allows subscribe for authenticated channel using connection metadata', () => {
    mockConnection('c6', { userId: 'u1' });

    const result = service.subscribe({
      clientId: 'c6',
      channel: 'news.private',
      metadata: { userId: 'u1' },
    });

    expect(result).toEqual({ ok: true });
  });

  it('allows subscribe for authenticated channel via subscription metadata', () => {
    mockConnection('c10');

    const result = service.subscribe({
      clientId: 'c10',
      channel: 'news.private',
      metadata: { userId: 'u1' },
    });

    expect(result).toEqual({ ok: true });
  });

  it('broadcasts public channel events to subscribed clients regardless of metadata', async () => {
    const { res } = mockConnection('c11', { userId: 'u1' });
    expect(service.subscribe({ clientId: 'c11', channel: 'news.public' })).toEqual({ ok: true });

    jest.advanceTimersByTime(25);
    (res.write as any).mockClear();

    const delivered = await service.broadcast(
      'news.public',
      { event: 'news.updated', data: { id: 1 } },
    );

    jest.advanceTimersByTime(25);

    expect(delivered).toBe(1);
    expect(res.write).toHaveBeenCalled();
  });

  it('broadcasts authenticated channel events after authorized subscription', async () => {
    const { res } = mockConnection('c12');
    expect(
      service.subscribe({
        clientId: 'c12',
        channel: 'news.private',
        metadata: { userId: 'u1' },
      }),
    ).toEqual({ ok: true });

    jest.advanceTimersByTime(25);
    (res.write as any).mockClear();

    const delivered = await service.broadcast(
      'news.private',
      { event: 'news.updated', data: { id: 2 } },
    );

    jest.advanceTimersByTime(25);

    expect(delivered).toBe(1);
    expect(res.write).toHaveBeenCalled();
  });

  it('authorizes pattern channels with params and metadata', () => {
    mockConnection('c7', { orderId: '123', userId: 'u1' });

    const denied = service.subscribe({
      clientId: 'c7',
      channel: 'orders.999',
      metadata: { userId: 'u1', orderId: '123' },
    });
    const allowed = service.subscribe({
      clientId: 'c7',
      channel: 'orders.123',
      metadata: { userId: 'u1', orderId: '123' },
    });

    expect(denied).toEqual({ ok: false, reason: 'unauthorized' });
    expect(allowed).toEqual({ ok: true });
  });

  it('flushes a single buffered event when the flush timer elapses', async () => {
    const { res } = mockConnection('c8');

    // Flush initial connection.ready message so this assertion only covers stream data.
    jest.advanceTimersByTime(25);
    (res.write as any).mockClear();

    (service as any).enqueueWrite('c8', {
      event: 'numbers.item',
      data: 1,
      timestamp: Date.now(),
    });

    expect(res.write).not.toHaveBeenCalled();

    jest.advanceTimersByTime(25);

    expect(res.write).toHaveBeenCalledTimes(1);
    expect((res.write as any).mock.calls[0][0]).toContain('event: numbers.item');
  });

  it('flushes buffered events immediately as a batch when size threshold is reached', () => {
    const { res } = mockConnection('c9');

    // Flush initial connection.ready message so this assertion only covers stream data.
    jest.advanceTimersByTime(25);
    (res.write as any).mockClear();

    (service as any).enqueueWrite('c9', {
      event: 'numbers.item',
      data: 1,
      timestamp: Date.now(),
    });

    expect(res.write).not.toHaveBeenCalled();

    (service as any).enqueueWrite('c9', {
      event: 'numbers.item',
      data: 2,
      timestamp: Date.now(),
    });

    expect(res.write).toHaveBeenCalledTimes(1);
    expect((res.write as any).mock.calls[0][0]).toContain('event: events.batch');
  });
});
