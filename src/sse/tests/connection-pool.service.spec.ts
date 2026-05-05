import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { ConnectionPoolService, type ConnectionRecord } from '../services/connection-pool.service';

describe('ConnectionPoolService', () => {
  let service: ConnectionPoolService;

  beforeEach(() => {
    service = new ConnectionPoolService();
  });

  function record(clientId: string): ConnectionRecord {
    return {
      clientId,
      response: {
        write: jest.fn(),
        end: jest.fn(),
      } as any,
      metadata: {},
      topics: new Set<string>(),
      createdAt: Date.now(),
      lastSeenAt: Date.now(),
      bufferedEvents: [],
      isClosed: false,
    };
  }

  it('creates and counts connections', () => {
    service.create(record('c1'));
    service.create(record('c2'));

    expect(service.count()).toBe(2);
    expect(service.has('c1')).toBe(true);
  });

  it('subscribes and unsubscribes from topics', () => {
    service.create(record('c1'));

    expect(service.subscribe('c1', 'news')).toBe(true);
    expect(service.topicCount('news')).toBe(1);

    expect(service.unsubscribe('c1', 'news')).toBe(true);
    expect(service.topicCount('news')).toBe(0);
  });

  it('updates metadata and snapshots', () => {
    service.create(record('c1'));

    service.setMetadata('c1', { role: 'user' });
    const snapshot = service.snapshot('c1');

    expect(snapshot?.metadata).toEqual({ role: 'user' });
  });
});
