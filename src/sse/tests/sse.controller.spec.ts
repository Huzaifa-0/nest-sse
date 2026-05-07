import { BadRequestException, ForbiddenException } from '@nestjs/common';
import { SseController } from '../controllers/sse.controller';

describe('SseController', () => {
  it('passes x-user-id to subscription metadata', () => {
    const subscribe = jest.fn(() => ({ ok: true }));
    const controller = new SseController({
      subscribe,
    } as any);

    controller.subscribe(
      {
        clientId: 'c1',
        channel: 'orders.1',
      },
      'user-1',
    );

    expect(subscribe).toHaveBeenCalledWith({
      clientId: 'c1',
      channel: 'orders.1',
      metadata: { userId: 'user-1' },
    });
  });

  it('returns 403 when channel subscription is unauthorized', () => {
    const controller = new SseController({
      subscribe: () => ({ ok: false, reason: 'unauthorized' }),
    } as any);

    expect(() => {
      controller.subscribe({
        clientId: 'c1',
        channel: 'orders.1',
      });
    }).toThrow(ForbiddenException);
  });

  it('returns 400 when subscription fails for non-auth reasons', () => {
    const controller = new SseController({
      subscribe: () => ({ ok: false, reason: 'connection-not-found' }),
    } as any);

    expect(() => {
      controller.subscribe({
        clientId: 'c1',
        channel: 'orders.1',
      });
    }).toThrow(BadRequestException);
  });
});
