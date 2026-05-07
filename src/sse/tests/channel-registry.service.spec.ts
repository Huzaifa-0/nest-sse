import { describe, expect, it } from '@jest/globals';
import { ChannelRegistryService } from '../services/channel-registry.service';
import type { ChannelAuthorizationContext } from '../types/sse.types';

describe('ChannelRegistryService', () => {
  function context(
    input: Partial<ChannelAuthorizationContext> = {},
  ): ChannelAuthorizationContext {
    return {
      clientId: input.clientId ?? 'c1',
      channel: input.channel ?? 'demo.public',
      params: input.params ?? {},
      requestMetadata: input.requestMetadata ?? {},
    };
  }

  it('resolves channel params from a dynamic authenticated pattern', () => {
    const registry = new ChannelRegistryService([
      {
        pattern: 'orders.{orderId}',
        audience: 'authenticated',
      },
    ]);

    const resolved = registry.resolve('orders.123');

    expect(resolved).not.toBeNull();
    expect(resolved?.definition.pattern).toBe('orders.{orderId}');
    expect(resolved?.definition.audience).toBe('authenticated');
    expect(resolved?.params).toEqual({ orderId: '123' });
  });

  it('returns null for unmatched channels', () => {
    const registry = new ChannelRegistryService([
      {
        pattern: 'demo.public',
        audience: 'public',
      },
    ]);

    expect(registry.resolve('demo.private')).toBeNull();
  });

  it('prefers the most specific match when multiple patterns match', () => {
    const registry = new ChannelRegistryService();
    registry.register({ pattern: 'demo.{id}', audience: 'public' });
    registry.register({ pattern: 'demo.static', audience: 'public' });

    const resolved = registry.resolve('demo.static');

    expect(resolved).not.toBeNull();
    expect(resolved?.definition.pattern).toBe('demo.static');
  });

  it('denies unauthenticated connections on authenticated channels', () => {
    const registry = new ChannelRegistryService([
      {
        pattern: 'private.channel',
        audience: 'authenticated',
      },
    ]);

    const resolved = registry.resolve('private.channel');
    expect(resolved).not.toBeNull();

    const result = registry.authorize(
      resolved!,
      context({
        channel: 'private.channel',
      }),
    );

    expect(result).toEqual({
      allowed: false,
      reason: 'authenticated-channel-required',
    });
  });

  it('supports boolean authorize callback responses', () => {
    const registry = new ChannelRegistryService([
      {
        pattern: 'users.{userId}.alerts',
        audience: 'authenticated',
        authorize: ({ params, requestMetadata }) => {
          const currentUser = String(requestMetadata.userId ?? '');
          return currentUser === params.userId;
        },
      },
    ]);

    const resolved = registry.resolve('users.42.alerts');
    expect(resolved).not.toBeNull();

    const denied = registry.authorize(
      resolved!,
      context({
        channel: 'users.42.alerts',
        params: resolved!.params,
        requestMetadata: { userId: '77' },
      }),
    );

    const allowed = registry.authorize(
      resolved!,
      context({
        channel: 'users.42.alerts',
        params: resolved!.params,
        requestMetadata: { userId: '42' },
      }),
    );

    expect(denied).toEqual({ allowed: false });
    expect(allowed).toEqual({ allowed: true });
  });

  it('supports object authorize callback responses with metadata', () => {
    const registry = new ChannelRegistryService([
      {
        pattern: 'orders.{orderId}',
        audience: 'authenticated',
        authorize: ({ params, requestMetadata }) => {
          const currentOrderId = String(requestMetadata.orderId ?? '');
          return {
            allowed: currentOrderId === params.orderId,
            reason: 'order-mismatch',
            metadata: { checkedOrderId: params.orderId },
          };
        },
      },
    ]);

    const resolved = registry.resolve('orders.100');
    expect(resolved).not.toBeNull();

    const denied = registry.authorize(
      resolved!,
      context({
        channel: 'orders.100',
        params: resolved!.params,
        requestMetadata: { userId: 'u1', orderId: '555' },
      }),
    );

    const allowed = registry.authorize(
      resolved!,
      context({
        channel: 'orders.100',
        params: resolved!.params,
        requestMetadata: { userId: 'u1', orderId: '100' },
      }),
    );

    expect(denied).toEqual({
      allowed: false,
      reason: 'order-mismatch',
      metadata: { checkedOrderId: '100' },
    });
    expect(allowed).toEqual({
      allowed: true,
      reason: 'order-mismatch',
      metadata: { checkedOrderId: '100' },
    });
  });
});