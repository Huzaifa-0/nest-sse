import { BadRequestException, Inject, Injectable } from '@nestjs/common';
import { Readable } from 'node:stream';
import {
  SseEventBusService,
  SseService,
  SSE_CHANNEL_REGISTRY,
  type ChannelAuthorizeFn,
  type ChannelRegistry,
} from '../sse';
import type { Request, Response } from 'express';

interface ConnectOptions {
  clientId: string;
  request: Request;
  response: Response;
}

interface ConnectAuthOptions extends ConnectOptions {
  userId: string;
}

interface ServiceStatus {
  service: string;
  connections: number;
}

interface SubscribeRequest {
  clientId: string;
  channel: string;
  metadata?: Record<string, unknown>;
}

interface SubscribeResponse {
  ok: boolean;
  reason?: string;
}

interface RegisterChannelRequest {
  pattern: string;
}

interface BatchEmitRequest {
  channel: string;
  event?: string;
  count?: number;
  payload?: Record<string, unknown>;
}

interface BatchEmitResult {
  totalEvents: number;
  delivered: number;
}

interface IterableEmitRequest {
  clientId: string;
  items: unknown[];
  event?: string;
  intervalMs?: number;
  cancelAfterMs?: number;
}

interface ReadableEmitRequest {
  clientId: string;
  chunks: string[];
  event?: string;
  cancelAfterMs?: number;
}

interface StreamEmitResult {
  sent: number;
  cancelled: boolean;
}

interface CancellableStreamRequest {
  clientId: string;
  channel?: string;
  total?: number;
  intervalMs?: number;
  failAt?: number;
  cancelAfterMs?: number;
  cancelOnError?: boolean;
}

interface CancellableStreamResult {
  sent: number;
  abortedBySignal: boolean;
  cancelledByEvent: boolean;
}

interface StreamScenarioOptions {
  failAt?: number;
  onError: (index: number) => Promise<void>;
}

@Injectable()
export class AppService {
  constructor(
    private readonly sse: SseService,
    private readonly eventBus: SseEventBusService,
    @Inject(SSE_CHANNEL_REGISTRY)
    private readonly channelRegistry: ChannelRegistry,
  ) {}

  /**
   * @description Returns basic runtime status for the demo application.
   * @returns Current service label and active SSE connection count.
   */
  getStatus(): ServiceStatus {
    return {
      service: 'nest-sse package is running',
      connections: this.sse.connectionCount(),
    };
  }

  /**
   * @description Opens a public SSE connection for a client.
   * @param options Connection request and response context.
   * @returns Nothing.
   */
  connectPublic({ clientId, request, response }: ConnectOptions): void {
    this.sse.openStream({
      clientId,
      request,
      response,
      metadata: { audience: 'public' },
    });
  }

  /**
   * @description Opens an authenticated SSE connection for a client.
   * @param options Connection request, response, and user identity context.
   * @returns Nothing.
   */
  connectAuthenticated({
    clientId,
    userId,
    request,
    response,
  }: ConnectAuthOptions): void {
    this.sse.openStream({
      clientId,
      request,
      response,
      metadata: { userId, audience: 'authenticated' },
    });
  }

  /**
   * @description Subscribes a connected client to a channel.
   * @param input Client and channel details for the subscription request.
   * @returns A structured success flag and optional failure reason.
   */
  subscribe(input: SubscribeRequest): SubscribeResponse {
    const result = this.sse.subscribe(input);
    return {
      ok: result.ok,
      reason: result.reason,
    };
  }

  /**
   * @description Removes a client subscription from a channel.
   * @param input Client and channel details for the unsubscribe request.
   * @returns True when a subscription existed and was removed.
   */
  unsubscribe(input: SubscribeRequest): boolean {
    return this.sse.unsubscribe(input);
  }

  /**
   * @description Registers a publicly accessible channel pattern.
   * @param request Channel pattern registration request.
   * @returns Nothing.
   */
  registerPublicChannel({ pattern }: RegisterChannelRequest): void {
    this.channelRegistry.registerPublic(pattern);
  }

  /**
   * @description Registers an authenticated channel pattern with authorization checks.
   * @param request Channel pattern registration request.
   * @returns Nothing.
   */
  registerAuthenticatedChannel({ pattern }: RegisterChannelRequest): void {
    const authorize: ChannelAuthorizeFn = ({ params, requestMetadata }) => {
      if (!requestMetadata.userId) {
        return { allowed: false, reason: 'authenticated-channel-required' };
      }

      const requiredUserId = String(
        requestMetadata.userId ?? params.userId ?? '',
      );
      const currentUserId = String(requestMetadata.userId ?? '');

      if (requiredUserId && currentUserId !== requiredUserId) {
        return { allowed: false, reason: 'user-mismatch' };
      }

      return {
        allowed: true,
        metadata: {
          ...requestMetadata,
          channelAuthorizedAt: Date.now(),
        },
      };
    };

    this.channelRegistry.registerAuthenticated(pattern, authorize);
  }

  /**
   * @description Emits a batch of synthetic events to a channel.
   * @param request Channel and payload options for batched emission.
   * @returns Number of attempted events and delivered fan-out count.
   */
  async emitBatch({
    channel,
    event = 'demo.batch.item',
    count = 5,
    payload = {},
  }: BatchEmitRequest): Promise<BatchEmitResult> {
    let delivered = 0;

    for (let i = 0; i < count; i += 1) {
      delivered += await this.sse.broadcast(
        channel,
        {
          event,
          data: {
            index: i,
            ...payload,
          },
          timestamp: Date.now(),
        },
      );
    }

    return {
      totalEvents: count,
      delivered,
    };
  }

  /**
   * @description Emits a finite list of values to a client stream.
   * @param request Iterable emission options for a single client.
   * @returns Sent item count and cancellation state.
   */
  async emitIterable({
    clientId,
    items,
    event = 'demo.iterable.item',
    intervalMs = 0,
    cancelAfterMs,
  }: IterableEmitRequest): Promise<StreamEmitResult> {
    const abortController = new AbortController();
    let timeout: NodeJS.Timeout | undefined;

    if (typeof cancelAfterMs === 'number' && cancelAfterMs > 0) {
      timeout = setTimeout(() => abortController.abort(), cancelAfterMs);
    }

    try {
      const sent = await this.sse.pipeIterable({
        clientId,
        iterable: this.createIterable(items, intervalMs),
        event,
        signal: abortController.signal,
      });

      return {
        sent,
        cancelled: abortController.signal.aborted,
      };
    } finally {
      if (timeout) {
        clearTimeout(timeout);
      }
    }
  }

  /**
   * @description Emits chunks from a readable source to a client stream.
   * @param request Readable source and cancellation options.
   * @returns Sent chunk count and cancellation state.
   */
  async emitReadable({
    clientId,
    chunks,
    event = 'demo.readable.chunk',
    cancelAfterMs,
  }: ReadableEmitRequest): Promise<StreamEmitResult> {
    const abortController = new AbortController();
    let timeout: NodeJS.Timeout | undefined;

    if (typeof cancelAfterMs === 'number' && cancelAfterMs > 0) {
      timeout = setTimeout(() => abortController.abort(), cancelAfterMs);
    }

    try {
      const sent = await this.sse.pipeReadable({
        clientId,
        readable: Readable.from(chunks ?? []),
        event,
        signal: abortController.signal,
      });

      return {
        sent,
        cancelled: abortController.signal.aborted,
      };
    } finally {
      if (timeout) {
        clearTimeout(timeout);
      }
    }
  }

  /**
   * @description Streams synthetic events and optionally cancels on signal or emitted error event.
   * @param request Scenario options controlling pacing, failures, and cancellation behavior.
   * @returns Stream totals and cancellation indicators.
   */
  async streamAndCancelOnSignalOrEvent({
    clientId,
    channel = 'demo.stream.errors',
    total = 20,
    intervalMs = 50,
    failAt,
    cancelAfterMs,
    cancelOnError = true,
  }: CancellableStreamRequest): Promise<CancellableStreamResult> {
    const abortController = new AbortController();
    let timeout: NodeJS.Timeout | undefined;
    let cancelledByEvent = false;

    if (typeof cancelAfterMs === 'number' && cancelAfterMs > 0) {
      timeout = setTimeout(() => abortController.abort(), cancelAfterMs);
    }

    const iterable = this.createStreamScenarioIterable(total, intervalMs, {
      failAt,
      onError: async (index) => {
        this.eventBus.emit('stream.aborted', {
          clientId,
          reason: 'event.error',
        });

        await this.sse.broadcast(channel, {
          event: 'event.error',
          data: {
            clientId,
            index,
            message: 'Synthetic error event',
          },
          timestamp: Date.now(),
        });

        if (cancelOnError) {
          await this.sse.broadcast(channel, {
            event: 'event.cancel',
            data: {
              clientId,
              reason: 'event.error',
            },
            timestamp: Date.now(),
          });

          await this.sse.broadcast(channel, {
            event: 'event.cancle',
            data: {
              clientId,
              reason: 'event.error',
            },
            timestamp: Date.now(),
          });

          this.sse.cancelStream(clientId, 'event.error');
          cancelledByEvent = true;
        }
      },
    });

    try {
      const sent = await this.sse.pipeIterable({
        clientId,
        iterable,
        event: 'demo.stream.tick',
        signal: abortController.signal,
      });

      return {
        sent,
        abortedBySignal: abortController.signal.aborted,
        cancelledByEvent,
      };
    } finally {
      if (timeout) {
        clearTimeout(timeout);
      }
    }
  }

  /**
   * @description Creates an async iterable with optional delay between items.
   * @param items Source items to yield.
   * @param intervalMs Delay in milliseconds between yielded items.
   * @returns Async iterable sequence of source items.
   */
  private async *createIterable(
    items: unknown[],
    intervalMs: number,
  ): AsyncIterable<unknown> {
    for (const item of items) {
      if (intervalMs > 0) {
        await this.delay(intervalMs);
      }
      yield item;
    }
  }

  /**
   * @description Creates a synthetic stream used by cancellation and failure test scenarios.
   * @param total Number of items to generate.
   * @param intervalMs Delay in milliseconds between generated items.
   * @param options Failure trigger and error callback options.
   * @returns Async iterable sequence of generated payload objects.
   */
  private async *createStreamScenarioIterable(
    total: number,
    intervalMs: number,
    options: StreamScenarioOptions,
  ): AsyncIterable<unknown> {
    for (let i = 0; i < total; i += 1) {
      if (typeof options.failAt === 'number' && i === options.failAt) {
        await options.onError(i);
        if (options.failAt >= 0) {
          break;
        }
      }

      if (intervalMs > 0) {
        await this.delay(intervalMs);
      }

      yield {
        index: i,
        at: Date.now(),
      };
    }
  }

  /**
   * @description Waits for a duration in milliseconds.
   * @param ms Delay duration in milliseconds.
   * @returns Resolves when the delay is complete.
   */
  private async delay(ms: number): Promise<void> {
    if (ms <= 0) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
  }

  /**
   * @description Validates that a client identifier is present.
   * @param clientId Optional client identifier.
   * @returns A non-empty client identifier string.
   */
  ensureClientId(clientId?: string): string {
    if (!clientId) {
      throw new BadRequestException('Missing clientId');
    }

    return clientId;
  }
}