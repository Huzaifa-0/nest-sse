import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { once } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Readable } from 'node:stream';
import { ConnectionPoolService, type ConnectionRecord } from './connection-pool.service';
import {
  SSE_CHANNEL_REGISTRY,
  SSE_OPTIONS,
  SSE_TRANSPORT,
} from '../constants/sse.constants';
import {
  defaultSseModuleOptions,
  type SseModuleOptions,
} from '../interfaces/sse-options.interface';
import { SseEventBusService } from './sse-event-bus.service';
import type {
  BroadcastTarget,
  ChannelRegistry,
  ConnectionSnapshot,
  PipeIterableOptions,
  PipeReadableOptions,
  SseEventEnvelope,
  StreamOpenOptions,
  SubscribeOptions,
  SubscribeResult,
} from '../types/sse.types';
import type { SseTransportAdapter } from '../interfaces/transport.interface';

@Injectable()
/**
 * @description Core SSE orchestration service for connections, topics, and event delivery.
 */
export class SseService implements OnModuleInit, OnModuleDestroy {
  private heartbeatTimer?: NodeJS.Timeout;
  private eventSequence = 0;
  private readonly options: Required<Omit<SseModuleOptions, 'transport' | 'channels'>>;

  constructor(
    private readonly pool: ConnectionPoolService,
    private readonly eventBus: SseEventBusService,
    @Inject(SSE_OPTIONS) private readonly rawOptions: SseModuleOptions,
    @Inject(SSE_TRANSPORT) private readonly transport: SseTransportAdapter,
    @Inject(SSE_CHANNEL_REGISTRY)
    private readonly channelRegistry: ChannelRegistry,
  ) {
    this.options = {
      ...defaultSseModuleOptions,
      ...rawOptions,
    };
  }

  /**
   * @description Subscribes to transport events and starts heartbeat scheduling.
   * @returns Promise resolved when startup hooks complete.
   */
  async onModuleInit(): Promise<void> {
    await this.transport.subscribe((message) => {
      void this.deliverLocal(message.topic, message.envelope);
    });

    this.startHeartbeat();
  }

  /**
   * @description Closes active streams and releases transport resources.
   * @returns Promise resolved after shutdown cleanup finishes.
   */
  async onModuleDestroy(): Promise<void> {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }

    for (const connection of this.pool.all()) {
      this.closeConnection(connection.clientId, 'shutdown');
    }

    await this.transport.close();
  }

  /**
   * @description Opens or replaces a client SSE stream.
   * @param options Stream open options including client id and HTTP primitives.
   * @returns Nothing.
   */
  openStream(options: StreamOpenOptions): void {
    if (this.pool.count() >= this.options.maxConnections) {
      options.response.statusCode = 429;
      options.response.end('Connection limit reached');
      return;
    }

    this.writeHeaders(options.response);

    const now = Date.now();
    const existing = this.pool.get(options.clientId);
    if (existing) {
      this.closeConnection(options.clientId, 'replaced');
    }

    const record: ConnectionRecord = {
      clientId: options.clientId,
      response: options.response,
      metadata: options.metadata ?? {},
      topics: new Set<string>(),
      createdAt: now,
      lastSeenAt: now,
      bufferedEvents: [],
      isClosed: false,
    };

    this.pool.create(record);
    this.eventBus.emit('connection.opened', {
      clientId: options.clientId,
    });

    const onAbort = () => {
      this.eventBus.emit('stream.aborted', {
        clientId: options.clientId,
        reason: 'request-aborted',
      });
      this.closeConnection(options.clientId, 'request-aborted');
    };

    options.request.on('close', onAbort);
    options.request.on('aborted', onAbort);

    this.enqueueOrWrite(options.clientId, {
      event: 'connection.ready',
      data: { clientId: options.clientId, ts: now },
      timestamp: now,
    });
  }

  /**
   * @description Closes an active connection by client id.
   * @param clientId Unique client identifier.
   * @param reason Optional closure reason.
   * @returns Nothing.
   */
  closeConnection(clientId: string, reason = 'closed'): void {
    const connection = this.pool.get(clientId);
    if (!connection || connection.isClosed) {
      return;
    }

    connection.isClosed = true;

    try {
      connection.response.end();
    } catch {
      // Ignore close errors from dead sockets.
    }

    this.pool.remove(clientId);
    this.eventBus.emit('connection.closed', { clientId, reason });
  }

  /**
   * @description Subscribes a client to a topic with structured failure reasons.
   * @param options Subscription options containing client id, topic, and metadata.
   * @returns Structured subscribe result.
   */
  subscribe({
    clientId,
    topic,
    metadata,
  }: SubscribeOptions): SubscribeResult {
    const connection = this.pool.get(clientId);
    if (!connection) {
      return { ok: false, reason: 'connection-not-found' };
    }

    if (connection.isClosed) {
      return { ok: false, reason: 'connection-closed' };
    }

    if (!connection.topics.has(topic) && connection.topics.size >= this.options.maxTopicsPerConnection) {
      return { ok: false, reason: 'max-topics-reached' };
    }

    const resolved = this.channelRegistry.resolve(topic);
    let mergedMetadata: Record<string, unknown> = {...metadata};

    if (!resolved){
      return { ok: false, reason: 'topic-not-found' };
    }

    const authorizeResult = this.channelRegistry.authorize(resolved, {
      clientId,
      topic,
      params: resolved.params,
      requestMetadata: metadata ?? {},
    });

    if (!authorizeResult.allowed) {
      return { ok: false, reason: 'unauthorized' };
    }

    if (authorizeResult.metadata) {
      mergedMetadata = {
        ...mergedMetadata,
        ...authorizeResult.metadata,
      };
    }

    if(Object.keys(mergedMetadata).length > 0){
      this.pool.setMetadata(clientId, mergedMetadata);
      this.eventBus.emit('connection.metadata.updated', { clientId });
    }

    const ok = this.pool.subscribe(clientId, topic);
    if (ok) {
      this.eventBus.emit('subscription.added', { clientId, topic });
    }

    if (!ok) {
      return { ok: false, reason: 'connection-not-found' };
    }

    return { ok: true };
  }

  /**
   * @description Unsubscribes a client from a topic.
   * @param options Client and topic subscription identifiers.
   * @returns True when an existing subscription was removed.
   */
  unsubscribe({ clientId, topic }: SubscribeOptions): boolean {
    const ok = this.pool.unsubscribe(clientId, topic);
    if (ok) {
      this.eventBus.emit('subscription.removed', { clientId, topic });
    }
    return ok;
  }

  /**
   * @description Returns active connection count.
   * @returns Number of active connections.
   */
  connectionCount(): number {
    return this.pool.count();
  }

  /**
   * @description Returns subscriber count for a topic.
   * @param topic Topic name to inspect.
   * @returns Number of subscribers currently attached to the topic.
   */
  topicCount(topic: string): number {
    return this.pool.topicCount(topic);
  }

  /**
   * @description Returns diagnostics for a single connection.
   * @param clientId Unique client identifier.
   * @returns Serialized connection snapshot or null when missing.
   */
  connectionSnapshot(clientId: string): ConnectionSnapshot | null {
    return this.pool.snapshot(clientId);
  }

  /**
   * @description Returns diagnostics for all active connections.
   * @returns Array of serialized connection snapshots.
   */
  connectionSnapshots(): ConnectionSnapshot[] {
    return this.pool.snapshots();
  }

  /**
   * @description Merges metadata into a connection.
   * @param clientId Unique client identifier.
   * @param metadata Metadata patch to merge.
   * @returns True when metadata was applied.
   */
  setMetadata(clientId: string, metadata: Record<string, unknown>): boolean {
    const ok = this.pool.setMetadata(clientId, metadata);
    if (ok) {
      this.eventBus.emit('connection.metadata.updated', { clientId });
    }
    return ok;
  }

  /**
   * @description Broadcasts an envelope to topic subscribers.
   * @param topic Topic name to publish against.
   * @param envelope Event envelope to normalize and deliver.
   * @returns Number of local subscribers that received the event.
   */
  async broadcast(
    topic: string,
    envelope: SseEventEnvelope,
  ): Promise<number> {
    const normalized: SseEventEnvelope = {
      ...envelope,
      id: envelope.id ?? this.nextEventId(),
      timestamp: envelope.timestamp ?? Date.now(),
    };

    const target = this.resolveBroadcastTarget(topic);

    await this.transport.publish({
      topic,
      envelope: normalized,
      target,
    });

    return this.deliverLocal(topic, normalized);
  }

  /**
   * @description Emits a cancellation event and closes the stream.
   * @param clientId Unique client identifier.
   * @param reason Optional cancellation reason.
   * @returns True when the connection existed and was cancelled.
   */
  cancelStream(clientId: string, reason = 'cancellation-signal'): boolean {
    const connection = this.pool.get(clientId);
    if (!connection) {
      return false;
    }

    this.enqueueOrWrite(clientId, {
      id: this.nextEventId(),
      event: this.options.cancelEventName,
      data: { reason },
      timestamp: Date.now(),
    });

    this.flush(clientId);
    this.eventBus.emit('stream.cancelled', { clientId, reason });
    this.closeConnection(clientId, reason);
    return true;
  }

  /**
   * @description Pipes iterable items to an active stream as SSE events.
   * @param options Pipe options for source iterable and target client.
   * @returns Number of emitted events before completion or cancellation.
   */
  async pipeIterable({
    clientId,
    iterable,
    event = 'stream.item',
    signal,
  }: PipeIterableOptions): Promise<number> {
    let count = 0;

    for await (const item of iterable) {
      if (signal?.aborted) {
        this.cancelStream(clientId, 'abort-signal');
        this.eventBus.emit('stream.aborted', { clientId, reason: 'abort-signal' });
        return count;
      }

      this.enqueueOrWrite(clientId, {
        id: this.nextEventId(),
        event,
        data: item,
        timestamp: Date.now(),
      });
      count++;
    }

    this.flush(clientId);
    this.eventBus.emit('stream.completed', { clientId, count });
    return count;
  }

  /**
   * @description Pipes readable stream chunks to an active stream as SSE events.
   * @param options Pipe options for source readable and target client.
   * @returns Number of emitted events before completion or cancellation.
   */
  async pipeReadable({
    clientId,
    readable,
    event = 'stream.chunk',
    signal,
  }: PipeReadableOptions): Promise<number> {
    let count = 0;

    for await (const chunk of readable) {
      if (signal?.aborted) {
        this.cancelStream(clientId, 'abort-signal');
        this.eventBus.emit('stream.aborted', { clientId, reason: 'abort-signal' });
        return count;
      }

      this.enqueueOrWrite(clientId, {
        id: this.nextEventId(),
        event,
        data: chunk,
        timestamp: Date.now(),
      });
      count++;
    }

    this.flush(clientId);
    this.eventBus.emit('stream.completed', { clientId, count });
    return count;
  }

  /**
   * @description Delivers an envelope to in-process subscribers for a topic.
   * @param topic Topic name used to select subscribers.
   * @param envelope Event envelope to write.
   * @returns Number of in-process subscribers that received the event.
   */
  private async deliverLocal(
    topic: string,
    envelope: SseEventEnvelope,
  ): Promise<number> {
    const target = this.resolveBroadcastTarget(topic);

    const subscribers = this.pool.forTopic(topic);
    let count = 0;

    for (const connection of subscribers) {
      this.enqueueOrWrite(connection.clientId, envelope);
      count++;
    }

    this.eventBus.emit('broadcast.sent', { topic, target, count });
    return count;
  }

  private resolveBroadcastTarget(topic: string): BroadcastTarget {
    const resolved = this.channelRegistry.resolve(topic);
    return resolved?.definition.audience ?? 'all';
  }

  /**
   * @description Starts periodic heartbeat emission and dead-connection detection.
   * @returns Nothing.
   */
  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      const now = Date.now();

      for (const connection of this.pool.all()) {
        if (now - connection.lastSeenAt > this.options.deadConnectionMs) {
          this.eventBus.emit('connection.heartbeat.timeout', {
            clientId: connection.clientId,
          });
          this.closeConnection(connection.clientId, 'heartbeat-timeout');
          continue;
        }

        this.enqueueOrWrite(connection.clientId, {
          id: this.nextEventId(),
          event: this.options.heartbeatEventName,
          data: { ts: now },
          timestamp: now,
        });

        this.eventBus.emit('connection.heartbeat.sent', {
          clientId: connection.clientId,
        });
      }
    }, this.options.heartbeatIntervalMs);
  }

  /**
   * @description Buffers or writes an event for a client based on configured batch settings.
   * @param clientId Unique client identifier.
   * @param envelope Event envelope to enqueue or write.
   * @returns Nothing.
   */
  private enqueueOrWrite(clientId: string, envelope: SseEventEnvelope): void {
    const connection = this.pool.get(clientId);
    if (!connection || connection.isClosed) {
      return;
    }

    if (this.options.bufferMaxEvents <= 1) {
      this.writeEvent(connection.response, envelope);
      this.pool.updateLastSeen(clientId);
      return;
    }

    connection.bufferedEvents.push(envelope);

    if (connection.bufferedEvents.length >= this.options.bufferMaxEvents) {
      this.flush(clientId);
      return;
    }

    if (!connection.flushTimer) {
      connection.flushTimer = setTimeout(() => {
        this.flush(clientId);
      }, this.options.bufferFlushMs);
    }
  }

  /**
   * @description Flushes buffered events to a client.
   * @param clientId Unique client identifier.
   * @returns Nothing.
   */
  private flush(clientId: string): void {
    const connection = this.pool.get(clientId);
    if (!connection || connection.isClosed || connection.bufferedEvents.length === 0) {
      return;
    }

    if (connection.flushTimer) {
      clearTimeout(connection.flushTimer);
      connection.flushTimer = undefined;
    }

    let shouldTouchLastSeen = false;

    if (connection.bufferedEvents.length === 1) {
      const [single] = connection.bufferedEvents;
      this.writeEvent(connection.response, single);
      shouldTouchLastSeen = single.event !== this.options.heartbeatEventName;
    } else {
      const payload = connection.bufferedEvents.map((evt) => ({
        id: evt.id,
        event: evt.event,
        data: evt.data,
        retry: evt.retry,
        timestamp: evt.timestamp,
        metadata: evt.metadata,
      }));

      shouldTouchLastSeen = connection.bufferedEvents.some(
        (evt) => evt.event !== this.options.heartbeatEventName,
      );

      this.writeEvent(connection.response, {
        id: this.nextEventId(),
        event: this.options.batchEventName,
        data: payload,
        timestamp: Date.now(),
      });
    }

    connection.bufferedEvents = [];
    if (shouldTouchLastSeen) {
      this.pool.updateLastSeen(clientId);
    }
  }

  /**
   * @description Writes standard SSE response headers.
   * @param response HTTP response for the SSE stream.
   * @returns Nothing.
   */
  private writeHeaders(response: ServerResponse): void {
    response.setHeader('Content-Type', 'text/event-stream');
    response.setHeader('Cache-Control', 'no-cache, no-transform');
    response.setHeader('Connection', 'keep-alive');
    response.setHeader('X-Accel-Buffering', 'no');
    response.flushHeaders?.();
  }

  /**
   * @description Serializes and writes a single SSE event envelope.
   * @param response HTTP response for the SSE stream.
   * @param envelope Event envelope to serialize.
   * @returns Nothing.
   */
  private writeEvent(response: ServerResponse, envelope: SseEventEnvelope): void {
    const lines: string[] = [];
    if (envelope.id) {
      lines.push(`id: ${envelope.id}`);
    }
    if (envelope.event) {
      lines.push(`event: ${envelope.event}`);
    }
    if (typeof envelope.retry === 'number') {
      lines.push(`retry: ${envelope.retry}`);
    }

    const payload = JSON.stringify(envelope.data);
    for (const line of payload.split(/\r?\n/)) {
      lines.push(`data: ${line}`);
    }

    lines.push('', '');
    response.write(lines.join('\n'));
  }

  /**
   * @description Generates a monotonic event identifier.
   * @returns Event id string.
   */
  private nextEventId(): string {
    this.eventSequence += 1;
    return `${Date.now()}-${this.eventSequence}`;
  }

  /**
   * @description Awaits request stream closure.
   * @param request Incoming request stream.
   * @returns Promise resolved when the request closes.
   */
  async waitForClose(request: IncomingMessage): Promise<void> {
    await once(request, 'close');
  }
}
