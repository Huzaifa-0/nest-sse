import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Readable } from 'node:stream';

/**
 * Delivery audience for channel broadcasts.
 */
export type BroadcastTarget = 'all' | 'public' | 'authenticated';

/**
 * Wire-level SSE payload sent to clients.
 *
 * @template T Payload data type.
 */
export interface SseEventEnvelope<T = unknown> {
  id?: string;
  event: string;
  data: T;
  retry?: number;
  timestamp?: number;
  metadata?: Record<string, unknown>;
}

/**
 * Options required to initialize a client stream.
 */
export interface StreamOpenOptions {
  clientId: string;
  request: IncomingMessage;
  response: ServerResponse;
  metadata?: Record<string, unknown>;
}

/**
 * Subscription request details.
 */
export interface SubscribeOptions {
  clientId: string;
  channel: string;
  metadata?: Record<string, unknown>;
}

/**
 * Access level for a channel definition.
 */
export type ChannelAudience = 'public' | 'authenticated';

/**
 * Named parameters extracted from a channel pattern.
 */
export type ChannelParams = Record<string, string>;

/**
 * Context provided to channel authorization callbacks.
 */
export interface ChannelAuthorizationContext {
  clientId: string;
  channel: string;
  params: ChannelParams;
  requestMetadata: Record<string, unknown>;
}

/**
 * Result returned from a channel authorization callback.
 */
export interface ChannelAuthorizationResult {
  allowed: boolean;
  metadata?: Record<string, unknown>;
  reason?: string;
}

/**
 * Channel authorization callback contract.
 */
export type ChannelAuthorizeFn =
  | ((
      context: ChannelAuthorizationContext,
    ) => boolean | ChannelAuthorizationResult);

/**
 * Channel registration definition.
 */
export interface ChannelDefinition {
  pattern: string;
  audience?: ChannelAudience;
  authorize?: ChannelAuthorizeFn;
}

/**
 * Matched channel definition details for a channel.
 */
export interface ResolvedChannel {
  definition: ChannelDefinition;
  params: ChannelParams;
}

/**
 * Registry contract used by the SSE service.
 */
export interface ChannelRegistry {
  register(definition: ChannelDefinition): void;
  registerPublic(pattern: string): void;
  registerAuthenticated(pattern: string, authorize?: ChannelAuthorizeFn): void;
  resolve(channel: string): ResolvedChannel | null;
  authorize(
    resolved: ResolvedChannel,
    context: ChannelAuthorizationContext,
  ): ChannelAuthorizationResult;
}

/**
 * Structured subscribe failure reasons.
 */
export type SubscribeFailureReason =
  | 'connection-not-found'
  | 'connection-closed'
  | 'max-channels-reached'
  | 'unauthorized'
  | 'channel-not-found';

/**
 * Subscribe operation result.
 */
export interface SubscribeResult {
  ok: boolean;
  reason?: SubscribeFailureReason;
}

/**
 * Stream items from an iterable source to an active client connection.
 */
export interface PipeIterableOptions {
  clientId: string;
  iterable: AsyncIterable<unknown> | Iterable<unknown>;
  event?: string;
  signal?: AbortSignal;
}

/**
 * Stream chunks from a Node readable stream to an active client connection.
 */
export interface PipeReadableOptions {
  clientId: string;
  readable: Readable;
  event?: string;
  signal?: AbortSignal;
}

/**
 * Serializable connection details for diagnostics and metrics endpoints.
 */
export interface ConnectionSnapshot {
  clientId: string;
  channels: string[];
  metadata: Record<string, unknown>;
  createdAt: number;
  lastSeenAt: number;
  bufferedEvents: number;
}

/**
 * Internal lifecycle events emitted by the SSE event bus.
 */
export interface SseLifecycleEvents {
  'connection.opened': { clientId: string };
  'connection.closed': { clientId: string; reason: string };
  'connection.heartbeat.sent': { clientId: string };
  'connection.heartbeat.timeout': { clientId: string };
  'subscription.added': { clientId: string; channel: string };
  'subscription.removed': { clientId: string; channel: string };
  'broadcast.sent': { channel: string; target: BroadcastTarget; count: number };
  'stream.aborted': { clientId: string; reason: string };
  'stream.completed': { clientId: string; count: number };
  'stream.cancelled': { clientId: string; reason: string };
  'connection.metadata.updated': { clientId: string };
}
