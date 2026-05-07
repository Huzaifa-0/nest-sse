import type { SseTransportAdapter } from './transport.interface';
import type { ChannelDefinition } from '../types/sse.types';

/**
 * Connection options for the built-in Redis transport.
 */
export interface RedisTransportOptions {
  host: string;
  port: number;
  password?: string;
  db?: number;
  channel?: string;
}

/**
 * Configuration for the SSE module.
 */
export interface SseModuleOptions {
  maxConnections?: number;
  maxChannelsPerConnection?: number;
  heartbeatIntervalMs?: number;
  deadConnectionMs?: number;
  bufferMaxEvents?: number;
  bufferFlushMs?: number;
  batchEventName?: string;
  cancelEventName?: string;
  heartbeatEventName?: string;
  channels?: ChannelDefinition[];
  transport?:
    | { driver: 'redis'; options: RedisTransportOptions }
    | { driver: 'custom'; factory: () => SseTransportAdapter };
}

/**
 * Async factory options for module configuration.
 */
export interface SseModuleAsyncOptions {
  inject?: any[];
  useFactory: (...args: any[]) => Promise<SseModuleOptions> | SseModuleOptions;
}

/**
 * Default runtime values applied when module options are omitted.
 */
export const defaultSseModuleOptions: Required<
  Omit<SseModuleOptions, 'transport' | 'channels'>
> = {
  maxConnections: 10_000,
  maxChannelsPerConnection: 128,
  heartbeatIntervalMs: 15_000,
  deadConnectionMs: 45_000,
  bufferMaxEvents: 16,
  bufferFlushMs: 250,
  batchEventName: 'events.batch',
  cancelEventName: 'stream.cancel',
  heartbeatEventName: 'heartbeat',
};
