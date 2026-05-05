export { SseModule } from './sse.module';
export { SseService } from './sse.service';
export { ChannelRegistryService } from './channel-registry.service';
export { SSE_CHANNEL_REGISTRY } from './sse.constants';
export { SseEventBusService } from './sse-event-bus.service';
export { ConnectionPoolService } from './connection-pool.service';
export {
  SseChannels,
  publicChannel,
  authenticatedChannel,
  SSE_CHANNELS_METADATA,
} from './channel.decorators';
export { RedisSseTransport } from './transports/redis.transport';
export type {
  SseModuleOptions,
  SseModuleAsyncOptions,
  RedisTransportOptions,
} from './sse-options.interface';
export type {
  SseEventEnvelope,
  BroadcastTarget,
  ChannelAudience,
  ChannelAuthorizationContext,
  ChannelAuthorizationResult,
  ChannelAuthorizeFn,
  ChannelDefinition,
  ChannelParams,
  ChannelRegistry,
  ResolvedChannel,
  ConnectionSnapshot,
  StreamOpenOptions,
  SubscribeOptions,
  SubscribeResult,
  SubscribeFailureReason,
  PipeIterableOptions,
  PipeReadableOptions,
} from './sse.types';
export type {
  SseTransportAdapter,
  TransportMessage,
} from './transports/transport.interface';
