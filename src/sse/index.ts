export { SseModule } from './sse.module';
export { SseService } from './services/sse.service';
export { ChannelRegistryService } from './services/channel-registry.service';
export { SSE_CHANNEL_REGISTRY } from './constants/sse.constants';
export { SseEventBusService } from './services/sse-event-bus.service';
export { ConnectionPoolService } from './services/connection-pool.service';
export {
  SseChannels,
  publicChannel,
  authenticatedChannel,
  SSE_CHANNELS_METADATA,
} from './decorators/channel.decorators';
export { RedisSseTransport } from './transports/redis.transport';
export type {
  SseModuleOptions,
  SseModuleAsyncOptions,
  RedisTransportOptions,
} from './interfaces/sse-options.interface';
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
} from './types/sse.types';
export type {
  SseTransportAdapter,
  TransportMessage,
} from './interfaces/transport.interface';
