import { DynamicModule, Module } from '@nestjs/common';
import { ChannelRegistryService } from './channel-registry.service';
import { ConnectionPoolService } from './connection-pool.service';
import {
  SSE_CHANNEL_REGISTRY,
  SSE_OPTIONS,
  SSE_TRANSPORT,
} from './sse.constants';
import {
  defaultSseModuleOptions,
  type SseModuleAsyncOptions,
  type SseModuleOptions,
} from './sse-options.interface';
import { SseController } from './sse.controller';
import { SseEventBusService } from './sse-event-bus.service';
import { SseService } from './sse.service';
import { RedisSseTransport } from './transports/redis.transport';
import type { SseTransportAdapter } from './transports/transport.interface';

const noOpTransport: SseTransportAdapter = {
  async publish(): Promise<void> {
    return;
  },
  async subscribe(): Promise<void> {
    return;
  },
  async close(): Promise<void> {
    return;
  },
};

/**
 * Resolve a transport adapter from module options.
 */
function createTransport(options: SseModuleOptions): SseTransportAdapter {
  if (!options.transport) {
    return noOpTransport;
  }

  if (options.transport.driver === 'redis') {
    return new RedisSseTransport(options.transport.options);
  }

  if (options.transport.driver === 'custom') {
    return options.transport.factory();
  }

  return noOpTransport;
}

@Module({})
/**
 * Configurable SSE module with sync and async registration APIs.
 */
export class SseModule {
  /**
   * Register the SSE module using static options.
   */
  static forRoot(options: SseModuleOptions = {}): DynamicModule {
    const merged: SseModuleOptions = {
      ...defaultSseModuleOptions,
      ...options,
    };

    return {
      module: SseModule,
      controllers: [SseController],
      providers: [
        {
          provide: SSE_OPTIONS,
          useValue: merged,
        },
        {
          provide: SSE_TRANSPORT,
          useFactory: (moduleOptions: SseModuleOptions) =>
            createTransport(moduleOptions),
          inject: [SSE_OPTIONS],
        },
        {
          provide: SSE_CHANNEL_REGISTRY,
          useFactory: (moduleOptions: SseModuleOptions) =>
            new ChannelRegistryService(moduleOptions.channels ?? []),
          inject: [SSE_OPTIONS],
        },
        ConnectionPoolService,
        SseEventBusService,
        SseService,
      ],
      exports: [
        SseService,
        SseEventBusService,
        SSE_OPTIONS,
        SSE_TRANSPORT,
        SSE_CHANNEL_REGISTRY,
      ],
    };
  }

  /**
   * Register the SSE module using an async options factory.
   */
  static forRootAsync(options: SseModuleAsyncOptions): DynamicModule {
    return {
      module: SseModule,
      controllers: [SseController],
      providers: [
        {
          provide: SSE_OPTIONS,
          useFactory: async (...args: any[]) => {
            const loaded = await options.useFactory(...args);
            return {
              ...defaultSseModuleOptions,
              ...loaded,
            };
          },
          inject: options.inject ?? [],
        },
        {
          provide: SSE_TRANSPORT,
          useFactory: (moduleOptions: SseModuleOptions) =>
            createTransport(moduleOptions),
          inject: [SSE_OPTIONS],
        },
        {
          provide: SSE_CHANNEL_REGISTRY,
          useFactory: (moduleOptions: SseModuleOptions) =>
            new ChannelRegistryService(moduleOptions.channels ?? []),
          inject: [SSE_OPTIONS],
        },
        ConnectionPoolService,
        SseEventBusService,
        SseService,
      ],
      exports: [
        SseService,
        SseEventBusService,
        SSE_OPTIONS,
        SSE_TRANSPORT,
        SSE_CHANNEL_REGISTRY,
      ],
    };
  }
}
