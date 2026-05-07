# nest-sse

Scalable, event-driven Server-Sent Events (SSE) package for NestJS.

This package provides:

- Stream lifecycle management (open, close, cancel, replace)
- Connection pool with channel subscriptions and metadata
- Channel broadcasts with audience targeting (`all`, `public`, `authenticated`)
- Heartbeat delivery and dead-connection cleanup
- Event buffering and automatic batch flush
- Iterable and readable stream piping to clients
- channel patterns with authorization callbacks
- Optional multi-node transports (`redis` or custom adapter)
- Typed lifecycle event bus for observability/hooks
- Static and async module configuration

## Table Of Contents

- Installation
- Basic Setup
- Complete Module Configuration
- Transport Drivers
- Channel Registry And Authorization
- HTTP API (Built-in Controller)
- Programmatic API (SseService)
- Event Bus Hooks
- Event Envelope And Wire Format
- Client Example (Browser EventSource)
- Exports
- Testing
- Production Notes

## Installation

```bash
npm install
```

Run the sample app:

```bash
npm run start:dev
```

Default port is `3000`.

If you use Redis transport:

```bash
npm i ioredis
```

## Basic Setup

```ts
import { Module } from '@nestjs/common';
import { SseModule } from './src/sse';

@Module({
  imports: [
    SseModule.forRoot(),
  ],
})
export class AppModule {}
```

## Complete Module Configuration

```ts
import { Module } from '@nestjs/common';
import {
  authenticatedChannel,
  publicChannel,
  SseModule,
  type SseModuleOptions,
} from './src/sse';

const sseOptions: SseModuleOptions = {
  maxConnections: 5000,
  maxChannelsPerConnection: 64,
  heartbeatIntervalMs: 12_000,
  deadConnectionMs: 40_000,
  bufferMaxEvents: 8,
  bufferFlushMs: 150,
  batchEventName: 'events.batch',
  cancelEventName: 'stream.cancel',
  heartbeatEventName: 'heartbeat',
  channels: [
    publicChannel('feed.public'),
    authenticatedChannel('orders.{orderId}', ({ params, connection }) => {
      return {
        allowed: String(connection.metadata.orderId ?? '') === params.orderId,
        reason: 'order-mismatch',
      };
    }),
  ],
};

@Module({
  imports: [SseModule.forRoot(sseOptions)],
})
export class AppModule {}
```

Async configuration:

```ts
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { SseModule } from './src/sse';

@Module({
  imports: [
    ConfigModule.forRoot(),
    SseModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        maxConnections: config.get<number>('SSE_MAX_CONNECTIONS', 10_000),
      }),
    }),
  ],
})
export class AppModule {}
```

### Option Reference

```ts
interface SseModuleOptions {
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
```

Runtime defaults:

- `maxConnections`: `10_000`
- `maxChannelsPerConnection`: `128`
- `heartbeatIntervalMs`: `15_000`
- `deadConnectionMs`: `45_000`
- `bufferMaxEvents`: `16`
- `bufferFlushMs`: `250`
- `batchEventName`: `events.batch`
- `cancelEventName`: `stream.cancel`
- `heartbeatEventName`: `heartbeat`

## Transport Drivers

### Single instance

No transport configuration is required for single-instance deployments.

### 1) Redis Transport (multi instance fan-out)

```ts
SseModule.forRoot({
  transport: {
    driver: 'redis',
    options: {
      host: '127.0.0.1',
      port: 6379,
      password: process.env.REDIS_PASSWORD,
      db: 0,
      channel: 'nest-sse:broadcast',
    },
  },
});
```

### 2) Custom Transport Adapter

```ts
import { type SseTransportAdapter, type TransportMessage } from './src/sse';

class KafkaTransport implements SseTransportAdapter {
  async publish(message: TransportMessage): Promise<void> {
    // Publish message to your broker.
  }

  async subscribe(handler: (message: TransportMessage) => void): Promise<void> {
    // Subscribe and invoke handler for incoming messages.
  }

  async close(): Promise<void> {
    // Cleanup resources.
  }
}

SseModule.forRoot({
  transport: {
    driver: 'custom',
    factory: () => new KafkaTransport(),
  },
});
```

## Channel Registry And Authorization

Define rules with channel patterns:

- `public` channels accept public and authenticated streams.
- `authenticated` channels require authenticated streams.
- `authorize(context)` adds custom policy checks.

Pattern params use braces, for example `orders.{orderId}`.

```ts
import { authenticatedChannel, publicChannel } from './src/sse';

channels: [
  publicChannel('news.global'),
  authenticatedChannel('users.{userId}.notifications', ({ params, requestMetadata }) => {
    const currentUserId = String(requestMetadata.userId ?? '');
    return {
      allowed: currentUserId === params.userId,
      reason: 'forbidden-user-channel',
      metadata: { channelCheckedAt: Date.now() },
    };
  }),
];
```

`authorize` context shape:

```ts
{
  clientId: string;
  channel: string;
  params: Record<string, string>;
  requestMetadata: Record<string, unknown>;
}
```

You can also register channels at runtime by injecting `SSE_CHANNEL_REGISTRY`:

```ts
import { Inject, Injectable } from '@nestjs/common';
import {
  SSE_CHANNEL_REGISTRY,
  type ChannelAuthorizeFn,
  type ChannelRegistry,
} from './src/sse';

@Injectable()
export class ChannelSetupService {
  constructor(
    @Inject(SSE_CHANNEL_REGISTRY)
    private readonly registry: ChannelRegistry,
  ) {}

  register(): void {
    const authorize: ChannelAuthorizeFn = ({ requestMetadata, params }) => ({
      allowed: String(requestMetadata.userId ?? '') === params.userId,
      reason: 'user-mismatch',
    });

    this.registry.registerPublic('status.public');
    this.registry.registerAuthenticated('users.{userId}.alerts', authorize);
  }
}
```

Decorator helper for storing channel definitions on providers:

```ts
import { SseChannels, publicChannel } from './src/sse';

@SseChannels([publicChannel('decorator.example')])
export class ExampleProvider {}
```

## HTTP API (Built-in Controller)

The module ships with a controller mounted at `/sse`.

### Open streams

Public stream:

```bash
curl -N "http://localhost:3000/sse/events?clientId=client-public-1"
```

Authenticated demo stream:

```bash
curl -N -H "x-user-id: 42" "http://localhost:3000/demo/connect/auth?clientId=client-auth-42"
```

### Subscribe / unsubscribe

```bash
curl -X POST http://localhost:3000/sse/subscribe \
  -H "content-type: application/json" \
  -d '{"clientId":"client-public-1","channel":"feed.public","metadata":{"app":"web"}}'

curl -X POST http://localhost:3000/sse/unsubscribe \
  -H "content-type: application/json" \
  -d '{"clientId":"client-public-1","channel":"feed.public"}'
```

### Broadcast to a channel

```bash
curl -X POST http://localhost:3000/sse/broadcast \
  -H "content-type: application/json" \
  -d '{
    "channel":"feed.public",
    "event":"feed.updated",
    "data":{"message":"new update"},
    "target":"all"
  }'
```

`target` can be `all`, `public`, or `authenticated`.

### Pipe iterable items to a client

```bash
curl -X POST http://localhost:3000/sse/pipe/iterable \
  -H "content-type: application/json" \
  -d '{
    "clientId":"client-public-1",
    "items":[{"id":1},{"id":2},{"id":3}],
    "event":"stream.item"
  }'
```

### Cancel a stream

```bash
curl -X POST http://localhost:3000/sse/cancel \
  -H "content-type: application/json" \
  -d '{"clientId":"client-public-1","reason":"manual-stop"}'
```

### Connection diagnostics

```bash
curl http://localhost:3000/sse/connections
```

Unauthorized subscription attempts return `403` when channel policies deny access.

## Programmatic API (SseService)

Inject `SseService` when you need direct control from your own services/controllers.

```ts
import { Injectable } from '@nestjs/common';
import { Readable } from 'node:stream';
import { SseService } from './src/sse';

@Injectable()
export class NotificationsService {
  constructor(private readonly sse: SseService) {}

  async broadcastSystemStatus(): Promise<number> {
    return this.sse.broadcast(
      'system.status',
      {
        event: 'status.changed',
        data: { healthy: true },
      },
      { target: 'all' },
    );
  }

  async streamIterable(clientId: string): Promise<number> {
    return this.sse.pipeIterable({
      clientId,
      iterable: [1, 2, 3, 4, 5],
      event: 'numbers.item',
    });
  }

  async streamReadable(clientId: string): Promise<number> {
    const readable = Readable.from(['chunk-a', 'chunk-b', 'chunk-c']);
    return this.sse.pipeReadable({
      clientId,
      readable,
      event: 'chunks.item',
    });
  }

  cancel(clientId: string): boolean {
    return this.sse.cancelStream(clientId, 'admin-cancelled');
  }

  diagnostics(channel: string): {
    totalConnections: number;
    subscribers: number;
    snapshots: ReturnType<SseService['connectionSnapshots']>;
  } {
    return {
      totalConnections: this.sse.connectionCount(),
      subscribers: this.sse.channelCount(channel),
      snapshots: this.sse.connectionSnapshots(),
    };
  }

  annotate(clientId: string): boolean {
    return this.sse.setMetadata(clientId, { lastTouchedBy: 'system' });
  }
}
```

Available core methods:

- `openStream(options)`
- `closeConnection(clientId, reason?)`
- `subscribe(options)`
- `unsubscribe(options)`
- `broadcast(channel, envelope, { target? })`
- `pipeIterable(options)`
- `pipeReadable(options)`
- `cancelStream(clientId, reason?)`
- `connectionCount()` / `channelCount(channel)`
- `connectionSnapshot(clientId)` / `connectionSnapshots()`
- `setMetadata(clientId, metadata)`
- `waitForClose(request)`

## Event Bus Hooks

`SseEventBusService` exposes a typed, in-memory event bus for lifecycle hooks.

```ts
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { SseEventBusService } from './src/sse';

@Injectable()
export class SseMetricsService implements OnModuleInit, OnModuleDestroy {
  private unsubscribe?: () => void;

  constructor(private readonly bus: SseEventBusService) {}

  onModuleInit(): void {
    this.unsubscribe = this.bus.on('broadcast.sent', ({ channel, target, count }) => {
      console.log('[sse] broadcast', { channel, target, count });
    });
  }

  onModuleDestroy(): void {
    this.unsubscribe?.();
  }
}
```

Lifecycle events:

- `connection.opened`
- `connection.closed`
- `connection.heartbeat.sent`
- `connection.heartbeat.timeout`
- `subscription.added`
- `subscription.removed`
- `broadcast.sent`
- `stream.aborted`
- `stream.completed`
- `stream.cancelled`
- `connection.metadata.updated`

## Event Envelope And Wire Format

Event envelope type:

```ts
interface SseEventEnvelope<T = unknown> {
  id?: string;
  event: string;
  data: T;
  retry?: number;
  timestamp?: number;
  metadata?: Record<string, unknown>;
}
```

Notes:

- If `id` is omitted, the service generates a monotonic ID.
- If `timestamp` is omitted, the service adds one.
- When buffer size is greater than 1, multiple events may flush as one wrapper event.
- Wrapper event name defaults to `events.batch`.
- Cancellation event name defaults to `stream.cancel`.
- Heartbeat event name defaults to `heartbeat`.

## Client Example (Browser EventSource)

```ts
const clientId = 'web-client-1';
const es = new EventSource(`/sse/events?clientId=${encodeURIComponent(clientId)}`);

es.addEventListener('connection.ready', (evt) => {
  console.log('connected', JSON.parse((evt as MessageEvent).data));
});

es.addEventListener('events.batch', (evt) => {
  const events = JSON.parse((evt as MessageEvent).data);
  console.log('batch', events);
});

es.addEventListener('feed.updated', (evt) => {
  console.log('feed update', JSON.parse((evt as MessageEvent).data));
});

es.addEventListener('heartbeat', () => {
  // Keepalive signal.
});

es.addEventListener('stream.cancel', (evt) => {
  console.log('cancelled', JSON.parse((evt as MessageEvent).data));
  es.close();
});
```

## Exports

Package exports include:

- `SseModule`
- `SseService`
- `SseEventBusService`
- `ConnectionPoolService`
- `ChannelRegistryService`
- `SSE_CHANNEL_REGISTRY`
- `SseChannels`, `publicChannel`, `authenticatedChannel`, `SSE_CHANNELS_METADATA`
- `RedisSseTransport`
- Type exports from `sse.types.ts`, `sse-options.interface.ts`, and `transports/transport.interface.ts`

## Testing

```bash
npm run build
npm test -- --runInBand
```

## Production Notes

- Disable compression for `text/event-stream` routes.
- Keep proxy/read timeouts greater than `heartbeatIntervalMs`.
- Use Redis transport for horizontal scaling across app instances.
- Ensure `clientId` values are stable and unique per logical stream owner.
