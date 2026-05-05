import type { RedisTransportOptions } from '../sse-options.interface';
import type { SseTransportAdapter, TransportMessage } from './transport.interface';

type RedisLike = {
  publish(channel: string, message: string): Promise<number>;
  subscribe(channel: string): Promise<number>;
  on(event: 'message', listener: (channel: string, message: string) => void): void;
  off(event: 'message', listener: (channel: string, message: string) => void): void;
  quit(): Promise<'OK'>;
};

type DynamicImportFn = (modulePath: string) => Promise<unknown>;
type RedisCtor = new (...args: unknown[]) => RedisLike;

interface RedisModule {
  default: RedisCtor;
}

const hasRedisCtor = (moduleValue: unknown): moduleValue is RedisModule => {
  if (typeof moduleValue !== 'object' || moduleValue === null) {
    return false;
  }
  return 'default' in moduleValue;
};

/**
 * @description Redis-backed transport for multi-instance SSE fan-out.
 */
export class RedisSseTransport implements SseTransportAdapter {
  private readonly channel: string;
  private pubClient?: RedisLike;
  private subClient?: RedisLike;
  private listener?: (channel: string, message: string) => void;

  /**
   * @description Creates a transport using Redis connection options.
   * @param options Redis connection options and channel overrides.
   * @returns Constructed Redis transport instance.
   */
  constructor(private readonly options: RedisTransportOptions) {
    this.channel = options.channel ?? 'nest-sse:broadcast';
  }

  /**
   * @description Lazily creates a Redis client via dynamic import to keep dependency optional.
   * @returns Promise resolved with an initialized Redis-like client.
   */
  private async createClient(): Promise<RedisLike> {
    const dynamicImport = new Function(
      'modulePath',
      'return import(modulePath)',
    ) as DynamicImportFn;

    const mod = await dynamicImport('ioredis').catch(() => null);
    if (!hasRedisCtor(mod)) {
      throw new Error(
        'ioredis is required for Redis transport. Install with: npm i ioredis',
      );
    }

    return new mod.default({
      host: this.options.host,
      port: this.options.port,
      password: this.options.password,
      db: this.options.db,
    });
  }

  /**
   * @description Publishes a transport message to the configured Redis channel.
   * @param message Transport message payload.
   * @returns Promise resolved when publish completes.
   */
  async publish(message: TransportMessage): Promise<void> {
    if (!this.pubClient) {
      this.pubClient = await this.createClient();
    }

    await this.pubClient.publish(this.channel, JSON.stringify(message));
  }

  /**
   * @description Subscribes to Redis pub/sub messages and forwards parsed payloads.
   * @param handler Callback invoked for each parsed transport message.
   * @returns Promise resolved when subscription is active.
   */
  async subscribe(handler: (message: TransportMessage) => void): Promise<void> {
    if (!this.subClient) {
      this.subClient = await this.createClient();
      await this.subClient.subscribe(this.channel);
    }

    this.listener = (_channel: string, message: string) => {
      try {
        const parsed = JSON.parse(message) as TransportMessage;
        handler(parsed);
      } catch {
        // Ignore malformed transport payloads.
      }
    };

    this.subClient.on('message', this.listener);
  }

  /**
   * @description Closes Redis clients and clears local listener references.
   * @returns Promise resolved when shutdown attempts complete.
   */
  async close(): Promise<void> {
    if (this.subClient && this.listener) {
      this.subClient.off('message', this.listener);
    }

    await Promise.all([
      this.pubClient?.quit().catch(() => 'OK'),
      this.subClient?.quit().catch(() => 'OK'),
    ]);

    this.pubClient = undefined;
    this.subClient = undefined;
    this.listener = undefined;
  }
}
