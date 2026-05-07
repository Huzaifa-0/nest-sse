import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { authenticatedChannel, publicChannel, SseModule } from '../sse';

@Module({
  imports: [
    SseModule.forRoot({
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
        publicChannel('feed'),
        authenticatedChannel('orders.{orderId}', ({ params, requestMetadata }) => {
          return {
            allowed: String(requestMetadata.orderId ?? '') === params.orderId,
            reason: 'order-mismatch',
          };
        }),
      ],
  }),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
