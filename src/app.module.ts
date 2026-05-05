import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { SseModule } from './sse';

@Module({
  imports: [
    SseModule.forRoot({
      maxConnections: 5000,
      maxTopicsPerConnection: 64,
      heartbeatIntervalMs: 12_000,
      deadConnectionMs: 40_000,
      bufferMaxEvents: 8,
      bufferFlushMs: 150,
      channels: [
        {
          pattern: 'demo.public',
          audience: 'public',
        },
        {
          pattern: 'demo.private.{userId}',
          audience: 'authenticated',
          authorize: ({ params, requestMetadata }) => {
            const userId = String(requestMetadata.userId ?? '');
            return userId === params.userId;
          },
        },
      ],
    }),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
