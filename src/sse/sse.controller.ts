import {
  BadRequestException,
  Body,
  Controller,
  ForbiddenException,
  Get,
  Headers,
  HttpCode,
  NotFoundException,
  Post,
  Query,
  Req,
  Res,
} from '@nestjs/common';
import type { Request, Response } from 'express';
import { SseService } from './sse.service';

/**
 * Payload for subscribe and unsubscribe endpoints.
 */
interface SubscribeBody {
  clientId: string;
  topic: string;
  metadata?: Record<string, unknown>;
}

/**
 * Payload for topic broadcast endpoint.
 */
interface BroadcastBody {
  topic: string;
  event: string;
  data: unknown;
  target?: 'all' | 'public' | 'authenticated';
}

/**
 * Payload for iterable piping endpoint.
 */
interface PipeBody {
  clientId: string;
  items: unknown[];
  event?: string;
}

@Controller('sse')
/**
 * HTTP API for opening streams and managing SSE operations.
 */
export class SseController {
  constructor(private readonly sse: SseService) {}

  @Get('events')
  /**
   * Open a public (unauthenticated) SSE stream.
   */
  openPublicStream(
    @Query('clientId') clientId: string,
    @Req() req: Request,
    @Res() res: Response,
  ): void {
    if (!clientId) {
      throw new BadRequestException('Missing clientId');
    }

    this.sse.openStream({
      clientId,
      request: req,
      response: res,
    });
  }

  @Post('subscribe')
  @HttpCode(204)
  /**
   * Subscribe a client to a topic.
   */
  subscribe(
    @Body() body: SubscribeBody,
    @Headers('x-user-id') userId?: string,
  ): void {
    if (!body.clientId || !body.topic) {
      throw new BadRequestException('clientId and topic are required');
    }

    const metadata = userId
      ? {
          ...(body.metadata ?? {}),
          userId,
        }
      : body.metadata;

    const result = this.sse.subscribe({
      clientId: body.clientId,
      topic: body.topic,
      metadata,
    });
    if (!result.ok) {
      if (result.reason === 'unauthorized') {
        throw new ForbiddenException('Forbidden channel subscription');
      }
      throw new BadRequestException('Subscription failed');
    }
  }

  @Post('unsubscribe')
  @HttpCode(204)
  /**
   * Unsubscribe a client from a topic.
   */
  unsubscribe(@Body() body: SubscribeBody): void {
    if (!body.clientId || !body.topic) {
      throw new BadRequestException('clientId and topic are required');
    }

    const ok = this.sse.unsubscribe(body);
    if (!ok) {
      throw new BadRequestException('Unsubscribe failed');
    }
  }

  @Post('broadcast')
  /**
   * Broadcast an event payload to topic subscribers.
   */
  async broadcast(@Body() body: BroadcastBody): Promise<{ delivered: number }> {
    if (!body.topic || !body.event) {
      throw new BadRequestException('topic and event are required');
    }

    const delivered = await this.sse.broadcast(
      body.topic,
      {
        event: body.event,
        data: body.data,
      },
      {
        target: body.target,
      },
    );

    return { delivered };
  }

  @Post('cancel')
  @HttpCode(204)
  /**
   * Cancel and close a client stream.
   */
  cancel(@Body() body: { clientId: string; reason?: string }): void {
    if (!body.clientId) {
      throw new BadRequestException('clientId is required');
    }

    const ok = this.sse.cancelStream(body.clientId, body.reason);
    if (!ok) {
      throw new NotFoundException('Connection not found');
    }
  }

  @Post('pipe/iterable')
  /**
   * Push iterable items to a client stream as SSE events.
   */
  async pipeIterable(@Body() body: PipeBody): Promise<{ sent: number }> {
    if (!body.clientId || !Array.isArray(body.items)) {
      throw new BadRequestException('clientId and items[] are required');
    }

    const sent = await this.sse.pipeIterable({
      clientId: body.clientId,
      iterable: body.items,
      event: body.event,
    });

    return { sent };
  }

  @Get('connections')
  /**
   * Return active connection snapshots.
   */
  stats(): { count: number; items: unknown[] } {
    const items = this.sse.connectionSnapshots();
    return {
      count: items.length,
      items,
    };
  }
}
