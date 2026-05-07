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
import { SseService } from '../services/sse.service';

/**
 * Payload for subscribe and unsubscribe endpoints.
 */
interface SubscribeBody {
  clientId: string;
  channel: string;
  metadata?: Record<string, unknown>;
}

/**
 * Payload for channel broadcast endpoint.
 */
interface BroadcastBody {
  channel: string;
  event: string;
  data: unknown;
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

  /**
   * Open a public (unauthenticated) SSE stream.
   */
  @Get('events')
  openStream(
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

  /**
   * Subscribe a client to a channel.
   */
  @Post('subscribe')
  @HttpCode(204)
  subscribe(
    @Body() body: SubscribeBody,
    @Headers('x-user-id') userId?: string,
  ): void {
    if (!body.clientId || !body.channel) {
      throw new BadRequestException('clientId and channel are required');
    }

    const metadata = userId
      ? {
          ...(body.metadata ?? {}),
          userId,
        }
      : body.metadata;

    const result = this.sse.subscribe({
      clientId: body.clientId,
      channel: body.channel,
      metadata,
    });
    if (!result.ok) {
      if (result.reason === 'unauthorized') {
        throw new ForbiddenException('Forbidden channel subscription');
      }
      throw new BadRequestException('Subscription failed');
    }
  }

  /**
   * Unsubscribe a client from a channel.
   */
  @Post('unsubscribe')
  @HttpCode(204)
  unsubscribe(@Body() body: SubscribeBody): void {
    if (!body.clientId || !body.channel) {
      throw new BadRequestException('clientId and channel are required');
    }

    const ok = this.sse.unsubscribe(body);
    if (!ok) {
      throw new BadRequestException('Unsubscribe failed');
    }
  }
  
  /**
   * Broadcast an event payload to channel subscribers.
   */
  @Post('broadcast')
  async broadcast(@Body() body: BroadcastBody): Promise<{ delivered: number }> {
    if (!body.channel || !body.event) {
      throw new BadRequestException('channel and event are required');
    }

    const delivered = await this.sse.broadcast(
      body.channel,
      {
        event: body.event,
        data: body.data,
      },
    );

    return { delivered };
  }

  /**
   * Cancel and close a client stream.
   */
  @Post('cancel')
  @HttpCode(204)
  cancel(@Body("clientId") clientId: string): void {
    if (!clientId) {
      throw new BadRequestException('clientId is required');
    }

    const ok = this.sse.cancelStream(clientId);
    if (!ok) {
      throw new NotFoundException('Connection not found');
    }
  }

  /**
   * Push iterable items to a client stream as SSE events.
   */
  @Post('pipe/iterable')
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

  /**
   * Return active connection snapshots.
   */
  @Get('connections')
  stats(): { count: number; items: unknown[] } {
    const items = this.sse.connectionSnapshots();
    return {
      count: items.length,
      items,
    };
  }
}
