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
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  getHello(): { service: string; connections: number } {
    return this.appService.getStatus();
  }

  @Get('demo/connect/public')
  connectPublic(
    @Query('clientId') clientId: string,
    @Req() req: Request,
    @Res() res: Response,
  ): void {
    this.appService.connectPublic({
      clientId: this.appService.ensureClientId(clientId),
      request: req,
      response: res,
    });
  }

  @Get('demo/connect/auth')
  connectAuth(
    @Query('clientId') clientId: string,
    @Headers('x-user-id') userId: string,
    @Req() req: Request,
    @Res() res: Response,
  ): void {
    if (!userId) {
      throw new BadRequestException('Missing x-user-id header');
    }

    this.appService.connectAuthenticated({
      clientId: this.appService.ensureClientId(clientId),
      userId,
      request: req,
      response: res,
    });
  }

  @Post('demo/subscribe')
  @HttpCode(200)
  subscribe(
    @Body() body: { clientId: string; channel: string; metadata?: Record<string, unknown> },
  ): { ok: boolean } {
    if (!body.clientId || !body.channel) {
      throw new BadRequestException('clientId and channel are required');
    }

    const result = this.appService.subscribe(body);
    if (!result.ok) {
      if (result.reason === 'unauthorized') {
        throw new ForbiddenException('Forbidden channel subscription');
      }

      throw new BadRequestException(`Subscription failed: ${result.reason ?? 'unknown'}`);
    }

    return { ok: true };
  }

  @Post('demo/unsubscribe')
  @HttpCode(200)
  unsubscribe(@Body() body: { clientId: string; channel: string }): { ok: boolean } {
    if (!body.clientId || !body.channel) {
      throw new BadRequestException('clientId and channel are required');
    }

    const ok = this.appService.unsubscribe(body);
    if (!ok) {
      throw new NotFoundException('Subscription not found');
    }

    return { ok };
  }

  @Post('demo/channels/register/public')
  registerPublicChannel(@Body() body: { pattern: string }): { ok: boolean; pattern: string } {
    if (!body.pattern) {
      throw new BadRequestException('pattern is required');
    }

    this.appService.registerPublicChannel(body);
    return {
      ok: true,
      pattern: body.pattern,
    };
  }

  @Post('demo/channels/register/auth')
  registerAuthChannel(@Body() body: { pattern: string }): { ok: boolean; pattern: string } {
    if (!body.pattern) {
      throw new BadRequestException('pattern is required');
    }

    this.appService.registerAuthenticatedChannel(body);
    return {
      ok: true,
      pattern: body.pattern,
    };
  }

  @Post('demo/batch')
  async emitBatch(
    @Body()
    body: {
      channel: string;
      event?: string;
      count?: number;
      payload?: Record<string, unknown>;
    },
  ): Promise<{ totalEvents: number; delivered: number }> {
    if (!body.channel) {
      throw new BadRequestException('channel is required');
    }

    return this.appService.emitBatch(body);
  }

  @Post('demo/emit/iterable')
  async emitIterable(
    @Body()
    body: {
      clientId: string;
      items: unknown[];
      event?: string;
      intervalMs?: number;
      cancelAfterMs?: number;
    },
  ): Promise<{ sent: number; cancelled: boolean }> {
    if (!body.clientId || !Array.isArray(body.items)) {
      throw new BadRequestException('clientId and items[] are required');
    }

    return this.appService.emitIterable(body);
  }

  @Post('demo/emit/readable')
  async emitReadable(
    @Body()
    body: {
      clientId: string;
      chunks: string[];
      event?: string;
      cancelAfterMs?: number;
    },
  ): Promise<{ sent: number; cancelled: boolean }> {
    if (!body.clientId || !Array.isArray(body.chunks)) {
      throw new BadRequestException('clientId and chunks[] are required');
    }

    return this.appService.emitReadable(body);
  }

  @Post('demo/stream/cancellable')
  async streamCancellable(
    @Body()
    body: {
      clientId: string;
      channel?: string;
      total?: number;
      intervalMs?: number;
      failAt?: number;
      cancelAfterMs?: number;
      cancelOnError?: boolean;
    },
  ): Promise<{
    sent: number;
    abortedBySignal: boolean;
    cancelledByEvent: boolean;
  }> {
    if (!body.clientId) {
      throw new BadRequestException('clientId is required');
    }

    return this.appService.streamAndCancelOnSignalOrEvent(body);
  }

  @Post('demo/broadcast/public')
  async demoPublicBroadcast(
    @Body() body: { channel: string; message: string },
  ): Promise<{ delivered: number }> {
    const result = await this.appService.emitBatch({
      channel: body.channel,
      count: 1,
      event: 'demo.public',
      payload: {
        message: body.message,
      },
    });

    return { delivered: result.delivered };
  }

  @Post('demo/broadcast/auth')
  async demoAuthBroadcast(
    @Body() body: { channel: string; message: string },
  ): Promise<{ delivered: number }> {
    const result = await this.appService.emitBatch({
      channel: body.channel,
      count: 1,
      event: 'demo.auth',
      payload: {
        message: body.message,
      },
    });

    return { delivered: result.delivered };
  }

  @Post('demo/pipe/readable')
  async demoPipeReadable(
    @Body() body: { clientId: string; chunks: string[] },
  ): Promise<{ sent: number }> {
    const result = await this.appService.emitReadable({
      clientId: body.clientId,
      chunks: body.chunks,
      event: 'demo.chunk',
    });

    return { sent: result.sent };
  }
}
