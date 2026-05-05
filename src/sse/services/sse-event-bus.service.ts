import { Injectable } from '@nestjs/common';
import { EventEmitter } from 'node:events';
import type { SseLifecycleEvents } from './sse.types';

type EventEmitterHandler = (...args: unknown[]) => void;

@Injectable()
/**
 * @description Typed in-memory event bus for SSE lifecycle hooks.
 */
export class SseEventBusService {
  private readonly emitter = new EventEmitter();

  /**
   * @description Emits a typed lifecycle event.
   * @param event Lifecycle event name.
   * @param payload Event payload for the selected event.
   * @returns True when at least one listener handled the event.
   */
  emit<K extends keyof SseLifecycleEvents>(
    event: K,
    payload: SseLifecycleEvents[K],
  ): boolean {
    return this.emitter.emit(event, payload);
  }

  /**
   * @description Registers a lifecycle event handler and returns an unsubscribe function.
   * @param event Lifecycle event name.
   * @param handler Typed event handler callback.
   * @returns Function that removes the registered listener.
   */
  on<K extends keyof SseLifecycleEvents>(
    event: K,
    handler: (payload: SseLifecycleEvents[K]) => void,
  ): () => void {
    const eventHandler = handler as EventEmitterHandler;
    this.emitter.on(event, eventHandler);
    return () => this.emitter.off(event, eventHandler);
  }
}
