import type { SseEventEnvelope } from '../types/sse.types';

/**
 * Message envelope exchanged by transport drivers.
 */
export interface TransportMessage {
  channel: string;
  envelope: SseEventEnvelope;
  target: 'all' | 'public' | 'authenticated';
}

/**
 * Transport contract used to fan-out broadcasts across processes.
 */
export interface SseTransportAdapter {
  /**
   * Publish a channel message to the transport backend.
   */
  publish(message: TransportMessage): Promise<void>;

  /**
   * Subscribe to incoming transport messages.
   */
  subscribe(handler: (message: TransportMessage) => void): Promise<void>;

  /**
   * Release transport resources and active subscriptions.
   */
  close(): Promise<void>;
}
