import { Injectable } from '@nestjs/common';
import type { ServerResponse } from 'node:http';
import type { ConnectionSnapshot, SseEventEnvelope } from '../types/sse.types';

/**
 * Runtime state associated with a connected SSE client.
 */
export interface ConnectionRecord {
  clientId: string;
  response: ServerResponse;
  metadata: Record<string, unknown>;
  channels: Set<string>;
  createdAt: number;
  lastSeenAt: number;
  bufferedEvents: SseEventEnvelope[];
  isClosed: boolean;
  flushTimer?: NodeJS.Timeout;
}

@Injectable()
/**
 * @description Tracks active connections and channel membership.
 */
export class ConnectionPoolService {
  private readonly connections = new Map<string, ConnectionRecord>();
  private readonly channelMap = new Map<string, Set<string>>();

  /**
   * @description Registers a new connection record.
   * @param record Connection record to store.
   * @returns Nothing.
   */
  create(record: ConnectionRecord): void {
    this.connections.set(record.clientId, record);
  }

  /**
   * @description Retrieves a connection by client id.
   * @param clientId Unique client identifier.
   * @returns The matching connection record when present.
   */
  get(clientId: string): ConnectionRecord | undefined {
    return this.connections.get(clientId);
  }

  /**
   * @description Checks whether a connection exists.
   * @param clientId Unique client identifier.
   * @returns True when the connection is currently tracked.
   */
  has(clientId: string): boolean {
    return this.connections.has(clientId);
  }

  /**
   * @description Removes a connection and cleans up subscriptions and timers.
   * @param clientId Unique client identifier.
   * @returns Nothing.
   */
  remove(clientId: string): void {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return;
    }

    for (const channel of connection.channels) {
      this.unsubscribe(clientId, channel);
    }

    if (connection.flushTimer) {
      clearTimeout(connection.flushTimer);
    }

    this.connections.delete(clientId);
  }

  /**
   * @description Adds a channel subscription for a client.
   * @param clientId Unique client identifier.
   * @param channel Channel name to subscribe to.
   * @returns True when the client exists and the subscription is applied.
   */
  subscribe(clientId: string, channel: string): boolean {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return false;
    }

    connection.channels.add(channel);
    const clients = this.channelMap.get(channel) ?? new Set<string>();
    clients.add(clientId);
    this.channelMap.set(channel, clients);

    return true;
  }

  /**
   * @description Removes a channel subscription for a client.
   * @param clientId Unique client identifier.
   * @param channel Channel name to unsubscribe from.
   * @returns True when the client exists.
   */
  unsubscribe(clientId: string, channel: string): boolean {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return false;
    }

    connection.channels.delete(channel);
    const clients = this.channelMap.get(channel);

    if (clients) {
      clients.delete(clientId);
      if (clients.size === 0) {
        this.channelMap.delete(channel);
      }
    }

    return true;
  }

  /**
   * @description Merges metadata into a connection record.
   * @param clientId Unique client identifier.
   * @param patch Metadata key-value patch to merge.
   * @returns True when the connection exists.
   */
  setMetadata(clientId: string, patch: Record<string, unknown>): boolean {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return false;
    }

    connection.metadata = {
      ...connection.metadata,
      ...patch,
    };
    return true;
  }

  /**
   * @description Updates the last activity timestamp for a connection.
   * @param clientId Unique client identifier.
   * @returns Nothing.
   */
  updateLastSeen(clientId: string): void {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return;
    }

    connection.lastSeenAt = Date.now();
  }

  /**
   * @description Returns all active connection records.
   * @returns Array of active connection records.
   */
  all(): ConnectionRecord[] {
    return [...this.connections.values()];
  }

  /**
   * @description Returns active subscriber connection records for a channel.
   * @param channel Channel name to look up.
   * @returns Array of active subscriber connection records.
   */
  forChannel(channel: string): ConnectionRecord[] {
    const clientIds = this.channelMap.get(channel);
    if (!clientIds) {
      return [];
    }

    const list: ConnectionRecord[] = [];
    for (const clientId of clientIds) {
      const connection = this.connections.get(clientId);
      if (connection) {
        list.push(connection);
      }
    }

    return list;
  }

  /**
   * @description Returns the current active connection count.
   * @returns Number of tracked active connections.
   */
  count(): number {
    return this.connections.size;
  }

  /**
   * @description Returns the number of subscribers for a channel.
   * @param channel Channel name to look up.
   * @returns Subscriber count for the channel.
   */
  channelCount(channel: string): number {
    return this.channelMap.get(channel)?.size ?? 0;
  }

  /**
   * @description Builds a serializable snapshot for one connection.
   * @param clientId Unique client identifier.
   * @returns Connection snapshot or null when not found.
   */
  snapshot(clientId: string): ConnectionSnapshot | null {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return null;
    }

    return {
      clientId: connection.clientId,
      channels: [...connection.channels],
      metadata: { ...connection.metadata },
      createdAt: connection.createdAt,
      lastSeenAt: connection.lastSeenAt,
      bufferedEvents: connection.bufferedEvents.length,
    };
  }

  /**
   * @description Builds serializable snapshots for all active connections.
   * @returns Snapshot array for every active connection.
   */
  snapshots(): ConnectionSnapshot[] {
    return [...this.connections.keys()]
      .map((id) => this.snapshot(id))
      .filter((entry): entry is ConnectionSnapshot => !!entry);
  }
}
