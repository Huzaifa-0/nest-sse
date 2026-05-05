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
  topics: Set<string>;
  createdAt: number;
  lastSeenAt: number;
  bufferedEvents: SseEventEnvelope[];
  isClosed: boolean;
  flushTimer?: NodeJS.Timeout;
}

@Injectable()
/**
 * @description Tracks active connections and topic membership.
 */
export class ConnectionPoolService {
  private readonly connections = new Map<string, ConnectionRecord>();
  private readonly topicMap = new Map<string, Set<string>>();

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

    for (const topic of connection.topics) {
      this.unsubscribe(clientId, topic);
    }

    if (connection.flushTimer) {
      clearTimeout(connection.flushTimer);
    }

    this.connections.delete(clientId);
  }

  /**
   * @description Adds a topic subscription for a client.
   * @param clientId Unique client identifier.
   * @param topic Topic name to subscribe to.
   * @returns True when the client exists and the subscription is applied.
   */
  subscribe(clientId: string, topic: string): boolean {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return false;
    }

    connection.topics.add(topic);
    const clients = this.topicMap.get(topic) ?? new Set<string>();
    clients.add(clientId);
    this.topicMap.set(topic, clients);

    return true;
  }

  /**
   * @description Removes a topic subscription for a client.
   * @param clientId Unique client identifier.
   * @param topic Topic name to unsubscribe from.
   * @returns True when the client exists.
   */
  unsubscribe(clientId: string, topic: string): boolean {
    const connection = this.connections.get(clientId);
    if (!connection) {
      return false;
    }

    connection.topics.delete(topic);
    const clients = this.topicMap.get(topic);

    if (clients) {
      clients.delete(clientId);
      if (clients.size === 0) {
        this.topicMap.delete(topic);
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
   * @description Returns active subscriber connection records for a topic.
   * @param topic Topic name to look up.
   * @returns Array of active subscriber connection records.
   */
  forTopic(topic: string): ConnectionRecord[] {
    const clientIds = this.topicMap.get(topic);
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
   * @description Returns the number of subscribers for a topic.
   * @param topic Topic name to look up.
   * @returns Subscriber count for the topic.
   */
  topicCount(topic: string): number {
    return this.topicMap.get(topic)?.size ?? 0;
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
      topics: [...connection.topics],
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
