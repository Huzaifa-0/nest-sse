/**
 * Dependency injection token for resolved SSE module options.
 */
export const SSE_OPTIONS = Symbol('SSE_OPTIONS');

/**
 * Dependency injection token for the configured transport adapter.
 */
export const SSE_TRANSPORT = Symbol('SSE_TRANSPORT');

/**
 * Dependency injection token for channel registry.
 */
export const SSE_CHANNEL_REGISTRY = Symbol('SSE_CHANNEL_REGISTRY');

/**
 * Default heartbeat event name.
 */
export const DEFAULT_HEARTBEAT_EVENT = 'heartbeat';

/**
 * Default stream cancellation event name.
 */
export const DEFAULT_CANCEL_EVENT = 'stream.cancel';

/**
 * Default batched event wrapper name.
 */
export const DEFAULT_BATCH_EVENT = 'events.batch';
