import { SetMetadata } from '@nestjs/common';
import type { ChannelDefinition, ChannelAuthorizeFn } from '../types/sse.types';

/**
 * Metadata key for channel definitions declared on providers.
 */
export const SSE_CHANNELS_METADATA = 'sse:channels';

/**
 * Declare channel definitions on a provider class.
 */
export function SseChannels(definitions: ChannelDefinition[]): ClassDecorator {
  return SetMetadata(SSE_CHANNELS_METADATA, definitions);
}

/**
 * Helper for declaring a public channel definition.
 */
export function publicChannel(pattern: string): ChannelDefinition {
  return {
    pattern,
    audience: 'public',
  };
}

/**
 * Helper for declaring an authenticated channel definition.
 */
export function authenticatedChannel(
  pattern: string,
  authorize?: ChannelAuthorizeFn,
): ChannelDefinition {
  return {
    pattern,
    audience: 'authenticated',
    authorize,
  };
}
