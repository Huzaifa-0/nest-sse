import { Injectable } from '@nestjs/common';
import type {
  ChannelAudience,
  ChannelAuthorizationContext,
  ChannelAuthorizationResult,
  ChannelAuthorizeFn,
  ChannelDefinition,
  ChannelRegistry,
  ChannelParams,
  ResolvedChannel,
} from '../types/sse.types';

interface CompiledChannelDefinition {
  definition: ChannelDefinition;
  regex: RegExp;
  paramNames: string[];
  staticSegments: number;
}

interface ResolvedMatch {
  compiled: CompiledChannelDefinition;
  params: ChannelParams;
}

@Injectable()
/**
 * @description Registry for channel definitions and authorization.
 */
export class ChannelRegistryService implements ChannelRegistry {
  private readonly definitions: CompiledChannelDefinition[] = [];

  constructor(initialDefinitions: ChannelDefinition[] = []) {
    for (const definition of initialDefinitions) {
      this.register(definition);
    }
  }

  /**
   * @description Registers a generic channel definition.
   * @param definition Channel definition to compile and store.
   * @returns Nothing.
   */
  register(definition: ChannelDefinition): void {
    const audience: ChannelAudience = definition.audience ?? 'public';
    const normalized: ChannelDefinition = {
      ...definition,
      audience,
    };

    this.definitions.push(this.compileDefinition(normalized));
  }

  /**
   * @description Registers a public channel pattern.
   * @param pattern Dot-separated channel pattern.
   * @returns Nothing.
   */
  registerPublic(pattern: string): void {
    this.register({ pattern, audience: 'public' });
  }

  /**
   * @description Registers an authenticated channel pattern and optional authorization callback.
   * @param pattern Dot-separated channel pattern.
   * @param authorize Optional callback used to authorize subscriptions.
   * @returns Nothing.
   */
  registerAuthenticated(pattern: string, authorize?: ChannelAuthorizeFn): void {
    this.register({ pattern, audience: 'authenticated', authorize });
  }

  /**
   * @description Resolves a topic to the most specific matching channel definition.
   * @param topic Topic name to match against registered patterns.
   * @returns The resolved channel with extracted params, or null when unmatched.
   */
  resolve(topic: string): ResolvedChannel | null {
    const matches: ResolvedMatch[] = [];

    for (const compiled of this.definitions) {
      const matched = topic.match(compiled.regex);
      if (!matched) {
        continue;
      }

      const params: ChannelParams = {};
      compiled.paramNames.forEach((name, idx) => {
        params[name] = matched[idx + 1] ?? '';
      });

      matches.push({ compiled, params });
    }

    if (matches.length === 0) {
      return null;
    }

    matches.sort((a, b) => {
      if (a.compiled.staticSegments !== b.compiled.staticSegments) {
        return b.compiled.staticSegments - a.compiled.staticSegments;
      }

      return a.compiled.paramNames.length - b.compiled.paramNames.length;
    });

    const winner = matches[0];
    return {
      definition: winner.compiled.definition,
      params: winner.params,
    };
  }

  /**
   * @description Authorizes a previously resolved channel for the provided request context.
   * @param resolved Channel resolution result.
   * @param context Authorization context for the request.
   * @returns Structured authorization result including optional metadata and reason.
   */
  authorize(
    resolved: ResolvedChannel,
    context: ChannelAuthorizationContext,
  ): ChannelAuthorizationResult {
    if (resolved.definition.audience === 'authenticated' && !context.requestMetadata?.userId) {
      return { allowed: false, reason: 'authenticated-channel-required' };
    }

    if (!resolved.definition.authorize) {
      return { allowed: true };
    }

    const response = resolved.definition.authorize(context);
    if (typeof response === 'boolean') {
      return { allowed: response };
    }

    return {
      allowed: response.allowed,
      reason: response.reason,
      metadata: response.metadata,
    };
  }

  /**
   * @description Returns all registered channel definitions.
   * @returns A shallow copy of registered channel definition entries.
   */
  all(): ChannelDefinition[] {
    return this.definitions.map((entry) => entry.definition);
  }

  private compileDefinition(definition: ChannelDefinition): CompiledChannelDefinition {
    const paramNames: string[] = [];
    const segments = definition.pattern.split('.');
    let staticSegments = 0;

    const regexSegments = segments.map((segment) => {
      const match = segment.match(/^\{([a-zA-Z0-9_]+)\}$/);
      if (match) {
        paramNames.push(match[1]);
        return '([^\\.]+)';
      }

      staticSegments += 1;
      return this.escapeRegex(segment);
    });

    return {
      definition,
      regex: new RegExp(`^${regexSegments.join('\\.')}$`),
      paramNames,
      staticSegments,
    };
  }

  private escapeRegex(input: string): string {
    return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}
