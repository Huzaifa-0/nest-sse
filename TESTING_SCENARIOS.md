# SSE Testing Scenarios

This document is a practical test plan for validating SSE behavior end-to-end and at unit level.

## Scope

- Channel registry resolution and authorization
- Stream lifecycle (connect, replace, cancel, abort, timeout)
- Subscribe and unsubscribe behavior
- Public vs authenticated channels
- Event triggering and delivery filters
- Event buffering and timer handling
- Redis transport fan-out and cleanup

## Test Strategy

- Unit tests: Pure service and transport logic
- Integration tests: Controller plus service with real Nest module wiring
- End-to-end tests: Real HTTP clients (EventSource or curl) and optionally two app instances for Redis

## Environment Matrix

Run the same critical scenarios against these profiles:

- Profile A: single app instance (no transport configured)
- Profile B: redis transport, single app instance
- Profile C: redis transport, two app instances sharing one Redis channel

Recommended config variations:

- bufferMaxEvents = 1 (immediate writes)
- bufferMaxEvents > 1 with short bufferFlushMs (batch mode)
- short heartbeatIntervalMs and deadConnectionMs for timeout tests

## Scenarios

## 1. Channel Registry

### CR-001 Register public channel
- Type: Unit
- Setup: Register pattern demo.public with public audience
- Steps: Resolve channel demo.public
- Expected: Match found, params empty, audience is public

### CR-002 Register authenticated channel with params
- Type: Unit
- Setup: Register orders.{orderId} as authenticated channel
- Steps: Resolve channel orders.123
- Expected: Match found with params.orderId = 123

### CR-003 No match for unknown channel
- Type: Unit
- Setup: Register demo.public only
- Steps: Resolve channel demo.unknown
- Expected: Resolve returns null

### CR-004 Most specific match wins
- Type: Unit
- Setup: Register demo.{id} and demo.static
- Steps: Resolve channel demo.static
- Expected: demo.static wins over demo.{id}

### CR-005 Fewer params wins on tie
- Type: Unit
- Setup: Register a.{x}.c and a.b.c
- Steps: Resolve channel a.b.c
- Expected: a.b.c wins because it has more static segments and fewer params

### CR-006 Auth gate denies unauthenticated connection
- Type: Unit
- Setup: Resolved channel is authenticated, context.requestMetadata.userId is missing
- Steps: Call authorize
- Expected: allowed = false, reason includes authenticated-channel-required

### CR-007 Authorize callback boolean response
- Type: Unit
- Setup: Channel authorize returns true and false in separate tests
- Steps: Call authorize
- Expected: allowed mirrors boolean

### CR-008 Authorize callback object response
- Type: Unit
- Setup: Authorize returns { allowed, reason, metadata }
- Steps: Call authorize
- Expected: Result preserves allowed, reason, metadata

## 2. Connect and Cancel Stream

### SC-001 Open public stream success
- Type: Integration
- Setup: Call GET /sse/events with clientId
- Steps: Observe response headers and first event
- Expected: text/event-stream headers set, connection.ready emitted

### SC-002 Open authenticated stream success
- Type: Integration
- Setup: Call GET /demo/connect/auth with clientId and x-user-id
- Steps: Inspect connection snapshot
- Expected: authenticated true and metadata includes userId

### SC-003 Open auth stream without header
- Type: Integration
- Setup: Missing x-user-id
- Steps: Call GET /demo/connect/auth
- Expected: 400 bad request

### SC-004 Max connection limit
- Type: Integration
- Setup: maxConnections = 1, keep first stream open
- Steps: Open second stream
- Expected: Second request is rejected with 429

### SC-005 Replace existing clientId connection
- Type: Integration
- Setup: Open stream for same clientId twice
- Steps: Open second stream with same clientId
- Expected: First connection closed with replaced reason, second stays active

### SC-006 Cancel stream endpoint
- Type: Integration
- Setup: Open stream
- Steps: POST /sse/cancel with clientId
- Expected: stream.cancel event emitted then stream closes and connection removed

### SC-007 Request abort closes stream
- Type: Integration
- Setup: Open stream and close client socket
- Steps: Simulate aborted/close from request
- Expected: service emits stream.aborted and removes connection

### SC-008 Wait for close helper
- Type: Unit
- Setup: Create mock request emitter
- Steps: Call waitForClose and emit close
- Expected: Promise resolves exactly once

## 3. Subscribe and Unsubscribe

### SU-001 Subscribe success for active connection
- Type: Integration
- Setup: Open stream then POST /sse/subscribe with channel
- Steps: Subscribe once
- Expected: 204 and channelCount(channel) increments

### SU-002 Subscribe fails when connection not found
- Type: Integration
- Setup: No open stream
- Steps: POST /sse/subscribe
- Expected: 400 with Subscription failed

### SU-003 Subscribe fails when connection closed
- Type: Unit
- Setup: Create and close connection
- Steps: Call subscribe
- Expected: ok false, reason connection-closed

### SU-004 Max channels per connection
- Type: Integration
- Setup: maxChannelsPerConnection = 1
- Steps: Subscribe to channel A then channel B
- Expected: second subscribe rejected with max-channels-reached

### SU-005 Duplicate subscribe idempotency
- Type: Unit
- Setup: Subscribe same client/channel twice
- Steps: Subscribe twice
- Expected: Both calls succeed, channel membership stored once

### SU-006 Unsubscribe success
- Type: Integration
- Setup: Active subscription
- Steps: POST /sse/unsubscribe
- Expected: 204 and channel removed

### SU-007 Unsubscribe missing connection
- Type: Integration
- Setup: Unknown clientId
- Steps: POST /sse/unsubscribe
- Expected: 400 Unsubscribe failed

### SU-008 Metadata merge during subscribe
- Type: Unit
- Setup: Connection metadata has userId, subscribe metadata has app
- Steps: subscribe with metadata
- Expected: Connection metadata contains both keys and emits metadata-updated event

## 4. Public vs Auth Channel Rules

### PA-001 Public channel accepts public connection
- Type: Integration
- Setup: Public stream, public channel
- Steps: Subscribe
- Expected: Allowed

### PA-002 Public channel accepts authenticated connection
- Type: Integration
- Setup: Auth stream, public channel
- Steps: Subscribe
- Expected: Allowed

### PA-003 Auth channel rejects public connection
- Type: Integration
- Setup: Public stream, authenticated channel
- Steps: Subscribe
- Expected: Controller returns 403

### PA-004 Auth channel accepts authenticated connection
- Type: Integration
- Setup: Auth stream, authenticated channel
- Steps: Subscribe
- Expected: Allowed

### PA-005 Param authorization allows matching identity
- Type: Integration
- Setup: Channel orders.{orderId}, connection metadata has orderId=123
- Steps: Subscribe to orders.123 and orders.999
- Expected: 123 allowed, 999 denied

### PA-006 Request metadata available in authorize context
- Type: Unit
- Setup: Authorize function checks requestMetadata
- Steps: Subscribe with metadata payload
- Expected: Callback receives request metadata and can decide policy

## 5. Trigger Event and Delivery

### EV-001 Broadcast to subscribed channel
- Type: Integration
- Setup: One subscriber on channel
- Steps: POST /sse/broadcast
- Expected: delivered count is 1 and client receives event

### EV-002 Broadcast to channel with no subscribers
- Type: Integration
- Setup: No subscribers
- Steps: Broadcast
- Expected: delivered count is 0

### EV-003 Target filter all
- Type: Integration
- Setup: One public and one authenticated subscriber on same channel
- Steps: Broadcast target all
- Expected: Both receive event

### EV-004 Target filter public
- Type: Integration
- Setup: Same as EV-003
- Steps: Broadcast target public
- Expected: Only public receives event

### EV-005 Target filter authenticated
- Type: Integration
- Setup: Same as EV-003
- Steps: Broadcast target authenticated
- Expected: Only authenticated receives event

### EV-006 Event id and timestamp auto-fill
- Type: Unit
- Setup: Broadcast envelope without id or timestamp
- Steps: Call broadcast
- Expected: Serialized event has generated id and timestamp

### EV-007 Cross-instance delivery hook
- Type: Integration
- Setup: Use transport subscribe callback path
- Steps: Publish transport message
- Expected: Message is delivered through local subscriber matching logic

## 6. Event Buffer and Timer Handling

### BT-001 Immediate mode when bufferMaxEvents <= 1
- Type: Unit
- Setup: bufferMaxEvents = 1
- Steps: Emit one event
- Expected: response.write called immediately

### BT-002 Batch flush on size threshold
- Type: Unit
- Setup: bufferMaxEvents = 3
- Steps: Emit three events quickly
- Expected: One batch wrapper event written with three payload entries

### BT-003 Batch flush on timer threshold
- Type: Unit
- Setup: bufferMaxEvents = 5, short bufferFlushMs
- Steps: Emit one event and advance timer
- Expected: Flush occurs after timer and buffer is cleared

### BT-004 Flush timer is cleared on connection removal
- Type: Unit
- Setup: Buffered events pending flush timer
- Steps: Close connection before timer fires
- Expected: Timer cleared, no further writes attempted

### BT-005 Heartbeat emits periodically
- Type: Unit
- Setup: heartbeatIntervalMs short, live connection
- Steps: Advance fake timers
- Expected: heartbeat events are emitted and written

### BT-006 Dead connection timeout
- Type: Unit
- Setup: deadConnectionMs small, stale lastSeenAt
- Steps: Advance fake timers beyond threshold
- Expected: connection closed with heartbeat-timeout reason

### BT-007 Last-seen update behavior
- Type: Unit
- Setup: Mix heartbeat and non-heartbeat events
- Steps: Flush single and batched events
- Expected: lastSeenAt updates for non-heartbeat traffic per service logic

## 7. Redis Transport

### RT-001 Missing ioredis dependency failure
- Type: Unit
- Setup: Environment without ioredis
- Steps: Call publish or subscribe on Redis transport
- Expected: Throw explicit install hint error

### RT-002 Publish serializes message to JSON
- Type: Unit
- Setup: Mock redis publish
- Steps: Call publish with transport message
- Expected: publish called with configured channel and JSON string

### RT-003 Subscribe parses valid JSON message
- Type: Unit
- Setup: Mock message event with valid payload
- Steps: Trigger message callback
- Expected: Handler called with parsed TransportMessage

### RT-004 Subscribe ignores malformed JSON message
- Type: Unit
- Setup: Mock message event with invalid JSON
- Steps: Trigger message callback
- Expected: No throw and handler not called

### RT-005 Close detaches listener and quits clients
- Type: Unit
- Setup: Initialized pub and sub clients
- Steps: Call close
- Expected: message listener removed and both clients quit

### RT-006 Cross-instance fan-out (two app instances)
- Type: End-to-end
- Setup: Start instance A and B with same Redis channel
- Steps: Client connected to B subscribes; broadcast from A
- Expected: Client on B receives event

### RT-007 Redis unavailable during publish
- Type: Integration
- Setup: Stop Redis after startup
- Steps: Broadcast event
- Expected: Error is surfaced and request fails predictably

### RT-008 Redis unavailable during subscribe startup
- Type: Integration
- Setup: Start app with redis transport while Redis is down
- Steps: Boot module
- Expected: Startup failure is clear and actionable

## 8. Lifecycle Events and Diagnostics

### LD-001 Event bus emits subscription lifecycle
- Type: Unit
- Setup: Attach listeners to event bus
- Steps: Subscribe and unsubscribe
- Expected: subscription.added and subscription.removed are emitted

### LD-002 Event bus emits connection lifecycle
- Type: Unit
- Setup: Attach listeners
- Steps: Open and close stream
- Expected: connection.opened and connection.closed are emitted

### LD-003 Connection snapshots are accurate
- Type: Integration
- Setup: Open streams and subscribe to channels
- Steps: Call /sse/connections
- Expected: count, channels, metadata, timestamps are coherent

## 9. Negative and Security Cases

### NS-001 Missing clientId in open stream request
- Type: Integration
- Setup: Call stream endpoint without clientId
- Steps: GET /sse/events
- Expected: 400 bad request

### NS-002 Missing channel in subscribe request
- Type: Integration
- Setup: POST /sse/subscribe without channel
- Steps: Send request
- Expected: 400 bad request

### NS-003 Unauthorized channel subscription maps to 403
- Type: Integration
- Setup: Force subscribe unauthorized
- Steps: POST /sse/subscribe
- Expected: 403 forbidden

### NS-004 Large channel fan-out under max limits
- Type: Load test
- Setup: Many clients and channel subscriptions within configured limits
- Steps: Broadcast bursts
- Expected: No crash, acceptable latency, stable memory

## Execution Checklist

- Add unit tests where missing in:
  - src/sse/channel-registry.service.spec.ts
  - src/sse/redis.transport.spec.ts
  - src/sse/sse.service.spec.ts
- Add or extend integration/e2e tests in:
  - test/app.e2e-spec.ts
- Run all tests in memory profile
- Run redis profile tests with disposable Redis instance
- Capture flaky timing tests with fake timers where possible

## Suggested Command Set

- npm run build
- npm test -- --runInBand
- npm run test:e2e
