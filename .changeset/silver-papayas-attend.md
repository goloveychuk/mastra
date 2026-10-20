---
'@mastra/inngest': minor
---

Added Inngest-owned thread routing for signals and queued turns across workers without shared Mastra PubSub coordination. Signal targets require both resourceId and threadId. Use asynchronous controls for remote ownership queries, cancellation, and graceful abort.

```typescript
const target = { resourceId: 'user-1', threadId: 'thread-1' };
const runId = await agent.getActiveThreadRunIdAsync(target);
await agent.cancelQueuedMessagesAsync({ ...target, queueOwnerId: 'reports' });
```

Fixed cross-worker `subscribeToThread()` output when a shared `pubsub` is configured. The shared transport carries only thread output and lifecycle events; steering, thread ownership, and queues remain Inngest-owned without shared PubSub leases or owner discovery.
