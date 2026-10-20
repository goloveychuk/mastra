import { describe, expect, it } from 'vitest';

import { applyThreadCommand } from './thread-controller';
import type { ControllerState, ThreadCommand } from './thread-controller';

function command(overrides: Partial<ThreadCommand>): ThreadCommand {
  return {
    id: 'command-id',
    operation: 'lookup',
    agentId: 'agent-id',
    resourceId: 'resource-id',
    threadId: 'thread-id',
    ...overrides,
  };
}

describe('Inngest thread controller', () => {
  it('persists a durable abort until the active run drains and releases the queued successor', () => {
    const state: ControllerState = {
      runId: 'run-1',
      agentId: 'agent-id',
      signals: [],
      queued: [
        {
          runId: 'run-2',
          agentId: 'agent-id',
          signal: {
            id: 'signal-2',
            type: 'user-message',
            contents: 'follow-up',
          },
        },
      ],
      replies: {},
    };

    expect(applyThreadCommand(state, command({ id: 'abort-1', operation: 'abort', runId: 'run-1' }))).toMatchObject({
      aborted: true,
      runId: 'run-1',
    });

    expect(applyThreadCommand(state, command({ id: 'drain-1', operation: 'drain', runId: 'run-1' }))).toMatchObject({
      aborted: true,
      signals: [],
    });

    expect(
      applyThreadCommand(state, command({ id: 'complete-1', operation: 'complete', runId: 'run-1' })),
    ).toMatchObject({ wake: { runId: 'run-2', signal: { id: 'signal-2' } } });
    expect(state.runId).toBe('run-2');
    expect(state.abortedRunId).toBeUndefined();
  });
});
