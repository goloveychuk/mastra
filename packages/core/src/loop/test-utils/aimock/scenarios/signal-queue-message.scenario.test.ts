/**
 * AIMock Scenario: queueMessage waits for the active run
 *
 * `queueMessage` targeting a thread with an active run must NOT interrupt the
 * run: the message is accepted (with a reserved follow-up run id distinct from
 * the active run) but stays queued until the active run completes, then starts
 * its own run.
 *
 * Mirrors the plain-Agent pin in agent-signals.test.ts ("queues queueMessage
 * until the active run completes") across all engine variants. The message is
 * queued from inside a tool execution, which is the deterministic cross-engine
 * way to act while the run is provably active.
 *
 * Asserts:
 * - acceptance action is `deliver` with a run id different from the active run
 * - no model request contains the queued content while the run is active
 * - the active run finishes with its own response, unaffected by the queue
 * - after the active run completes, the queued message starts a follow-up run
 *   whose model request contains the queued content
 */

import { stepCountIs } from '@internal/ai-sdk-v5';
import { expect, it } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../../events/event-emitter';
import { Mastra } from '../../../../mastra';
import { MockMemory } from '../../../../memory/mock';
import { RequestContext } from '../../../../request-context';
import { createTool } from '../../../../tools';
import { runLoopScenario, useLoopScenarioAimock, describeForAllEngines, createSharedAgent } from '../aimock-scenario';

const QUEUED = 'Queued follow-up';
const QUEUED_AFTER_FAILURE = 'Queued after failed run';
const CANCELLED = 'Cancelled follow-up';

function requestText(request: any): string {
  return JSON.stringify(request?.body?.messages ?? request?.body ?? {});
}

async function waitFor(condition: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for condition');
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

describeForAllEngines('AIMock scenario: queueMessage waits for the active run', engine => {
  const getMock = useLoopScenarioAimock();

  it.runIf(engine === 'inngest')(
    'runs a remote queued follow-up after the suspended run resumes and finishes',
    async () => {
      const threadId = 'queue-after-resume-thread';
      const resourceId = 'queue-after-resume-user';
      const tool = createTool({
        id: 'approve_work',
        description: 'Work requiring approval',
        inputSchema: z.object({}),
        suspendSchema: z.object({ message: z.string() }),
        resumeSchema: z.object({ approved: z.boolean() }),
        execute: async (_input, context) => {
          if (!context?.agent?.resumeData) return context?.agent?.suspend({ message: 'Approve work' });
          return { ok: true };
        },
      });
      const memory = new MockMemory();
      const shared = await createSharedAgent(getMock(), { engine, memory, tools: { approve_work: tool } });
      const sender = new Mastra({
        agents: { sender: shared.agent.__fork() },
        storage: shared.mastra.getStorage(),
        pubsub: new EventEmitterPubSub(),
        logger: false,
      }).getAgent('sender');
      getMock().on(
        { endpoint: 'chat', hasToolResult: false, predicate: req => !JSON.stringify(req).includes(QUEUED) },
        { toolCalls: [{ id: 'call_approve', name: 'approve_work', arguments: {} }] },
      );
      getMock().on(
        { endpoint: 'chat', hasToolResult: true, predicate: req => !JSON.stringify(req).includes(QUEUED) },
        { content: 'resumed response' },
      );
      getMock().on(
        { endpoint: 'chat', predicate: req => JSON.stringify(req).includes(QUEUED) },
        { content: 'queued response' },
      );
      const suspended = await shared.agent.generate('Do the work', {
        memory: { thread: threadId, resource: resourceId },
      });
      expect(suspended.finishReason).toBe('suspended');
      const queued = sender.queueMessage(QUEUED, { threadId, resourceId });
      await expect(queued.accepted).resolves.toMatchObject({ action: 'deliver' });
      expect(
        getMock()
          .getRequests()
          .some((request: any) => requestText(request).includes(QUEUED)),
      ).toBe(false);
      const resumed = await shared.agent.resumeGenerate({ approved: true }, { runId: suspended.runId! });
      expect(resumed.text).toContain('resumed response');
      await waitFor(() =>
        getMock()
          .getRequests()
          .some((request: any) => requestText(request).includes(QUEUED)),
      );
    },
  );

  it.runIf(engine === 'inngest')('starts a queued turn after the active turn fails', async () => {
    const threadId = 'queue-after-failure-thread';
    const resourceId = 'queue-after-failure-user';
    let sender: any;
    const failAfterQueue = createTool({
      id: 'fail_after_queue',
      description: 'Queues a follow-up and then fails the active turn',
      inputSchema: z.object({}),
      execute: async () => {
        const queued = sender.queueMessage(QUEUED_AFTER_FAILURE, { resourceId, threadId });
        await queued.accepted;
        throw new Error('active turn failed');
      },
    });
    const memory = new MockMemory();
    const shared = await createSharedAgent(getMock(), {
      tools: { fail_after_queue: failAfterQueue },
      memory,
      engine,
    });
    sender = new Mastra({
      agents: { sender: shared.agent.__fork() },
      storage: shared.mastra.getStorage(),
      pubsub: new EventEmitterPubSub(),
      logger: false,
    }).getAgent('sender');

    const result = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Fail after queueing the follow-up.',
      sharedAgent: shared,
      memory,
      threadId,
      resourceId,
      fixtures: llm => {
        llm.on(
          {
            endpoint: 'chat',
            hasToolResult: false,
            predicate: request => !requestText(request).includes(QUEUED_AFTER_FAILURE),
          },
          { toolCalls: [{ id: 'call_fail_after_queue', name: 'fail_after_queue', arguments: {} }] },
        );
        llm.on(
          { endpoint: 'chat', predicate: request => requestText(request).includes(QUEUED_AFTER_FAILURE) },
          { content: 'queued turn recovered' },
        );
      },
    });

    expect(await result.output.finishReason).toBe('error');
    await waitFor(() =>
      getMock()
        .getRequests()
        .some(request => requestText(request).includes(QUEUED_AFTER_FAILURE)),
    );
    expect(
      getMock()
        .getRequests()
        .some(request => requestText(request).includes(QUEUED_AFTER_FAILURE)),
    ).toBe(true);
  });

  it.each(engine === 'inngest' ? [false, true] : [false])(
    'holds a queued message until the active run completes, then runs it (remote sender: %s)',
    async remoteSender => {
      const threadId = `queue-message-thread-${remoteSender ? 'remote' : 'local'}`;
      const resourceId = 'queue-message-user';
      const acceptedResults: any[] = [];
      // Journal snapshot taken inside the tool, after acceptance settled while
      // the run was still active: the queued content must not have reached the
      // model yet.
      const queuedSeenWhileActive: boolean[] = [];

      let signalAgent: any;
      const doWork = createTool({
        id: 'do_work',
        description: 'Performs work; a follow-up message is queued while it runs',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        execute: async () => {
          if (remoteSender) {
            const cancelled = signalAgent.queueMessage(CANCELLED, { resourceId, threadId, queueOwnerId: 'draft' });
            await cancelled.accepted;
            expect(
              await signalAgent.cancelQueuedMessagesAsync({ resourceId, threadId, queueOwnerId: 'draft' }),
            ).toEqual({ cancelledSignalIds: [cancelled.signal.id] });
          }
          const result = signalAgent.queueMessage(QUEUED, {
            resourceId,
            threadId,
            ...(remoteSender
              ? {
                  ifIdle: {
                    streamOptions: {
                      requestContext: new RequestContext([['queue-instructions', 'Follow-up context arrived']]),
                    },
                  },
                }
              : {}),
          });
          acceptedResults.push(await result.accepted);
          // Give an eager (incorrect) dispatch a chance to surface before the
          // negative assertion below.
          await new Promise(resolve => setTimeout(resolve, 150));
          queuedSeenWhileActive.push(
            getMock()
              .getRequests()
              .some((request: any) => requestText(request).includes(QUEUED)),
          );
          return { ok: true };
        },
      });

      const memory = new MockMemory();
      const shared = await createSharedAgent(getMock(), {
        tools: { do_work: doWork },
        memory,
        engine,
        defaultOptions: ({ requestContext }) => ({
          instructions: requestContext.get('queue-instructions') ?? 'Do the work',
        }),
      });
      signalAgent = remoteSender
        ? new Mastra({
            agents: { sender: shared.agent.__fork() },
            storage: shared.mastra.getStorage(),
            pubsub: new EventEmitterPubSub(),
            logger: false,
          }).getAgent('sender')
        : shared.agent;

      const { output } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Please do the work.',
        sharedAgent: shared,
        memory,
        threadId,
        resourceId,
        stopWhen: stepCountIs(5),
        fixtures: llm => {
          // All matchers are mutually exclusive so registration order is moot.
          llm.on(
            {
              endpoint: 'chat',
              hasToolResult: false,
              predicate: req => !JSON.stringify(req).includes(QUEUED),
            },
            { toolCalls: [{ id: 'call_work', name: 'do_work', arguments: {} }] },
          );
          llm.on(
            {
              endpoint: 'chat',
              hasToolResult: true,
              predicate: req => !JSON.stringify(req).includes(QUEUED),
            },
            { content: 'first response' },
          );
          // The queued follow-up run.
          llm.on(
            { endpoint: 'chat', predicate: req => JSON.stringify(req).includes(QUEUED) },
            { content: 'queued response' },
          );
        },
      });

      // Accepted for a distinct follow-up run — not delivered into the active one.
      expect(acceptedResults).toHaveLength(1);
      expect(acceptedResults[0]).toMatchObject({ action: 'deliver' });
      const activeRunId = (output as any).runId;
      expect(acceptedResults[0].runId).toBeTruthy();
      expect(acceptedResults[0].runId).not.toBe(activeRunId);

      // Never dispatched while the run was active.
      expect(queuedSeenWhileActive).toEqual([false]);

      // The active run's own output is unaffected by the queued message.
      const text = await output.text;
      expect(text).toContain('first response');
      expect(text).not.toContain('queued response');

      // After the active run completes, the queued message starts its own run.
      await waitFor(() =>
        getMock()
          .getRequests()
          .some((request: any) => requestText(request).includes(QUEUED)),
      );
      expect(
        getMock()
          .getRequests()
          .some((request: any) => requestText(request).includes(CANCELLED)),
      ).toBe(false);
      if (remoteSender) {
        const followUp = getMock()
          .getRequests()
          .find((request: any) => requestText(request).includes(QUEUED));
        expect(requestText(followUp)).toContain('Follow-up context arrived');
      }
    },
  );
});
