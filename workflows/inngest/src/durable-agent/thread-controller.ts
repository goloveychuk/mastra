import { createSignal, resolveDeliveryAttributes } from '@mastra/core/agent';
import type { AgentSignal, AgentSignalAttributes, SendAgentSignalAccepted, SignalPart } from '@mastra/core/agent';
import type { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import type { Inngest } from 'inngest';

import { InngestPubSub } from '../pubsub';
import type { InngestAgent } from './create-inngest-agent';

export type ThreadScope = { agentId: string; resourceId: string; threadId: string };
type WireSignal<Signal> = Signal extends AgentSignal
  ? Omit<Signal, 'contents' | 'createdAt' | 'acceptedAt'> & {
      contents: string | SignalPart[];
      createdAt?: string;
      acceptedAt?: string;
    }
  : never;
type SignalInput = WireSignal<AgentSignal> & { id: string };
type QueuedTurn = {
  runId: string;
  agentId: string;
  signal: SignalInput;
  streamOptions?: Record<string, unknown>;
  queueOwnerId?: string;
};
export type ThreadCommand = ThreadScope & {
  id: string;
  operation: 'register' | 'send' | 'queue' | 'drain' | 'complete' | 'lookup' | 'cancel';
  runId?: string;
  nextRunId?: string;
  signal?: SignalInput;
  closing?: boolean;
  ifActive?: { behavior?: 'deliver' | 'persist' | 'discard'; attributes?: AgentSignalAttributes };
  ifIdle?: { behavior?: 'wake' | 'persist' | 'discard'; attributes?: AgentSignalAttributes };
  streamOptions?: Record<string, unknown>;
  queueOwnerId?: string;
  signalIds?: string[];
};
export type ThreadReply = {
  runId?: string;
  accepted?: Exclude<SendAgentSignalAccepted, { action: 'wake' }>;
  signals?: SignalInput[];
  busy?: boolean;
  wake?: QueuedTurn;
  signal?: SignalInput;
  error?: string;
  cancelledSignalIds?: string[];
};
type ControllerState = {
  runId?: string;
  agentId?: string;
  closing?: boolean;
  signals: SignalInput[];
  queued: QueuedTurn[];
  replies: Record<string, ThreadReply>;
};

const CONTROLLER_EVENT = 'mastra/inngest.thread.start';
const COMMAND_EVENT = 'mastra/inngest.thread.command';
const COMMANDS_PER_INVOCATION = 64;
type ThreadFunctions = {
  root: ReturnType<Inngest['createFunction']>;
  continuation: ReturnType<Inngest['createFunction']>;
  command: ReturnType<Inngest['createFunction']>;
};
const clients = new WeakMap<Inngest, ThreadFunctions>();
const hosts = new WeakMap<Inngest, Mastra>();

function threadKey(scope: ThreadScope): string {
  return `mastra.thread.${JSON.stringify([scope.resourceId, scope.threadId])}`;
}

function createThreadFunctions(inngest: Inngest) {
  const controller = async ({ event, step }: any): Promise<ControllerState> => {
    const scope = event.data.scope as ThreadScope;
    const state: ControllerState = event.data.state ?? { signals: [], queued: [], replies: {} };
    for (let index = 0; index < COMMANDS_PER_INVOCATION; index++) {
      const received = await step.waitForSignal(`command-${index}`, {
        signal: threadKey(scope),
        timeout: '1d',
        onConflict: 'fail',
      });
      if (!received) {
        if (state.runId) continue;
        return state;
      }
      const { command, replySignal } = received.data as { command: ThreadCommand; replySignal: string };
      let reply = state.replies[command.id];
      if (!reply) {
        switch (command.operation) {
          case 'register':
            reply = { runId: state.runId ?? command.runId, busy: !!state.runId && state.runId !== command.runId };
            if (!reply.busy) {
              state.runId = command.runId;
              state.agentId = command.agentId;
            }
            break;
          case 'send':
          case 'queue': {
            if (command.runId && command.runId !== state.runId) {
              reply = { error: 'No active Inngest run found for signal target' };
              break;
            }
            if (state.runId && state.agentId !== command.agentId) {
              reply = { accepted: { action: 'blocked', reason: 'thread-blocked', runId: state.runId } };
              break;
            }
            const active = !!state.runId && !state.closing;
            const behavior = active ? (command.ifActive?.behavior ?? 'deliver') : (command.ifIdle?.behavior ?? 'wake');
            const signal = serializeThreadSignal(
              resolveDeliveryAttributes(
                createSignal(command.signal!),
                active ? command.ifActive?.attributes : command.ifIdle?.attributes,
              ),
            );
            if (behavior === 'discard' || behavior === 'persist') {
              reply = {
                accepted: { action: signal.transient ? 'discard' : behavior },
                signal,
              };
            } else if (active && state.runId && command.operation === 'send') {
              state.signals.push(signal);
              reply = { accepted: { action: 'deliver', runId: state.runId } };
            } else {
              const turn: QueuedTurn = {
                runId: command.nextRunId!,
                agentId: command.agentId,
                signal,
                streamOptions: command.streamOptions,
                queueOwnerId: command.queueOwnerId,
              };
              reply = { accepted: { action: 'deliver', runId: turn.runId } };
              if (state.runId) {
                state.queued.push(turn);
              } else {
                state.runId = turn.runId;
                state.agentId = turn.agentId;
                state.closing = false;
                reply.wake = turn;
              }
            }
            break;
          }
          case 'drain':
            reply = { signals: command.runId === state.runId ? state.signals.splice(0) : [] };
            if (command.closing && command.runId === state.runId && !reply.signals?.length) state.closing = true;
            break;
          case 'complete':
            if (state.runId === command.runId) {
              state.queued.unshift(
                ...state.signals.splice(0).map(signal => ({
                  runId: `${command.runId}:${signal.id}`,
                  agentId: state.agentId!,
                  signal,
                })),
              );
              const next = state.queued.shift();
              state.runId = next?.runId;
              state.agentId = next?.agentId;
              state.closing = false;
              reply = next ? { wake: next } : {};
            }
            reply ??= {};
            break;
          case 'lookup':
            reply = { runId: state.runId };
            break;
          case 'cancel': {
            const ids = new Set(command.signalIds);
            const cancelledSignalIds: string[] = [];
            state.queued = state.queued.filter(turn => {
              const matches = command.signalIds
                ? ids.has(turn.signal.id)
                : turn.agentId === command.agentId && turn.queueOwnerId === command.queueOwnerId;
              if (matches) cancelledSignalIds.push(turn.signal.id);
              return !matches;
            });
            if (command.signalIds) {
              state.signals = state.signals.filter(signal => {
                if (!ids.has(signal.id)) return true;
                cancelledSignalIds.push(signal.id);
                return false;
              });
            }
            reply = { cancelledSignalIds };
            break;
          }
        }
        reply ??= { error: 'Unsupported Inngest thread command' };
        state.replies[command.id] = reply;
      }
      for (let attempt = 0; attempt < 30; attempt++) {
        const sent = await step.sendSignal(`reply-${index}-${attempt}`, { signal: replySignal, data: reply });
        if (sent.runId) break;
        await step.sleep(`wait-for-reply-${index}-${attempt}`, '100ms');
      }
      if (command.operation === 'complete' && !state.runId && state.signals.length === 0) return state;
    }
    return step.invoke('continue-controller', { function: continuation, data: { scope, state } });
  };

  const continuation = inngest.createFunction({ id: 'mastra-thread-continuation', retries: 10 }, controller);
  const root = inngest.createFunction(
    {
      id: 'mastra-thread-controller',
      triggers: { event: CONTROLLER_EVENT },
      singleton: { key: 'event.data.key', mode: 'skip' },
      retries: 10,
    },
    controller,
  );
  const command = inngest.createFunction(
    {
      id: 'mastra-thread-command',
      triggers: { event: COMMAND_EVENT },
      concurrency: {
        limit: 1,
        key: 'string(size(event.data.resourceId)) + ":" + event.data.resourceId + event.data.threadId',
      },
      retries: 10,
    },
    async ({ event, step }): Promise<ThreadReply> => {
      const request = event.data as ThreadCommand;
      const replySignal = `mastra.thread.reply.${event.id}`;
      const reply = step.waitForSignal<ThreadReply>('reply', {
        signal: replySignal,
        timeout: '2m',
        onConflict: 'fail',
      });
      const deliver = step.run('deliver-command', async () => {
        for (let attempt = 0; attempt < 60; attempt++) {
          await inngest.send({
            name: CONTROLLER_EVENT,
            data: {
              scope: { agentId: request.agentId, resourceId: request.resourceId, threadId: request.threadId },
              key: threadKey(request),
            },
          });
          const sent = await inngest.sendSignal({
            signal: threadKey(request),
            data: { command: request, replySignal },
          });
          if (sent.runId) return;
          await new Promise(resolve => setTimeout(resolve, 100));
        }
        throw new Error('Inngest thread controller did not become available');
      });
      const [response] = await Promise.all([reply, deliver]);
      if (!response) throw new Error('Inngest thread controller did not acknowledge the command');
      const result = response.data as ThreadReply;
      if (result.wake) {
        await step.run('start-agent', async () => {
          const host = hosts.get(inngest);
          if (!host) throw new Error('Inngest thread controller has no registered Mastra host');
          const agent = host.getAgentById(result.wake!.agentId) as unknown as InngestAgent;
          const output = await agent.stream(createSignal(result.wake!.signal), {
            ...result.wake!.streamOptions,
            requestContext: new RequestContext(
              Object.entries((result.wake!.streamOptions?.requestContext as Record<string, unknown> | undefined) ?? {}),
            ),
            runId: result.wake!.runId,
            memory: { thread: request.threadId, resource: request.resourceId },
          });
          void output.output
            .consumeStream()
            .catch(error => host.getLogger().error('Inngest queued run stream failed', { error }));
        });
      }
      if (result.accepted?.action === 'persist' && result.signal) {
        await step.run('persist-signal', async () => {
          const memory = await hosts
            .get(inngest)
            ?.getAgentById(request.agentId)
            .getMemory({
              requestContext: new RequestContext(
                Object.entries((request.streamOptions?.requestContext as Record<string, unknown> | undefined) ?? {}),
              ),
            });
          if (!memory) throw new Error('Persisting an Inngest signal requires agent memory');
          await memory.saveMessages({
            messages: [
              createSignal(result.signal!).toDBMessage({ threadId: request.threadId, resourceId: request.resourceId }),
            ],
          });
        });
      }
      if (request.id) {
        await step.run('publish-reply', () =>
          new InngestPubSub(inngest, 'mastra-thread-control').publish(`agent.control.${request.id}`, {
            type: 'thread-command-result',
            runId: request.id,
            data: response.data,
          }),
        );
      }
      return response.data as ThreadReply;
    },
  );
  return { root, continuation, command };
}

export function getThreadFunctions(inngest: Inngest): ThreadFunctions {
  let functions = clients.get(inngest);
  if (!functions) {
    functions = createThreadFunctions(inngest);
    clients.set(inngest, functions);
  }
  return functions;
}

export function registerThreadHost(inngest: Inngest, mastra: Mastra): void {
  hosts.set(inngest, mastra);
}

export async function requestThreadCommand(inngest: Inngest, command: ThreadCommand): Promise<ThreadReply> {
  const pubsub = new InngestPubSub(inngest, 'mastra-thread-control');
  const topic = `agent.control.${command.id}`;
  let resolveReply!: (reply: ThreadReply) => void;
  const result = new Promise<ThreadReply>(resolve => {
    resolveReply = resolve;
  });
  const callback = (event: { data?: unknown }) => resolveReply(event.data as ThreadReply);
  await pubsub.subscribe(topic, callback);
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await inngest.send({ name: COMMAND_EVENT, data: command });
    return await Promise.race([
      result,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Inngest thread command timed out')), 120_000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    await pubsub.unsubscribe(topic, callback);
  }
}

export function serializeThreadSignal(signal: AgentSignal): SignalInput {
  return createSignal(signal).toDataPart().data as SignalInput;
}
