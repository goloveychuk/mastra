/**
 * Thread history + topic trimming for Inngest agents in serve topology.
 *
 * A completed durable run's parts must be trimmed from the thread topic once
 * its messages are saved, and a subscriber that joins afterwards with
 * `withInitialHistory` must get the run from storage exactly once — not again
 * as replayed live parts.
 */
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { Agent } from '@mastra/core/agent';
import { InMemoryServerCache } from '@mastra/core/cache';
import { PubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { DefaultStorage } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import { simulateReadableStream } from 'ai';
import { Inngest } from 'inngest';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { RedisStreamsPubSub } from '../../../../pubsub/redis-streams/src';
import { connect } from '../connect';
import { createInngestAgent } from '../durable-agent';
import {
  INNGEST_PORT,
  getSharedInngest,
  getSharedMastra,
  setupSharedTestInfrastructure,
  startConnectInngestDevServer,
  stopInngestDevServer,
  teardownSharedTestInfrastructure,
} from './durable-agent.test.utils';

vi.setConfig({ testTimeout: 150_000, hookTimeout: 90_000 });

const dbUrl = pathToFileURL(path.join(tmpdir(), `mastra-thread-history-serve-${Date.now()}.db`)).href;

function mockModel(): any {
  return {
    specificationVersion: 'v2',
    provider: 'mock',
    modelId: 'thread-history-serve-model',
    supportedUrls: {},
    async doGenerate() {
      throw new Error('not used');
    },
    async doStream() {
      return {
        stream: simulateReadableStream({
          chunks: [
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'r1', modelId: 'thread-history-serve-model', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'stored once' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: { inputTokens: 5, outputTokens: 5, totalTokens: 10 } },
          ],
        }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  };
}

describe('Inngest agent thread history (serve topology)', () => {
  beforeAll(async () => {
    await setupSharedTestInfrastructure();
  });

  afterAll(async () => {
    await teardownSharedTestInfrastructure();
  });

  it('trims a completed run from the thread topic and serves it from history once', async () => {
    const suffix = Date.now();
    const threadId = `thread-history-serve-thread-${suffix}`;
    const resourceId = `thread-history-serve-resource-${suffix}`;
    const trimSpy = vi.spyOn(PubSub.prototype, 'trimTopic');

    const storage = new DefaultStorage({ id: `thread-history-serve-${suffix}`, url: dbUrl });
    const agent = new Agent({
      id: `thread-history-serve-agent-${suffix}`,
      name: 'Thread History Serve Agent',
      instructions: 'Reply briefly.',
      model: mockModel(),
      memory: new Memory({ storage }),
    });
    const inngestAgent = createInngestAgent({ agent, inngest: getSharedInngest() });
    getSharedMastra().addAgent(inngestAgent);

    const result = await inngestAgent.stream([{ role: 'user', content: 'hi' }], {
      memory: { thread: threadId, resource: resourceId },
    });
    try {
      for await (const _ of result.output.fullStream) {
        // drain
      }
    } finally {
      result.cleanup();
    }

    const threadTrims = () =>
      trimSpy.mock.calls.filter(
        ([topic, options]) => topic.includes(encodeURIComponent(threadId)) && options.runId === result.runId,
      );
    await vi.waitFor(() => expect(threadTrims().length).toBeGreaterThan(0), { timeout: 15_000 });

    const subscription = await inngestAgent.subscribeToThread({
      resourceId,
      threadId,
      withInitialHistory: { perPage: 20 },
    } as any);
    const chunks: any[] = [];
    const reader = (async () => {
      for await (const chunk of subscription.stream as AsyncIterable<any>) chunks.push(chunk);
    })();
    await vi.waitFor(() => expect(chunks.some(chunk => chunk.type === 'thread-history')).toBe(true), {
      timeout: 15_000,
    });
    await new Promise(resolve => setTimeout(resolve, 1_000));
    subscription.unsubscribe();
    await reader.catch(() => undefined);

    const history = chunks.find(chunk => chunk.type === 'thread-history');
    const historyText = history.payload.messages
      .filter((message: any) => message.role === 'assistant')
      .flatMap((message: any) => message.content.parts)
      .filter((part: any) => part.type === 'text')
      .map((part: any) => part.text)
      .join('');
    expect(historyText).toBe('stored once');
    expect(chunks.filter(chunk => chunk.type === 'text-delta')).toEqual([]);
    trimSpy.mockRestore();
  });
});

describe('Inngest agent shared thread output', () => {
  it.runIf(!!process.env.REDIS_URL)(
    'streams to another instance through shared Redis output without Redis thread coordination',
    async () => {
      const suffix = crypto.randomUUID();
      const threadId = `thread-output-${suffix}`;
      const resourceId = `resource-output-${suffix}`;
      const agentId = `agent-output-${suffix}`;
      const devServer = await startConnectInngestDevServer();
      const inngest = new Inngest({ id: `thread-output-${suffix}`, baseUrl: `http://localhost:${INNGEST_PORT}` });
      const transports = [0, 1].map(
        () =>
          new RedisStreamsPubSub({
            url: process.env.REDIS_URL!,
            keyPrefix: `inngest-thread-output-${suffix}`,
          }),
      );
      const leaseSpies = transports.flatMap(transport => [
        vi.spyOn(transport, 'acquireLease'),
        vi.spyOn(transport, 'getLeaseOwner'),
      ]);
      const publishSpies = transports.map(transport => vi.spyOn(transport, 'publish'));
      const storage = new DefaultStorage({ id: `thread-output-${suffix}`, url: dbUrl });
      const cache = new InMemoryServerCache();
      const agents = transports.map(pubsub =>
        createInngestAgent({
          inngest,
          pubsub,
          cache,
          agent: new Agent({
            id: agentId,
            name: 'Thread Output Agent',
            instructions: 'Reply briefly.',
            model: mockModel(),
            memory: new Memory({ storage }),
          }),
        }),
      );
      const hosts = agents.map(agent => new Mastra({ storage, cache, agents: { [agentId]: agent } }));
      let worker: Awaited<ReturnType<typeof connect>> | undefined;
      let subscription: Awaited<ReturnType<(typeof agents)[1]['subscribeToThread']>> | undefined;
      let reader: Promise<void> | undefined;
      let result: Awaited<ReturnType<(typeof agents)[0]['stream']>> | undefined;
      const chunks: any[] = [];
      try {
        worker = await connect({ mastra: hosts[0], inngest });
        subscription = await hosts[1].getAgentById(agentId).subscribeToThread({ threadId, resourceId });
        const subscribed = subscription;
        reader = (async () => {
          for await (const chunk of subscribed.stream as AsyncIterable<any>) chunks.push(chunk);
        })();
        result = await agents[0].stream('hi', { memory: { thread: threadId, resource: resourceId } });
        for await (const chunk of result.output.fullStream) void chunk;
        await vi.waitFor(
          () => {
            expect(chunks.some(chunk => chunk.type === 'finish' && chunk.runId === result!.runId)).toBe(true);
          },
          { timeout: 5_000 },
        );
        expect(
          chunks
            .filter(chunk => chunk.type === 'text-delta')
            .map(chunk => chunk.payload.text)
            .join(''),
        ).toBe('stored once');
        const inputChunks = chunks.filter(chunk => chunk.type === 'data-user-message');
        expect(inputChunks).toHaveLength(1);
        expect(inputChunks[0].data.contents).toBe('hi');
        expect(chunks.indexOf(inputChunks[0])).toBeLessThan(chunks.findIndex(chunk => chunk.type === 'text-delta'));
        for (const spy of leaseSpies) expect(spy).not.toHaveBeenCalled();
        for (const spy of publishSpies) {
          expect(spy.mock.calls.some(([topic]) => topic.includes('owner-discovery'))).toBe(false);
        }
      } finally {
        result?.cleanup();
        subscription?.unsubscribe();
        await reader;
        await worker?.close();
        await Promise.all(transports.map(transport => transport.close()));
        await stopInngestDevServer(devServer);
        for (const spy of [...leaseSpies, ...publishSpies]) spy.mockRestore();
      }
    },
  );
});
