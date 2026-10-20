import type { Mastra } from '@mastra/core/mastra';
import type { InngestFunction } from 'inngest';
import { isInngestAgent } from './durable-agent';
import { getThreadFunctions, registerThreadHost } from './durable-agent/thread-controller';
import { InngestWorkflow } from './workflow';

export function collectInngestFunctions({
  mastra,
  functions: userFunctions = [],
}: {
  mastra: Mastra;
  functions?: InngestFunction.Like[];
}) {
  /**
   * Inngest agents share the same logical durable workflow IDs and resolve the
   * concrete agent from the workflow input, so only the first registered
   * agent's workflow needs to be served.
   */
  const durableAgent = Object.values(mastra.listAgents()).find(agent => isInngestAgent(agent));
  const workflows = [...Object.values(mastra.listWorkflows()), ...(durableAgent?.getDurableWorkflows() ?? [])];
  const workflowFunctions = new Map<string, InngestFunction.Like>();

  for (const workflow of workflows) {
    if (!(workflow instanceof InngestWorkflow)) continue;

    workflow.__registerMastra(mastra);
    if (workflow.id === 'inngest:durable-agentic-loop') {
      registerThreadHost(workflow.inngest, mastra);
      for (const fn of Object.values(getThreadFunctions(workflow.inngest))) {
        workflowFunctions.set(fn.id(), fn);
      }
    }
    for (const fn of workflow.getFunctions()) {
      const functionId = fn.id();
      if (!workflowFunctions.has(functionId)) {
        workflowFunctions.set(functionId, fn);
      }
    }
  }

  return [...workflowFunctions.values(), ...userFunctions];
}
