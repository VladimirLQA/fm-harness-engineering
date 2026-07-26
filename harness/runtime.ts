import { streamText } from 'ai';
import { randomUUID } from 'node:crypto';
import { EventType } from '@shared/events';
import { model } from './model';
import { runTool } from './tools';
import { DBOS } from '@dbos-inc/dbos-sdk';
import type { ModelMessage, JSONValue, ToolSet } from 'ai';
import { triageAgent, billingAgent, agents } from './agents';
import { emit } from './bus';
import {
  summarize,
  estimateTokens,
  buildContext,
  MAX_CONTEXT_TOKENS,
  KEEP_CONTEXT_TOKENS,
} from './memory';
import { json } from 'node:stream/consumers';
import type { Agent } from 'node:https';

const MAX_STEPS = 20;

type ToolCall = {
  toolCallId: string;
  toolName: string;
  input: Record<string, unknown>;
};

type Turn = {
  text: string;
  toolCalls: ToolCall[];
  responseMessages: ModelMessage[];
};

async function modelStep(
  workflowId: string,
  messages: ModelMessage[],
  agentTools: ToolSet
): Promise<Turn> {
  const result = streamText({
    model,
    tools: agentTools,
    messages,
  });

  for await (const part of result.fullStream) {
    if (part.type === 'text-delta') {
      await emit({ type: EventType.ModelDelta, workflowId, text: part.text });
    }
  }

  const rawCalls = await result.toolCalls;

  return {
    text: await result.text,
    toolCalls: rawCalls.map((c) => ({
      toolCallId: c.toolCallId,
      toolName: c.toolName,
      input: c.input as Record<string, unknown>,
    })),
    responseMessages: (await result.response).messages,
  };
}

async function toolStep(
  workflowId: string,
  call: ToolCall
): Promise<Record<string, unknown>> {
  await emit({
    type: EventType.ToolRequested,
    workflowId,
    toolCallId: call.toolCallId,
    name: call.toolName,
    args: call.input,
  });
  const output = await runTool(call.toolName, call.input);

  await emit({
    type: EventType.ToolCompleted,
    workflowId,
    toolCallId: call.toolCallId,
    result: output,
  });
  return output;
}

export function toolResultMessage(
  call: ToolCall,
  value: JSONValue
): ModelMessage {
  return {
    role: 'tool',
    content: [
      {
        type: 'tool-result',
        toolCallId: call.toolCallId,
        toolName: call.toolName,
        output: { type: 'json', value },
      },
    ],
  };
}

async function agentWorkflow(opts: { input: string }): Promise<string> {
  const { input } = opts;
  const workflowId = randomUUID();

  await DBOS.runStep(
    () => emit({ type: EventType.WorkflowStarted, workflowId, input }),
    { name: 'started' }
  );

  let currentAgent = triageAgent;

  let turns: ModelMessage[][] = [];
  let summary: string = '';

  let step = 0;
  while (step < MAX_STEPS) {
    // 1. Compact: while the recent window is over budget, peel the oldest turns
    //    into the running summary (keeping at least the last turn verbatim).
    if (estimateTokens(turns.flat()) > MAX_CONTEXT_TOKENS) {
      const old: ModelMessage[][] = [];
      while (
        turns.length > 1 &&
        estimateTokens(turns.flat()) > KEEP_CONTEXT_TOKENS
      ) {
        const oldest = turns.shift();
        if (oldest) old.push(oldest);
      }

      if (old.length > 0) {
        summary = await DBOS.runStep(() => summarize(old, summary), {
          name: `summarize-${step}`,
        });

        const contextTokens = estimateTokens(
          buildContext(currentAgent.systemPrompt, input, summary, turns)
        );
        await DBOS.runStep(
          () =>
            emit({
              type: EventType.MemoryCompacted,
              workflowId,
              summarizedTurns: old.length,
              contextTokens,
              summary,
            }),
          { name: `compacted-${step}` }
        );
      }
    }

    // 2 + 3. Hydrate the context and run one turn over it.
    const context = buildContext(
      currentAgent.systemPrompt,
      input,
      summary,
      turns
    );
    const turn = await DBOS.runStep(
      () => modelStep(workflowId, context, currentAgent.tools),
      {
        name: `model-#${step}`,
      }
    );

    const turnMessages: ModelMessage[] = [...turn.responseMessages];

    if (turn.toolCalls.length === 0) {
      await DBOS.runStep(
        () =>
          emit({ type: EventType.ModelCompleted, workflowId, text: turn.text }),
        { name: `model-done-${step}` }
      );
      await DBOS.runStep(
        () =>
          emit({
            type: EventType.WorkflowCompleted,
            workflowId,
            output: turn.text,
          }),
        { name: 'completed' }
      );
      return turn.text;
    }

    for (const call of turn.toolCalls) {
      if (call.toolName === 'handoff') {
        const to = String(call.input.to ?? '');
        const reason = String(call.input.reason ?? '');
        const from = currentAgent.name;

        await DBOS.runStep(
          () =>
            emit({
              type: EventType.AgentHandoff,
              workflowId,
              from,
              to,
              reason,
            }),
          { name: `handoff-${step}` }
        );
        currentAgent = agents[to] ?? currentAgent;
        turnMessages.push(
          toolResultMessage(call, { ok: true, handedOffTo: to })
        );
      } else {
        const toolCallOutput = await DBOS.runStep(
          () => toolStep(workflowId, call),
          {
            name: `tool-${call.toolCallId}`,
          }
        );

        turnMessages.push({
          role: 'tool',
          content: [
            {
              type: 'tool-result',
              toolCallId: call.toolCallId,
              output: { type: 'json', value: toolCallOutput as JSONValue },
              toolName: call.toolName,
            },
          ],
        });
      }
    }

    turns.push(turnMessages);
    step++;
  }

  await DBOS.runStep(
    () =>
      emit({
        type: EventType.WorkflowFailed,
        workflowId,
        error: `Hit the ${MAX_STEPS}-steps limit without finishing.`,
      }),
    { name: 'limit-hit' }
  );
  return '';
}

export const runAgentWorkflow = DBOS.registerWorkflow(agentWorkflow, {
  name: 'agentworkflow',
});
