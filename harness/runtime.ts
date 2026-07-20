import { streamText } from 'ai';
import { randomUUID } from 'node:crypto';
import { EventType } from '@shared/events';
import { model } from './model';
import { tools, runTool } from './tools';
import { SYSTEM_PROMPT } from './system-prompt';
import { DBOS } from '@dbos-inc/dbos-sdk';
import type { ModelMessage, JSONValue } from 'ai';
import { emit } from './bus';

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
  messages: ModelMessage[]
): Promise<Turn> {
  const result = streamText({
    model,
    tools,
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

// This is the seam the whole course lives in.
//
// Right now it is a STUB: it announces a workflow, logs that nothing is wired
// up yet, and finishes. The starter app runs end-to-end (browser → socket →
// server → bus → browser) with this hole in the middle.
//
// In LESSON 1 you replace the body with the brittle agent loop:
//   - call the model (streamText) with the task as the prompt
//   - stream tokens out as `model.delta` events
//   - when the model asks for a tool, run it and emit `tool.requested` /
//     `tool.completed`, then feed the result back to the model
//   - repeat until the model stops asking for tools
//
// Then you spend the rest of the day discovering everything this naive loop
// gets wrong in production, and building the harness that fixes it.
async function agentWorkflow(opts: { input: string }): Promise<string> {
  const { input } = opts;
  const workflowId = randomUUID();

  emit({ type: EventType.WorkflowStarted, workflowId, input });

  const messages: ModelMessage[] = [
    { role: 'system', content: SYSTEM_PROMPT },
    { role: 'user', content: input },
  ];

  let step = 0;
  while (step < MAX_STEPS) {
    const turn = await DBOS.runStep(() => modelStep(workflowId, messages), {
      name: `model-#${step}`,
    });

    // Append model's results message(s) - including any tool result - to history.
    messages.push(...turn.responseMessages);

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
      const toolCallOutput = await DBOS.runStep(
        () => toolStep(workflowId, call),
        {
          name: `tool-${call.toolCallId}`,
        }
      );

      messages.push({
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
