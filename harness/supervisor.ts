import { DBOS } from '@dbos-inc/dbos-sdk';
import { generateObject, generateText } from 'ai';
import { z } from 'zod';
import { EventType } from '@shared/events';
import { emit } from './bus';
import { model } from './model';
import { runInvestigator } from './investigators';

const PlanSchema = z.object({
  steps: z.array(
    z.object({
      id: z.string().describe('short id, i.e. "billing"'),
      agent: z.enum(['billing', 'technical', 'sales']),
      objective: z.string().describe('what this investigator should find out'),
    })
  ),
});

async function makePlan(task: string) {
  const { object } = await generateObject({
    model,
    schema: PlanSchema,
    system:
      'Decompose a customer escalation into independent sub-tasks — one per area the message actually raises (billing / technical / sales). Only include relevant areas.',
    prompt: task,
  });

  return object;
}

async function synthesize(
  task: string,
  findings: Array<{ agent: string; findings: string }>
) {
  const { text } = await generateText({
    model,
    system:
      "You are a support lead. Using your investigators' findings, write ONE clear, friendly reply to the customer that addresses every point they raised. If an area's investigation is missing, acknowledge it briefly and say you'll follow up.",
    prompt: `Customer escalation:\n${task}\n\nInvestigator findings:\n${
      findings.map((f) => `[${f.agent}] ${f.findings}`).join('\n\n') || '(none)'
    }`,
  });

  return text;
}

async function supervisorWorkflow(task: string) {
  const workflowId = DBOS.workflowID ?? 'unknown';

  await DBOS.runStep(
    () => emit({ type: EventType.WorkflowStarted, workflowId, input: task }),
    { name: 'plan started' }
  );

  const plan = await DBOS.runStep(() => makePlan(task), { name: 'plan' });

  await DBOS.runStep(
    () => emit({ type: EventType.PlanCreated, workflowId, steps: plan.steps }),
    { name: 'plan end' }
  );

  const settled = await Promise.allSettled(
    plan.steps.map((s) => {
      return DBOS.runStep(async () => {
        await emit({
          type: EventType.SubagentStarted,
          workflowId,
          stepId: s.id,
          agent: s.agent,
          objective: s.objective,
        });
        const findings = await runInvestigator(s.agent, s.objective);
        await emit({
          type: EventType.SubagentCompleted,
          workflowId,
          stepId: s.id,
          agent: s.agent,
          findings,
        });
        return { agent: s.agent, findings };
      });
    })
  );

  const findings: { agent: string; findings: string }[] = [];
  for (let i = 0; i < settled.length; i++) {
    const result = settled[i];
    const step = plan.steps[i];
    if (result.status === 'fulfilled') {
      findings.push(result.value);
    } else {
      await DBOS.runStep(
        () =>
          emit({
            type: EventType.SubagentFailed,
            workflowId,
            stepId: step.id,
            agent: step.agent,
            error: String(result.reason),
          }),
        { name: `subagent-failed-${step.id}` }
      );
    }
  }

  const reply = await DBOS.runStep(() => synthesize(task, findings), {
    name: 'synthesize',
  });
  await DBOS.runStep(
    () => emit({ type: EventType.ModelCompleted, workflowId, text: reply }),
    { name: 'synth' }
  );
  await DBOS.runStep(
    () =>
      emit({ type: EventType.WorkflowCompleted, workflowId, output: reply }),
    { name: 'completed' }
  );
  return reply;
}

export const runSupervisorWorkflow = DBOS.registerWorkflow(supervisorWorkflow, {
  name: 'supervisorWorkflow',
});
