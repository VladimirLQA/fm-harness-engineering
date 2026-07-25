import vm from 'node:vm';

export type SandboxResult =
  | { ok: true; result: unknown; logs: string[] }
  | { ok: false; error: string; logs: string[] };

export type SandboxApi = Record<string, (...args: any[]) => unknown>;

export async function runInSandbox(
  code: string,
  api: SandboxApi,
  opts?: { timeoutMs?: number }
) {
  const timeoutMs = opts?.timeoutMs ?? 2_000;
  const logs: string[] = [];

  const context = vm.createContext({
    tools: api,
    console: {
      log: (...args: unknown[]) => logs.push(args.map(String).join(' ')),
    },
  });

  const wrapped = `(async () => {${code}})()`;

  try {
    const pending = vm.runInContext(wrapped, context, { timeout: timeoutMs });
    const result = await withTimeout(pending, timeoutMs);

    return { ok: true, result, logs };
  } catch (error) {
    const err = error instanceof Error ? error.message : String(error);
    return { ok: false, error: err, logs };
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number) {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => {
      setTimeout(
        () => reject(new Error(`execution timedout after ${ms}ms`)),
        ms
      );
    }),
  ]);
}
