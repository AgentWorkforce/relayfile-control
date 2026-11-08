/**
 * A small vitest-compatible surface over node:test + node:assert, so Cloud's
 * vitest suites (relayfile-backpressure.test.ts, queue-retry-delay.test.ts) can
 * be ported nearly verbatim without adding vitest as a dependency.
 */
import assert from "node:assert/strict";
import { describe as nodeDescribe, it as nodeIt, mock } from "node:test";

type Fn = () => unknown | Promise<unknown>;

function format(name: string, args: unknown[]): string {
  let i = 0;
  return name.replace(/%[isdjo]/g, () => String(args[i++]));
}

export const describe = (name: string, fn: () => void) => nodeDescribe(name, fn);

type ItFn = ((name: string, fn: Fn) => void) & {
  // Loosely typed on purpose: vitest's per-position inference is not reproduced by this shim.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  each: <T>(rows: readonly T[]) => (name: string, fn: (...args: any[]) => unknown) => void;
};

export const it: ItFn = Object.assign((name: string, fn: Fn) => nodeIt(name, async () => { await fn(); }), {
  each:
    <T>(rows: readonly T[]) =>
    (name: string, fn: (...args: never[]) => unknown) => {
      for (const row of rows) {
        const args = (Array.isArray(row) ? row : [row]) as unknown[];
        nodeIt(format(name, args), async () => {
          await (fn as (...a: unknown[]) => unknown)(...args);
        });
      }
    },
});

type MockFn<A extends unknown[], R> = ((...args: A) => R) & { mock: { calls: A[] } };

function isMock(v: unknown): v is MockFn<unknown[], unknown> {
  return typeof v === "function" && "mock" in (v as object) && Array.isArray((v as unknown as { mock: { calls: unknown } }).mock?.calls);
}

function matchesObject(actual: unknown, expected: unknown): boolean {
  if (expected === null || typeof expected !== "object") return Object.is(actual, expected) || (typeof expected === "number" && actual === expected);
  if (actual === null || typeof actual !== "object") return false;
  for (const [k, v] of Object.entries(expected)) {
    if (!matchesObject((actual as Record<string, unknown>)[k], v)) return false;
  }
  return true;
}

function matchers(actual: unknown, negate: boolean) {
  const check = (pass: boolean, message: string) => {
    if (negate ? pass : !pass) assert.fail(`${negate ? "not " : ""}${message}`);
  };
  return {
    toBe: (e: unknown) => check(Object.is(actual, e), `expected ${String(actual)} to be ${String(e)}`),
    toEqual: (e: unknown) => {
      let pass = true;
      try {
        assert.deepStrictEqual(actual, e);
      } catch {
        pass = false;
      }
      check(pass, `expected ${JSON.stringify(actual)} to equal ${JSON.stringify(e)}`);
    },
    toBeNull: () => check(actual === null, `expected ${String(actual)} to be null`),
    toBeUndefined: () => check(actual === undefined, `expected ${String(actual)} to be undefined`),
    toBeGreaterThan: (n: number) => check((actual as number) > n, `expected ${String(actual)} > ${n}`),
    toBeGreaterThanOrEqual: (n: number) => check((actual as number) >= n, `expected ${String(actual)} >= ${n}`),
    toBeLessThan: (n: number) => check((actual as number) < n, `expected ${String(actual)} < ${n}`),
    toBeLessThanOrEqual: (n: number) => check((actual as number) <= n, `expected ${String(actual)} <= ${n}`),
    toBeInstanceOf: (c: abstract new (...a: never[]) => unknown) => check(actual instanceof c, `expected instance of ${c.name}`),
    toHaveLength: (n: number) => check((actual as { length: number }).length === n, `expected length ${n}, got ${(actual as { length: number }).length}`),
    toContain: (v: unknown) => check((actual as { includes(x: unknown): boolean }).includes(v), `expected to contain ${String(v)}`),
    toMatchObject: (e: unknown) => check(matchesObject(actual, e), `expected ${JSON.stringify(actual)} to match ${JSON.stringify(e)}`),
    toHaveBeenCalledTimes: (n: number) => check(isMock(actual) && actual.mock.calls.length === n, `expected ${n} calls, got ${isMock(actual) ? actual.mock.calls.length : "n/a"}`),
    toHaveBeenCalled: () => check(isMock(actual) && actual.mock.calls.length > 0, "expected to have been called"),
    toThrow: () => {
      let threw = false;
      try {
        (actual as () => unknown)();
      } catch {
        threw = true;
      }
      check(threw, "expected function to throw");
    },
  };
}

export function expect(actual: unknown) {
  return {
    ...matchers(actual, false),
    not: matchers(actual, true),
    rejects: new Proxy(
      {},
      {
        get: (_t, prop: string) => async (...args: unknown[]) => {
          let error: unknown;
          let threw = false;
          try {
            await (actual as Promise<unknown>);
          } catch (e) {
            threw = true;
            error = e;
          }
          if (!threw) assert.fail("expected promise to reject");
          (matchers(error, false) as unknown as Record<string, (...a: unknown[]) => void>)[prop]!(...args);
        },
      },
    ) as ReturnType<typeof matchers>,
  };
}

export const vi = {
  fn<A extends unknown[], R>(impl?: (...args: A) => R): MockFn<A, R> {
    const calls: A[] = [];
    const f = ((...args: A) => {
      calls.push(args);
      return impl?.(...args) as R;
    }) as MockFn<A, R>;
    f.mock = { calls };
    return f;
  },
  useFakeTimers(_opts?: unknown) {
    mock.timers.enable({ apis: ["setTimeout"] });
  },
  useRealTimers() {
    mock.timers.reset();
  },
  async advanceTimersByTimeAsync(ms: number) {
    const step = 10;
    for (let t = 0; t < ms; t += step) {
      await new Promise<void>((r) => setImmediate(r));
      mock.timers.tick(Math.min(step, ms - t));
    }
    await new Promise<void>((r) => setImmediate(r));
  },
};
