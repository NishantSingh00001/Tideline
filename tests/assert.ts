// A tiny test harness: check() records failures, the runner prints a summary.
export const results = { passed: 0, failed: [] as string[] };

export function check(cond: unknown, msg: string) {
  if (cond) results.passed++;
  else {
    results.failed.push(msg);
    console.log("  FAIL " + msg);
  }
}

export function section(name: string) {
  console.log(`\n${name}`);
}

export function throws(fn: () => unknown, pattern: RegExp, msg: string) {
  try {
    fn();
    check(false, `${msg} (expected an error matching ${pattern})`);
  } catch (e) {
    const m = (e as Error).message;
    check(pattern.test(m), `${msg}: got "${m}"`);
  }
}
