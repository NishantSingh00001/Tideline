// npm test: runs every test file and exits non-zero on any failure.
import { results } from "./assert.ts";

const files = ["./engine.test.ts", "./sql.test.ts", "./kafka.test.ts", "./parity.test.ts"];
const only = process.argv.slice(2);
const t0 = Date.now();
for (const f of files) {
  if (only.length && !only.some((o) => f.includes(o))) continue;
  await import(f);
}
console.log(`\n${results.passed} checks passed, ${results.failed.length} failed (${((Date.now() - t0) / 1000).toFixed(1)} s)`);
if (results.failed.length) process.exit(1);
