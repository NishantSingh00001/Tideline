// Writes queries.json (the SQL run on both engines) and ddl.sql (the table).
import { writeFileSync } from "node:fs";
import { EXAMPLES, PANELS, PARITY_EXTRA } from "../../shared/queries.ts";
import { REVERT_PATTERN } from "../../shared/change.ts";
import { flinkDdl } from "../../shared/table.ts";

const seen = new Set<string>();
const queries = [...Object.values(PANELS), ...EXAMPLES, ...PARITY_EXTRA].filter((q) => (seen.has(q.id) ? false : (seen.add(q.id), true))).map(({ id, sql }) => ({ id, sql }));
writeFileSync(new URL("./queries.json", import.meta.url), JSON.stringify({ queries, revertPattern: REVERT_PATTERN }, null, 2));
writeFileSync(
  new URL("./ddl.sql", import.meta.url),
  flinkDdl(5, "  'connector' = 'filesystem',\n  'path' = '__DATA__',\n  'format' = 'json'") + "\n",
);
console.log(`${queries.length} queries`);
