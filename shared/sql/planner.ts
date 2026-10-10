// Turns a parsed query into a chain of stream operators, type-checking every
// expression on the way, and renders Flink-style EXPLAIN output.

import { AGGREGATES, FUNCTIONS } from "./functions.ts";
import { exprText, sameExpr, type Expr, type FromItem, type Select, type SelectItem } from "./ast.ts";
import { SqlError } from "./lexer.ts";
import { parse } from "./parser.ts";
import { resultType, type AggCall } from "../engine/aggs.ts";
import { CalcOp, GroupAggOp, LimitOp, WindowAggOp, WindowRankOp, WindowTableOp, type SortKey } from "../engine/operators.ts";
import { formatTimestamp, wallClock } from "../engine/time.ts";
import { isIntegral, isNumeric, type Column, type EngineContext, type Operator, type Row, type SqlType, type Value } from "../engine/types.ts";
import { describeWindow, validateWindow, type WindowSpec } from "../engine/windows.ts";
import { COLUMNS, TABLE_NAME } from "../table.ts";

export interface PlanNode {
  name: string;
  detail: string;
}

export interface CompiledQuery {
  sql: string;
  explainOnly: boolean;
  ops: Operator[];
  schema: Column[];
  /** Results are a changelog (+I/-U/+U) rather than append-only. */
  changelog: boolean;
  /** Results arrive once per window, when the watermark passes its end. */
  windowed: boolean;
  window?: WindowSpec;
  /** Source to sink. */
  nodes: PlanNode[];
  explain: string;
  ctx: EngineContext;
  limit?: number;
}

export interface CompileOptions {
  timeZone: string;
  /** Watermark delay in ms, shown in EXPLAIN. */
  watermarkDelay: number;
}

interface Rel {
  ops: Operator[];
  nodes: PlanNode[];
  cols: Column[];
  quals: (string | undefined)[];
  changelog: boolean;
  /** A window TVF that hasn't been turned into an operator yet (it may fuse with GROUP BY). */
  pending?: { spec: WindowSpec; timeIdx: number; baseLen: number };
  windowed: boolean;
  window?: WindowSpec;
}

interface C {
  type: SqlType;
  nullable: boolean;
  fn: (row: Row) => Value;
  text: string;
  rowtime?: boolean;
}

type Resolver = (e: Expr) => C | undefined;

const WINDOW_COLS = ["window_start", "window_end", "window_time"];
const TS_COL: Column = { name: "", type: "TIMESTAMP_LTZ", nullable: false };

const typeLabel = (t: SqlType) =>
  t === "TIMESTAMP_LTZ" ? "TIMESTAMP_LTZ(3)" : t === "STRING" ? "STRING" : t === "INT" ? "INTEGER" : t === "NULL" ? "NULL" : t;

function err(msg: string, e?: { pos: { line: number; col: number; offset: number }; end: number }): never {
  if (e) {
    throw new SqlError(`SQL validation failed. From line ${e.pos.line}, column ${e.pos.col}: ${msg}`, e.pos, e.end, "validation");
  }
  throw new SqlError(`SQL validation failed. ${msg}`);
}

export function compile(sql: string, opts: CompileOptions): CompiledQuery {
  const stmt = parse(sql);
  const ctx: EngineContext = { timeZone: opts.timeZone, currentWatermark: -Infinity };
  const planner = new Planner(ctx, opts);
  const rel = planner.select(stmt.query);
  const nodes = rel.nodes;
  return {
    sql,
    explainOnly: stmt.explain,
    ops: rel.ops,
    schema: rel.cols,
    changelog: rel.changelog,
    windowed: rel.windowed,
    window: rel.window,
    nodes,
    explain: renderExplain(nodes, rel.cols),
    ctx,
    limit: stmt.query.limit,
  };
}

function renderExplain(nodes: PlanNode[], cols: Column[]): string {
  const top = [...nodes].reverse();
  const lines = top.map((n, i) => `${i === 0 ? "" : "   ".repeat(i - 1) + "+- "}${n.name}(${n.detail})`);
  return `== Optimized Execution Plan ==\n${lines.join("\n")}\n\n== Result schema ==\n${cols.map((c) => `${c.name} ${typeLabel(c.type)}${c.nullable ? "" : " NOT NULL"}${c.rowtime ? " *ROWTIME*" : ""}`).join(",\n")}`;
}

class Planner {
  constructor(private ctx: EngineContext, private opts: CompileOptions) {}

  // ---------------------------------------------------------------- FROM

  private source(alias?: string): Rel {
    return {
      ops: [],
      nodes: [
        { name: "TableSourceScan", detail: `table=[[default_catalog, default_database, ${TABLE_NAME}]], fields=[${COLUMNS.filter((c) => c.name !== "delta" && c.name !== "event_time").map((c) => c.name).join(", ")}]` },
        { name: "Calc", detail: "select=[*, (len_new - len_old) AS delta, TO_TIMESTAMP_LTZ(ts_ms, 3) AS event_time]" },
        { name: "WatermarkAssigner", detail: `rowtime=[event_time], watermark=[(event_time - ${this.opts.watermarkDelay}:INTERVAL SECOND)]` },
      ],
      cols: COLUMNS.map((c) => ({ ...c })),
      quals: COLUMNS.map(() => alias || TABLE_NAME),
      changelog: false,
      windowed: false,
    };
  }

  private from(f: FromItem): Rel {
    if (f.k === "table") {
      if (f.name !== TABLE_NAME) {
        const hint = f.name.toLowerCase() === TABLE_NAME ? ` Table names are case-sensitive: did you mean '${TABLE_NAME}'?` : ` The only table is '${TABLE_NAME}'.`;
        err(`Object '${f.name}' not found.${hint}`, f);
      }
      return this.source(f.alias);
    }
    if (f.k === "sub") {
      const rel = this.select(f.query);
      return { ...rel, quals: rel.cols.map(() => f.alias) };
    }
    return this.tvf(f);
  }

  private tvf(f: Extract<FromItem, { k: "tvf" }>): Rel {
    const [data, desc, ...rest] = f.args;
    if (!data || data.k !== "tableref") err(`${f.fn} expects TABLE ${TABLE_NAME} as its first argument.`, f);
    if (data.name !== TABLE_NAME) err(`Object '${data.name}' not found. The only table is '${TABLE_NAME}'.`, data);
    if (!desc || desc.k !== "descriptor") err(`${f.fn} expects DESCRIPTOR(event_time) as its second argument.`, f);
    const timeIdx = COLUMNS.findIndex((c) => c.name === desc.col);
    if (timeIdx < 0) err(`Column '${desc.col}' not found in table '${TABLE_NAME}'.`, desc);
    if (!COLUMNS[timeIdx].rowtime) err(`The window function requires the timecol is a time attribute type, but is ${typeLabel(COLUMNS[timeIdx].type)}.`, desc);
    const ivals = rest.map((a) => {
      if (a.k !== "interval") err(`${f.fn} expects INTERVAL arguments, e.g. INTERVAL '10' SECOND.`, a);
      return a.ms;
    });
    const need: Record<string, [number, number]> = { TUMBLE: [1, 2], HOP: [2, 3], CUMULATE: [2, 3], SESSION: [1, 1] };
    const [lo, hi] = need[f.fn];
    if (ivals.length < lo || ivals.length > hi) {
      const shapes: Record<string, string> = {
        TUMBLE: "TUMBLE(TABLE t, DESCRIPTOR(time), size [, offset])",
        HOP: "HOP(TABLE t, DESCRIPTOR(time), slide, size [, offset])",
        CUMULATE: "CUMULATE(TABLE t, DESCRIPTOR(time), step, size [, offset])",
        SESSION: "SESSION(TABLE t [PARTITION BY col], DESCRIPTOR(time), gap)",
      };
      err(`Invalid number of arguments to ${f.fn}. Expected ${shapes[f.fn]}.`, f);
    }
    if (f.fn !== "SESSION" && data.partitionBy.length) err(`PARTITION BY is only supported for SESSION windows.`, data);
    for (const p of data.partitionBy) if (!COLUMNS.some((c) => c.name === p)) err(`Column '${p}' not found in table '${TABLE_NAME}'.`, data);
    let spec: WindowSpec;
    if (f.fn === "TUMBLE") spec = { kind: "TUMBLE", size: ivals[0], offset: ivals[1] || 0 };
    else if (f.fn === "HOP") spec = { kind: "HOP", slide: ivals[0], size: ivals[1], offset: ivals[2] || 0 };
    else if (f.fn === "CUMULATE") spec = { kind: "CUMULATE", step: ivals[0], size: ivals[1], offset: ivals[2] || 0 };
    else spec = { kind: "SESSION", gap: ivals[0], partitionBy: data.partitionBy };
    const bad = validateWindow(spec);
    if (bad) err(bad, f);
    const base = this.source(f.alias);
    return {
      ...base,
      cols: [
        ...base.cols.map((c) => ({ ...c, rowtime: false })),
        { ...TS_COL, name: "window_start" },
        { ...TS_COL, name: "window_end" },
        { ...TS_COL, name: "window_time", rowtime: true },
      ],
      quals: [...base.quals, f.alias, f.alias, f.alias],
      pending: { spec, timeIdx, baseLen: base.cols.length },
    };
  }

  /** Turns a pending window TVF into a WindowTableFunction operator. */
  private materialize(rel: Rel): Rel {
    if (!rel.pending) return rel;
    const { spec, timeIdx } = rel.pending;
    if (spec.kind === "SESSION") err("A SESSION window table function must be followed by GROUP BY window_start, window_end (window aggregation).");
    const op = new WindowTableOp(rel.cols, spec, timeIdx, this.ctx.timeZone);
    return {
      ...rel,
      ops: [...rel.ops, op],
      nodes: [...rel.nodes, { name: "WindowTableFunction", detail: `window=[${describeWindow(spec)}]` }],
      pending: undefined,
      windowed: true,
      window: spec,
    };
  }

  // ---------------------------------------------------------------- SELECT

  select(sel: Select): Rel {
    if (sel.from.k === "sub" && sel.from.query.items.some((it) => isRowNumber(it.e))) return this.topN(sel);
    if (sel.items.some((it) => isRowNumber(it.e)))
      err("ROW_NUMBER() is supported for window Top-N: wrap this query and filter on the rank, e.g. SELECT * FROM (SELECT *, ROW_NUMBER() OVER (...) AS rownum FROM ...) WHERE rownum <= 10.", sel.items.find((it) => isRowNumber(it.e))!.e);

    let rel = this.from(sel.from);

    // WHERE
    if (sel.where) {
      const aggIn = findAgg(sel.where);
      if (aggIn) err("Aggregate functions are not allowed in WHERE; filter on aggregates with HAVING.", aggIn);
      if (rel.pending && mentionsWindowCol(sel.where)) rel = this.materialize(rel);
      rel = this.filter(rel, sel.where);
    }

    const aggs = collectAggs([...sel.items.map((i) => i.e), ...(sel.having ? [sel.having] : [])]);
    if (sel.having && !aggs.length && !sel.groupBy.length) err("HAVING needs GROUP BY or an aggregate function.", sel.having);
    if (aggs.length || sel.groupBy.length) {
      if (sel.distinct) err("SELECT DISTINCT together with GROUP BY isn't supported here.");
      rel = this.aggregate(rel, sel, aggs);
    } else if (sel.distinct) {
      rel = this.distinct(this.materialize(rel), sel.items);
    } else {
      rel = this.project(this.materialize(rel), sel.items);
    }

    if (sel.orderBy.length) {
      const o = sel.orderBy[0].e;
      err("Sort on a non-time-attribute field is not supported. In a stream, rank rows inside a window instead (window Top-N with ROW_NUMBER).", o);
    }
    if (sel.limit !== undefined) {
      if (rel.changelog) err("LIMIT on an updating result isn't supported here.");
      const op = new LimitOp(rel.cols, sel.limit);
      rel = { ...rel, ops: [...rel.ops, op], nodes: [...rel.nodes, { name: "Limit", detail: `offset=[0], fetch=[${sel.limit}]` }] };
    }
    return rel;
  }

  private filter(rel: Rel, where: Expr): Rel {
    const c = this.cx(where, rel);
    if (c.type !== "BOOLEAN" && c.type !== "NULL") err(`WHERE clause must be a condition, got ${typeLabel(c.type)}.`, where);
    const fn = c.fn;
    const op = new CalcOp(rel.cols, rel.changelog, (row) => fn(row) === true, null, `where=[${c.text}]`);
    return { ...rel, ops: [...rel.ops, op], nodes: [...rel.nodes, { name: "Calc", detail: `select=[*], where=[${c.text}]` }] };
  }

  private expandItems(rel: Rel, items: SelectItem[]): { exprs: C[]; cols: Column[] } {
    const exprs: C[] = [];
    const cols: Column[] = [];
    items.forEach((it, i) => {
      if (it.e.k === "star") {
        const qual = it.e.qual;
        if (qual && !rel.quals.includes(qual)) err(`Unknown identifier '${qual}'.`, it.e);
        rel.cols.forEach((col, ci) => {
          if (qual && rel.quals[ci] !== qual) return;
          exprs.push({ type: col.type, nullable: col.nullable, fn: (r) => r[ci], text: col.name, rowtime: col.rowtime });
          cols.push({ ...col });
        });
        return;
      }
      const c = this.cx(it.e, rel);
      exprs.push(c);
      cols.push({ name: it.alias || (it.e.k === "col" ? it.e.name : `EXPR$${i}`), type: c.type, nullable: c.nullable, rowtime: c.rowtime });
    });
    return { exprs, cols };
  }

  private project(rel: Rel, items: SelectItem[]): Rel {
    const { exprs, cols } = this.expandItems(rel, items);
    const identity = exprs.length === rel.cols.length && items.every((it) => it.e.k === "star");
    if (identity) return { ...rel, cols };
    const op = new CalcOp(cols, rel.changelog, null, exprs.map((e) => e.fn), `select=[${exprs.map((e, i) => (e.text === cols[i].name ? e.text : `${e.text} AS ${cols[i].name}`)).join(", ")}]`);
    return { ...rel, ops: [...rel.ops, op], cols, quals: cols.map(() => undefined), nodes: [...rel.nodes, { name: "Calc", detail: op.detail }] };
  }

  private distinct(rel: Rel, items: SelectItem[]): Rel {
    if (rel.changelog) err("SELECT DISTINCT on an updating result isn't supported here.");
    const { exprs, cols } = this.expandItems(rel, items);
    const op = new GroupAggOp(cols, exprs.map((e) => e.fn), [], exprs.map((e) => e.text));
    return {
      ...rel,
      ops: [...rel.ops, op],
      cols: cols.map((c) => ({ ...c, rowtime: false })),
      quals: cols.map(() => undefined),
      changelog: false,
      nodes: [...rel.nodes, { name: "Exchange", detail: `distribution=[hash[${exprs.map((e) => e.text).join(", ")}]]` }, { name: "GroupAggregate", detail: `groupBy=[${exprs.map((e) => e.text).join(", ")}], select=[${exprs.map((e) => e.text).join(", ")}]` }],
    };
  }

  private aggregate(rel: Rel, sel: Select, aggExprs: Expr[]): Rel {
    if (sel.items.some((i) => i.e.k === "star")) err("SELECT * can't be combined with GROUP BY or aggregates; list the columns.", sel.items.find((i) => i.e.k === "star")!.e);
    const isWin = (g: Expr, name: string) => g.k === "col" && !g.qual && g.name === name;
    const hasStart = sel.groupBy.some((g) => isWin(g, "window_start"));
    const hasEnd = sel.groupBy.some((g) => isWin(g, "window_end"));
    const windowGroup = !!rel.pending && hasStart && hasEnd;
    if ((hasStart || hasEnd) && !rel.pending && !rel.cols.some((c) => c.name === "window_start"))
      err("GROUP BY window_start, window_end needs a window table function in FROM, e.g. FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)).");
    if (rel.pending && (hasStart !== hasEnd))
      err("Window aggregation must group by both window_start and window_end.");
    if (!windowGroup) rel = this.materialize(rel);
    if (rel.changelog) err("Aggregating an updating result isn't supported here (the inner query already produces updates).");

    const keyExprs = sel.groupBy.filter((g) => !(windowGroup && (isWin(g, "window_start") || isWin(g, "window_end") || isWin(g, "window_time"))));
    const keyCs = keyExprs.map((g) => {
      if (findAgg(g)) err("Aggregate functions are not allowed in GROUP BY.", g);
      return this.cx(g, rel);
    });
    const calls: AggCall[] = aggExprs.map((a) => this.aggCall(a as Extract<Expr, { k: "call" }>, rel));
    const keyNames = keyExprs.map((g, i) => (g.k === "col" ? g.name : `$f${i}`));
    const prefix = windowGroup ? 3 : 0;
    const aggCols: Column[] = calls.map((c, i) => ({ name: `$a${i}`, type: c.type, nullable: c.fn !== "COUNT" }));
    const keyCols: Column[] = keyCs.map((c, i) => ({ name: keyNames[i], type: c.type, nullable: c.nullable }));
    const winCols: Column[] = windowGroup
      ? [{ ...TS_COL, name: "window_start" }, { ...TS_COL, name: "window_end" }, { ...TS_COL, name: "window_time", rowtime: true }]
      : [];
    const aggSchema = [...winCols, ...keyCols, ...aggCols];

    let op: Operator;
    let nodes = [...rel.nodes];
    const exchange: PlanNode = { name: "Exchange", detail: keyCs.length ? `distribution=[hash[${keyCs.map((k) => k.text).join(", ")}]]` : "distribution=[single]" };
    if (windowGroup) {
      const { spec, timeIdx } = rel.pending!;
      if (spec.kind === "SESSION") {
        const parts = spec.partitionBy;
        const keyColNames = keyExprs.map((g) => (g.k === "col" ? g.name : null));
        const same = parts.length === keyColNames.length && parts.every((p) => keyColNames.includes(p));
        if (!same) err(`SESSION window aggregation must group by exactly its PARTITION BY columns (${parts.join(", ") || "none"}).`);
      }
      op = new WindowAggOp(aggSchema, spec, timeIdx, keyCs.map((k) => k.fn), calls, this.ctx.timeZone, keyCs.map((k) => k.text));
      nodes.push(exchange, { name: "WindowAggregate", detail: (op as WindowAggOp).detail });
    } else {
      op = new GroupAggOp(aggSchema, keyCs.map((k) => k.fn), calls, keyCs.map((k) => k.text));
      nodes.push(exchange, { name: "GroupAggregate", detail: (op as GroupAggOp).detail });
    }

    // Everything after the aggregate reads its output row.
    const slot = (i: number, col: Column, text: string): C => ({ type: col.type, nullable: col.nullable, fn: (r) => r[i], text, rowtime: col.rowtime });
    const resolver: Resolver = (e) => {
      if (windowGroup && e.k === "col" && !e.qual) {
        const w = WINDOW_COLS.indexOf(e.name);
        if (w >= 0) return slot(w, winCols[w], e.name);
      }
      for (let i = 0; i < keyExprs.length; i++) if (sameExpr(keyExprs[i], e)) return slot(prefix + i, keyCols[i], keyCs[i].text);
      if (e.k === "call" && AGGREGATES.has(e.name) && !e.over) {
        const ai = aggExprs.findIndex((a) => sameExpr(a, e));
        if (ai >= 0) return slot(prefix + keyCs.length + ai, aggCols[ai], calls[ai].text);
      }
      return undefined;
    };
    const post: Rel = { ...rel, cols: aggSchema, quals: aggSchema.map(() => undefined), pending: undefined };
    let having: ((r: Row) => boolean) | null = null;
    let havingText = "";
    if (sel.having) {
      const h = this.cx(sel.having, post, resolver, true);
      if (h.type !== "BOOLEAN") err("HAVING must be a condition.", sel.having);
      having = (r) => h.fn(r) === true;
      havingText = h.text;
    }
    const exprs = sel.items.map((it) => this.cx(it.e, post, resolver, true));
    const cols: Column[] = sel.items.map((it, i) => ({
      name: it.alias || (it.e.k === "col" ? it.e.name : `EXPR$${i}`),
      type: exprs[i].type,
      nullable: exprs[i].nullable,
      rowtime: windowGroup ? exprs[i].rowtime : false,
    }));
    const calc = new CalcOp(cols, !windowGroup, having, exprs.map((e) => e.fn), "");
    nodes.push({ name: "Calc", detail: `select=[${exprs.map((e, i) => (e.text === cols[i].name ? e.text : `${e.text} AS ${cols[i].name}`)).join(", ")}]${havingText ? `, where=[${havingText}]` : ""}` });
    return {
      ops: [...rel.ops, op, calc],
      nodes,
      cols,
      quals: cols.map(() => undefined),
      changelog: !windowGroup,
      windowed: windowGroup || rel.windowed,
      window: windowGroup ? rel.pending!.spec : rel.window,
    };
  }

  private aggCall(e: Extract<Expr, { k: "call" }>, rel: Rel): AggCall {
    const fn = e.name as AggCall["fn"];
    if (e.over) err(`${fn}(...) OVER (...) isn't supported here; use a window table function and GROUP BY.`, e);
    if (e.star && fn !== "COUNT") err(`${fn}(*) isn't valid; only COUNT(*) is.`, e);
    if (!e.star && e.args.length !== 1) err(`Invalid number of arguments to function '${fn}'. Was expecting 1 arguments.`, e);
    for (const a of e.args) {
      const inner = findAgg(a);
      if (inner) err("Aggregate function calls cannot be nested.", inner);
    }
    const arg = e.star ? undefined : this.cx(e.args[0], rel);
    if (arg && (fn === "SUM" || fn === "AVG") && !isNumeric(arg.type) && arg.type !== "NULL")
      err(`Cannot apply '${fn}' to arguments of type '${fn}(<${typeLabel(arg.type)}>)'. Supported form(s): '${fn}(<NUMERIC>)'.`, e);
    let filter: ((r: Row) => boolean) | undefined;
    let filterText = "";
    if (e.filter) {
      const f = this.cx(e.filter, rel);
      if (f.type !== "BOOLEAN") err("FILTER (WHERE ...) must be a condition.", e.filter);
      filter = (r) => f.fn(r) === true;
      filterText = ` FILTER (WHERE ${f.text})`;
    }
    return {
      fn,
      star: e.star,
      distinct: e.distinct,
      arg: arg?.fn,
      argType: arg?.type,
      filter,
      type: resultType(fn, arg?.type),
      text: `${fn}(${e.star ? "*" : `${e.distinct ? "DISTINCT " : ""}${arg!.text}`})${filterText}`,
    };
  }

  // ---------------------------------------------------------------- window Top-N

  private topN(outer: Select): Rel {
    const inner = (outer.from as Extract<FromItem, { k: "sub" }>).query;
    const rnIdx = inner.items.findIndex((it) => isRowNumber(it.e));
    const rnItem = inner.items[rnIdx];
    const rnCall = rnItem.e as Extract<Expr, { k: "call" }>;
    const rnName = rnItem.alias || `EXPR$${rnIdx}`;
    if (rnCall.args.length) err("ROW_NUMBER() takes no arguments.", rnCall);
    if (inner.groupBy.length || collectAggs(inner.items.map((i) => i.e)).length)
      err("Compute the aggregate in a subquery, then rank it: ROW_NUMBER() can't share a SELECT with GROUP BY.", rnCall);

    let rel = this.from(inner.from);
    if (inner.where) {
      if (rel.pending && mentionsWindowCol(inner.where)) rel = this.materialize(rel);
      rel = this.filter(rel, inner.where);
    }
    rel = this.materialize(rel);
    if (rel.changelog) err("Window Top-N needs an append-only input such as a window aggregation.");
    const over = rnCall.over!;
    const colIdx = (e: Expr, what: string) => {
      if (e.k !== "col") err(`${what} must name a column.`, e);
      const i = this.resolve(rel, e);
      return i;
    };
    const part = over.partitionBy.map((e) => colIdx(e, "PARTITION BY"));
    const startIdx = rel.cols.findIndex((c) => c.name === "window_start");
    const endIdx = rel.cols.findIndex((c) => c.name === "window_end");
    if (startIdx < 0 || endIdx < 0 || !part.includes(startIdx) || !part.includes(endIdx))
      err("ROW_NUMBER() in a stream needs PARTITION BY window_start, window_end (window Top-N), over the output of a window table function or window aggregation.", rnCall);
    if (!over.orderBy.length) err("ROW_NUMBER() needs ORDER BY inside OVER (...).", rnCall);
    const order: SortKey[] = over.orderBy.map((o) => ({ idx: colIdx(o.e, "ORDER BY in OVER (...)"), desc: o.desc }));

    // The rank limit comes from the outer WHERE: rownum <= N, rownum < N or rownum = N.
    const conj = conjuncts(outer.where);
    let limit: number | undefined;
    let mode = "";
    const residual: Expr[] = [];
    for (const c of conj) {
      const m = rankBound(c, rnName);
      if (m && limit === undefined) {
        limit = m.limit;
        mode = m.text;
      } else residual.push(c);
    }
    if (limit === undefined)
      err(`Window Top-N needs a filter on the rank in the outer query, e.g. WHERE ${rnName} <= 10.`, outer.where || rnCall);
    if (limit < 1) err("The rank limit must be at least 1.", outer.where!);

    const others = part.filter((i) => i !== startIdx && i !== endIdx);
    const rankCols: Column[] = [...rel.cols, { name: rnName, type: "BIGINT", nullable: false }];
    const detail = `rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=${limit}], partitionBy=[${part.map((i) => rel.cols[i].name).join(", ")}], orderBy=[${over.orderBy.map((o, k) => `${rel.cols[order[k].idx].name} ${o.desc ? "DESC" : "ASC"}`).join(", ")}], select=[*, ${rnName}]`;
    const rankOp = new WindowRankOp(rankCols, startIdx, endIdx, others, order, limit!, true, detail);
    let r: Rel = {
      ...rel,
      ops: [...rel.ops, rankOp],
      cols: rankCols,
      quals: rankCols.map(() => undefined),
      nodes: [
        ...rel.nodes,
        { name: "Exchange", detail: `distribution=[hash[${part.filter((i) => i !== startIdx && i !== endIdx).map((i) => rel.cols[i].name).join(", ") || "window_start, window_end"}]]` },
        { name: "WindowRank", detail },
      ],
      windowed: true,
    };
    void mode;
    // The inner SELECT list (usually *, ROW_NUMBER() ...), then the outer query.
    // `*` here means the ranked input's columns (not the rank itself).
    const innerItems: SelectItem[] = inner.items.flatMap((it, i): SelectItem[] => {
      if (i === rnIdx) return [{ e: { k: "col", name: rnName, pos: it.e.pos, end: it.e.end }, alias: rnName }];
      if (it.e.k === "star") return rel.cols.map((c) => ({ e: { k: "col", name: c.name, pos: it.e.pos, end: it.e.end } as Expr }));
      return [it];
    });
    r = this.project(r, innerItems);
    r = { ...r, quals: r.cols.map(() => (outer.from as { alias?: string }).alias) };
    if (residual.length) r = this.filter(r, residual.reduce((a, b) => ({ k: "bin", op: "AND", l: a, r: b, pos: a.pos, end: b.end })));
    if (collectAggs(outer.items.map((i) => i.e)).length || outer.groupBy.length) err("Aggregating the result of a window Top-N isn't supported here.");
    r = this.project(r, outer.items);
    if (outer.limit !== undefined) {
      const op = new LimitOp(r.cols, outer.limit);
      r = { ...r, ops: [...r.ops, op], nodes: [...r.nodes, { name: "Limit", detail: `offset=[0], fetch=[${outer.limit}]` }] };
    }
    return r;
  }

  // ---------------------------------------------------------------- expressions

  private resolve(rel: Rel, e: Extract<Expr, { k: "col" }>): number {
    const matches: number[] = [];
    rel.cols.forEach((c, i) => {
      if (c.name === e.name && (!e.qual || rel.quals[i] === e.qual)) matches.push(i);
    });
    if (matches.length === 1) return matches[0];
    if (matches.length > 1) err(`Column '${e.name}' is ambiguous.`, e);
    if (e.qual && !rel.quals.includes(e.qual)) err(`Table '${e.qual}' not found.`, e);
    const ci = rel.cols.find((c) => c.name.toLowerCase() === e.name.toLowerCase());
    const where = rel.pending || rel.cols.length === COLUMNS.length ? "any table" : "the input";
    err(`Column '${e.name}' not found in ${where}${ci ? `; did you mean '${ci.name}'?` : "."}`, e);
  }

  private cx(e: Expr, rel: Rel, resolver?: Resolver, postAgg = false): C {
    const r = resolver?.(e);
    if (r) return r;
    const rec = (x: Expr) => this.cx(x, rel, resolver, postAgg);
    switch (e.k) {
      case "lit":
        return { type: e.t === "DECIMAL" ? "DECIMAL" : e.t, nullable: e.v === null, fn: () => e.v, text: exprText(e) };
      case "col": {
        if (postAgg) err(`Expression '${exprText(e)}' is not being grouped.`, e);
        const i = this.resolve(rel, e);
        const col = rel.cols[i];
        return { type: col.type, nullable: col.nullable, fn: (row) => row[i], text: col.name, rowtime: col.rowtime };
      }
      case "star":
        err("'*' can only be used as a select item or in COUNT(*).", e);
      // falls through (err never returns)
      case "un": {
        const a = rec(e.e);
        if (e.op === "NOT") {
          boolOrErr(a, e);
          return { type: "BOOLEAN", nullable: a.nullable, fn: (row) => { const v = a.fn(row); return v === null ? null : !v; }, text: `NOT(${a.text})` };
        }
        if (!isNumeric(a.type)) err(`Cannot apply '${e.op}' to arguments of type '${e.op}<${typeLabel(a.type)}>'.`, e);
        return { type: a.type, nullable: a.nullable, fn: e.op === "-" ? (row) => { const v = a.fn(row); return v === null ? null : -(v as number); } : a.fn, text: `${e.op}${a.text}` };
      }
      case "bin":
        return this.binary(e, rec);
      case "isnull": {
        const a = rec(e.e);
        return { type: "BOOLEAN", nullable: false, fn: (row) => (a.fn(row) === null) !== e.not, text: `${e.not ? "IS NOT NULL" : "IS NULL"}(${a.text})` };
      }
      case "isbool": {
        const a = rec(e.e);
        boolOrErr(a, e);
        return { type: "BOOLEAN", nullable: false, fn: (row) => (a.fn(row) === e.value) !== e.not, text: `${a.text} IS ${e.not ? "NOT " : ""}${e.value ? "TRUE" : "FALSE"}` };
      }
      case "like": {
        const a = rec(e.e);
        const p = rec(e.pat);
        if ((a.type !== "STRING" && a.type !== "NULL") || (p.type !== "STRING" && p.type !== "NULL")) err(`Cannot apply 'LIKE' to arguments of type '<${typeLabel(a.type)}> LIKE <${typeLabel(p.type)}>'.`, e);
        const cache = new Map<string, RegExp>();
        const toRe = (pat: string) => {
          let re = cache.get(pat);
          if (!re) {
            re = new RegExp("^" + pat.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/%/g, "[\\s\\S]*").replace(/_/g, "[\\s\\S]") + "$", "u");
            cache.set(pat, re);
          }
          return re;
        };
        return {
          type: "BOOLEAN",
          nullable: a.nullable || p.nullable,
          fn: (row) => {
            const s = a.fn(row);
            const pat = p.fn(row);
            if (s === null || pat === null) return null;
            return toRe(String(pat)).test(String(s)) !== e.not;
          },
          text: `${e.not ? "NOT LIKE" : "LIKE"}(${a.text}, ${p.text})`,
        };
      }
      case "in": {
        const a = rec(e.e);
        const list = e.list.map(rec);
        for (const l of list) comparableOrErr(a, l, "IN", e);
        return {
          type: "BOOLEAN",
          nullable: true,
          fn: (row) => {
            const v = a.fn(row);
            if (v === null) return null;
            let sawNull = false;
            for (const l of list) {
              const x = l.fn(row);
              if (x === null) sawNull = true;
              else if (x === v) return !e.not;
            }
            return sawNull ? null : e.not;
          },
          text: `${e.not ? "NOT " : ""}IN(${a.text}, ${list.map((l) => l.text).join(", ")})`,
        };
      }
      case "between": {
        const a = rec(e.e);
        const lo = rec(e.lo);
        const hi = rec(e.hi);
        comparableOrErr(a, lo, "BETWEEN", e);
        comparableOrErr(a, hi, "BETWEEN", e);
        return {
          type: "BOOLEAN",
          nullable: true,
          fn: (row) => {
            const v = a.fn(row);
            const l = lo.fn(row);
            const h = hi.fn(row);
            if (v === null || l === null || h === null) return null;
            return (v >= l && v <= h) !== e.not;
          },
          text: `${e.not ? "NOT " : ""}BETWEEN(${a.text}, ${lo.text}, ${hi.text})`,
        };
      }
      case "case": {
        const operand = e.operand ? rec(e.operand) : undefined;
        const whens = e.whens.map(([w, t]) => [rec(w), rec(t)] as const);
        const els = e.else ? rec(e.else) : undefined;
        for (const [w] of whens) {
          if (operand) comparableOrErr(operand, w, "CASE", e);
          else boolOrErr(w, e);
        }
        const results = [...whens.map(([, t]) => t), ...(els ? [els] : [])];
        const type = commonType(results.map((x) => x.type), e);
        const nullable = !els || results.some((x) => x.nullable);
        return {
          type,
          nullable,
          fn: (row) => {
            const ov = operand ? operand.fn(row) : undefined;
            for (const [w, t] of whens) {
              const wv = w.fn(row);
              if (operand ? ov !== null && wv === ov : wv === true) return t.fn(row);
            }
            return els ? els.fn(row) : null;
          },
          text: `CASE(${whens.map(([w, t]) => `${w.text}, ${t.text}`).join(", ")}${els ? `, ${els.text}` : ""})`,
        };
      }
      case "cast":
        return this.cast(rec(e.e), e.type, e.typeText, e);
      case "extract": {
        const a = rec(e.e);
        if (a.type !== "TIMESTAMP_LTZ") err(`Cannot apply 'EXTRACT' to a ${typeLabel(a.type)}.`, e);
        const tz = this.ctx.timeZone;
        const unit = e.unit;
        return {
          type: "BIGINT",
          nullable: a.nullable,
          fn: (row) => {
            const v = a.fn(row);
            if (v === null) return null;
            const w = wallClock(v as number, tz);
            return unit === "YEAR" ? w.year : unit === "MONTH" ? w.month : unit === "DAY" ? w.day : unit === "HOUR" ? w.hour : unit === "MINUTE" ? w.minute : unit === "SECOND" ? w.second : w.dow + 1;
          },
          text: `EXTRACT(FLAG(${unit}), ${a.text})`,
        };
      }
      case "interval":
        err("An INTERVAL can only be added to or subtracted from a timestamp, or passed to a window function.", e);
      // falls through
      case "descriptor":
      case "tableref":
        err(`${exprText(e)} is only valid inside a window table function.`, e);
      // falls through
      case "call":
        return this.call(e, rec, rel);
    }
  }

  private binary(e: Extract<Expr, { k: "bin" }>, rec: (x: Expr) => C): C {
    if (e.op === "AND" || e.op === "OR") {
      const a = rec(e.l);
      const b = rec(e.r);
      boolOrErr(a, e.l);
      boolOrErr(b, e.r);
      const and = e.op === "AND";
      return {
        type: "BOOLEAN",
        nullable: a.nullable || b.nullable,
        fn: and
          ? (row) => {
              const x = a.fn(row);
              if (x === false) return false;
              const y = b.fn(row);
              if (y === false) return false;
              return x === null || y === null ? null : true;
            }
          : (row) => {
              const x = a.fn(row);
              if (x === true) return true;
              const y = b.fn(row);
              if (y === true) return true;
              return x === null || y === null ? null : false;
            },
        text: `${and ? "AND" : "OR"}(${a.text}, ${b.text})`,
      };
    }
    // Timestamp ± INTERVAL
    if ((e.op === "+" || e.op === "-") && e.r.k === "interval") {
      const a = rec(e.l);
      if (a.type !== "TIMESTAMP_LTZ") err(`Cannot apply '${e.op}' to arguments of type '<${typeLabel(a.type)}> ${e.op} <INTERVAL>'.`, e);
      const ms = e.r.ms * (e.op === "-" ? -1 : 1);
      return { type: "TIMESTAMP_LTZ", nullable: a.nullable, fn: (row) => { const v = a.fn(row); return v === null ? null : (v as number) + ms; }, text: `(${a.text} ${e.op} ${e.r.ms}:INTERVAL)` };
    }
    const a = rec(e.l);
    const b = rec(e.r);
    const nullable = a.nullable || b.nullable;
    if (e.op === "||") {
      if ((a.type !== "STRING" && a.type !== "NULL") || (b.type !== "STRING" && b.type !== "NULL")) err(`Cannot apply '||' to arguments of type '<${typeLabel(a.type)}> || <${typeLabel(b.type)}>'.`, e);
      return { type: "STRING", nullable, fn: (row) => { const x = a.fn(row); const y = b.fn(row); return x === null || y === null ? null : String(x) + String(y); }, text: `||(${a.text}, ${b.text})` };
    }
    if (["=", "<>", "<", "<=", ">", ">="].includes(e.op)) {
      comparableOrErr(a, b, e.op, e);
      const cmp: Record<string, (x: Value, y: Value) => boolean> = {
        "=": (x, y) => x === y,
        "<>": (x, y) => x !== y,
        "<": (x, y) => (x as number) < (y as number),
        "<=": (x, y) => (x as number) <= (y as number),
        ">": (x, y) => (x as number) > (y as number),
        ">=": (x, y) => (x as number) >= (y as number),
      };
      const f = cmp[e.op];
      return { type: "BOOLEAN", nullable, fn: (row) => { const x = a.fn(row); const y = b.fn(row); return x === null || y === null ? null : f(x, y); }, text: `${e.op}(${a.text}, ${b.text})` };
    }
    // Arithmetic
    for (const x of [a, b]) if (!isNumeric(x.type) && x.type !== "NULL") err(`Cannot apply '${e.op}' to arguments of type '<${typeLabel(a.type)}> ${e.op} <${typeLabel(b.type)}>'. Supported form(s): '<NUMERIC> ${e.op} <NUMERIC>'.`, e);
    const type = numericResult(a.type, b.type);
    const integral = isIntegral(type);
    let f: (x: number, y: number) => number;
    switch (e.op) {
      case "+": f = (x, y) => x + y; break;
      case "-": f = (x, y) => x - y; break;
      case "*": f = (x, y) => x * y; break;
      case "/":
        f = integral
          ? (x, y) => {
              if (y === 0) throw new SqlError("Job failed: java.lang.ArithmeticException: / by zero", undefined, undefined, "runtime");
              return Math.trunc(x / y);
            }
          : (x, y) => x / y;
        break;
      default:
        f = (x, y) => {
          if (integral && y === 0) throw new SqlError("Job failed: java.lang.ArithmeticException: / by zero", undefined, undefined, "runtime");
          return x % y;
        };
    }
    return { type, nullable, fn: (row) => { const x = a.fn(row); const y = b.fn(row); return x === null || y === null ? null : f(x as number, y as number); }, text: `${e.op}(${a.text}, ${b.text})` };
  }

  private cast(a: C, to: SqlType, typeText: string, e: Expr): C {
    const tz = this.ctx.timeZone;
    const from = a.type;
    const bad = () => err(`Cast function cannot convert value of type ${typeLabel(from)} to type ${typeText}.`, e);
    let conv: (v: Value) => Value;
    switch (to) {
      case "STRING":
        conv = from === "TIMESTAMP_LTZ" ? (v) => formatTimestamp(v as number, tz) : from === "BOOLEAN" ? (v) => (v ? "TRUE" : "FALSE") : (v) => String(v);
        break;
      case "INT":
      case "BIGINT":
        if (from === "TIMESTAMP_LTZ") bad();
        conv = (v) => {
          if (typeof v === "boolean") return v ? 1 : 0;
          if (typeof v === "number") return Math.trunc(v);
          const s = String(v).trim();
          if (!/^[+-]?\d+$/.test(s)) throw new SqlError(`Job failed: java.lang.NumberFormatException: For input string: "${v}"`, undefined, undefined, "runtime");
          return Number(s);
        };
        break;
      case "DOUBLE":
      case "DECIMAL":
        if (from === "TIMESTAMP_LTZ") bad();
        conv = (v) => {
          if (typeof v === "boolean") return v ? 1 : 0;
          const n = Number(typeof v === "string" ? v.trim() : v);
          if (typeof v === "string" && (v.trim() === "" || Number.isNaN(n))) throw new SqlError(`Job failed: java.lang.NumberFormatException: For input string: "${v}"`, undefined, undefined, "runtime");
          return n;
        };
        break;
      case "BOOLEAN":
        if (from === "TIMESTAMP_LTZ") bad();
        conv = (v) => (typeof v === "string" ? (/^\s*true\s*$/i.test(v) ? true : /^\s*false\s*$/i.test(v) ? false : null) : typeof v === "number" ? v !== 0 : v);
        break;
      case "TIMESTAMP_LTZ":
        if (from === "BOOLEAN") bad();
        conv = (v) => (typeof v === "number" ? v : Number.isFinite(Date.parse(String(v).replace(" ", "T") + "Z")) ? Date.parse(String(v).replace(" ", "T") + "Z") : null);
        break;
      default:
        bad();
    }
    return { type: to, nullable: a.nullable || to === "BOOLEAN", fn: (row) => { const v = a.fn(row); return v === null ? null : conv(v); }, text: `CAST(${a.text} AS ${typeText})` };
  }

  private call(e: Extract<Expr, { k: "call" }>, rec: (x: Expr) => C, rel: Rel): C {
    if (AGGREGATES.has(e.name)) err(`Aggregate function '${e.name}' isn't allowed here.`, e);
    if (e.name === "ROW_NUMBER") err("ROW_NUMBER() is only supported for window Top-N.", e);
    if (e.over) err(`${e.name}(...) OVER (...) isn't supported here.`, e);
    const def = FUNCTIONS[e.name];
    if (!def) {
      const close = Object.keys(FUNCTIONS).find((n) => n.replace(/_/g, "") === e.name.replace(/_/g, ""));
      err(`No match found for function signature ${e.name}(${e.args.map(() => "<ANY>").join(", ")})${close ? `; did you mean ${close}?` : "."}`, e);
    }
    if (e.star || e.distinct) err(`${e.name} doesn't take ${e.star ? "*" : "DISTINCT"}.`, e);
    if (e.args.length < def.min || e.args.length > def.max)
      err(`Invalid number of arguments to function '${e.name}'. Was expecting ${def.min === def.max ? def.min : `${def.min} to ${def.max}`} arguments.`, e);
    const args = e.args.map(rec);
    if (e.name === "CURRENT_WATERMARK" && !args[0].rowtime) err("CURRENT_WATERMARK must be called with the rowtime attribute (event_time).", e);
    const t = def.type(args.map((a) => a.type));
    if (t.includes(" ")) err(t, e);
    const type = t as SqlType;
    const ctx = this.ctx;
    const impl = def.impl;
    const fns = args.map((a) => a.fn);
    const nullSafe = def.nullSafe;
    void rel;
    return {
      type,
      nullable: nullSafe || e.name === "REGEXP_EXTRACT" || e.name === "SPLIT_INDEX" || e.name === "SUBSTRING" || e.name === "CURRENT_WATERMARK" ? true : args.some((a) => a.nullable),
      fn: (row) => {
        const vals = fns.map((f) => f(row));
        if (!nullSafe && vals.some((v) => v === null)) return null;
        return impl(vals, ctx);
      },
      text: `${e.name}(${args.map((a) => a.text).join(", ")})`,
    };
  }
}

// ---------------------------------------------------------------- helpers

function isRowNumber(e: Expr) {
  return e.k === "call" && e.name === "ROW_NUMBER";
}

function walk(e: Expr | undefined, visit: (x: Expr) => boolean | void) {
  if (!e) return;
  if (visit(e) === false) return;
  switch (e.k) {
    case "bin": walk(e.l, visit); walk(e.r, visit); break;
    case "un": case "isnull": case "isbool": case "cast": case "extract": walk(e.e, visit); break;
    case "like": walk(e.e, visit); walk(e.pat, visit); break;
    case "in": walk(e.e, visit); e.list.forEach((x) => walk(x, visit)); break;
    case "between": walk(e.e, visit); walk(e.lo, visit); walk(e.hi, visit); break;
    case "case": walk(e.operand, visit); e.whens.forEach(([w, t]) => { walk(w, visit); walk(t, visit); }); walk(e.else, visit); break;
    case "call": e.args.forEach((x) => walk(x, visit)); walk(e.filter, visit); break;
  }
}

function findAgg(e: Expr): Expr | undefined {
  let found: Expr | undefined;
  walk(e, (x) => {
    if (found) return false;
    if (x.k === "call" && AGGREGATES.has(x.name) && !x.over) {
      found = x;
      return false;
    }
  });
  return found;
}

function collectAggs(exprs: Expr[]): Expr[] {
  const out: Expr[] = [];
  for (const e of exprs)
    walk(e, (x) => {
      if (x.k === "call" && AGGREGATES.has(x.name) && !x.over) {
        if (!out.some((o) => sameExpr(o, x))) out.push(x);
        return false;
      }
    });
  return out;
}

function mentionsWindowCol(e: Expr): boolean {
  let yes = false;
  walk(e, (x) => {
    if (x.k === "col" && WINDOW_COLS.includes(x.name)) yes = true;
  });
  return yes;
}

function conjuncts(e?: Expr): Expr[] {
  if (!e) return [];
  if (e.k === "bin" && e.op === "AND") return [...conjuncts(e.l), ...conjuncts(e.r)];
  return [e];
}

function rankBound(e: Expr, rn: string): { limit: number; text: string } | null {
  if (e.k !== "bin") return null;
  const isRn = (x: Expr) => x.k === "col" && x.name === rn;
  const lit = (x: Expr) => (x.k === "lit" && typeof x.v === "number" && Number.isInteger(x.v) ? x.v : null);
  let op = e.op;
  let n: number | null = null;
  if (isRn(e.l)) n = lit(e.r);
  else if (isRn(e.r)) {
    n = lit(e.l);
    op = ({ "<": ">", ">": "<", "<=": ">=", ">=": "<=" } as Record<string, string>)[op] || op;
  } else return null;
  if (n === null) return null;
  if (op === "<=") return { limit: n, text: `<= ${n}` };
  if (op === "<") return { limit: n - 1, text: `< ${n}` };
  if (op === "=" && n === 1) return { limit: 1, text: "= 1" };
  return null;
}

function boolOrErr(c: C, e: Expr) {
  if (c.type !== "BOOLEAN" && c.type !== "NULL") err(`Expected a BOOLEAN expression but got ${typeLabel(c.type)}: ${exprText(e)}`, e);
}

function family(t: SqlType) {
  return isNumeric(t) ? "NUMERIC" : t;
}

function comparableOrErr(a: C, b: C, op: string, e: Expr) {
  if (a.type === "NULL" || b.type === "NULL") return;
  if (family(a.type) !== family(b.type))
    err(`Cannot apply '${op}' to arguments of type '<${typeLabel(a.type)}> ${op} <${typeLabel(b.type)}>'. Supported form(s): '<COMPARABLE_TYPE> ${op} <COMPARABLE_TYPE>'`, e);
}

function numericResult(a: SqlType, b: SqlType): SqlType {
  if (a === "NULL") return b === "NULL" ? "INT" : b;
  if (b === "NULL") return a;
  if (a === "DOUBLE" || b === "DOUBLE") return "DOUBLE";
  if (a === "DECIMAL" || b === "DECIMAL") return "DECIMAL";
  if (a === "BIGINT" || b === "BIGINT") return "BIGINT";
  return "INT";
}

function commonType(types: SqlType[], e: Expr): SqlType {
  const known = types.filter((t) => t !== "NULL");
  if (!known.length) return "NULL";
  if (known.every((t) => isNumeric(t))) return (known as SqlType[]).reduce((a, b) => numericResult(a, b));
  const first = known[0];
  if (known.some((t) => t !== first)) err(`Illegal mixing of types in CASE or COALESCE statement: ${known.map(typeLabel).join(", ")}.`, e);
  return first;
}
