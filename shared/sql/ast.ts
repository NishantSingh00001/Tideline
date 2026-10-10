// Syntax tree for the Flink SQL subset Tideline runs.

import type { SqlType, Value } from "../engine/types.ts";
import type { Pos } from "./lexer.ts";

export type Expr =
  | { k: "lit"; v: Value; t: SqlType; pos: Pos; end: number }
  | { k: "col"; name: string; qual?: string; pos: Pos; end: number }
  | { k: "star"; qual?: string; pos: Pos; end: number }
  | { k: "bin"; op: string; l: Expr; r: Expr; pos: Pos; end: number }
  | { k: "un"; op: "NOT" | "-" | "+"; e: Expr; pos: Pos; end: number }
  | { k: "isnull"; e: Expr; not: boolean; pos: Pos; end: number }
  | { k: "isbool"; e: Expr; value: boolean; not: boolean; pos: Pos; end: number }
  | { k: "like"; e: Expr; pat: Expr; not: boolean; pos: Pos; end: number }
  | { k: "in"; e: Expr; list: Expr[]; not: boolean; pos: Pos; end: number }
  | { k: "between"; e: Expr; lo: Expr; hi: Expr; not: boolean; pos: Pos; end: number }
  | { k: "case"; operand?: Expr; whens: [Expr, Expr][]; else?: Expr; pos: Pos; end: number }
  | { k: "cast"; e: Expr; type: SqlType; typeText: string; pos: Pos; end: number }
  | { k: "call"; name: string; args: Expr[]; distinct: boolean; star: boolean; filter?: Expr; over?: Over; pos: Pos; end: number }
  | { k: "interval"; ms: number; text: string; pos: Pos; end: number }
  | { k: "extract"; unit: string; e: Expr; pos: Pos; end: number }
  | { k: "descriptor"; col: string; pos: Pos; end: number }
  | { k: "tableref"; name: string; partitionBy: string[]; pos: Pos; end: number };

export interface Over {
  partitionBy: Expr[];
  orderBy: OrderItem[];
}

export interface OrderItem {
  e: Expr;
  desc: boolean;
}

export interface SelectItem {
  e: Expr;
  alias?: string;
}

export type FromItem =
  | { k: "table"; name: string; alias?: string; pos: Pos; end: number }
  | { k: "tvf"; fn: "TUMBLE" | "HOP" | "CUMULATE" | "SESSION"; args: Expr[]; alias?: string; pos: Pos; end: number }
  | { k: "sub"; query: Select; alias?: string; pos: Pos; end: number };

export interface Select {
  distinct: boolean;
  items: SelectItem[];
  from: FromItem;
  where?: Expr;
  groupBy: Expr[];
  having?: Expr;
  orderBy: OrderItem[];
  limit?: number;
  pos: Pos;
}

export interface Statement {
  explain: boolean;
  query: Select;
}

/** Source text for an expression node (for EXPLAIN and column names). */
export function exprText(e: Expr): string {
  switch (e.k) {
    case "lit":
      return e.v === null ? "NULL" : typeof e.v === "string" ? `'${e.v.replace(/'/g, "''")}'` : String(e.v).toUpperCase() === "TRUE" || String(e.v).toUpperCase() === "FALSE" ? String(e.v).toUpperCase() : String(e.v);
    case "col":
      return e.qual ? `${e.qual}.${e.name}` : e.name;
    case "star":
      return e.qual ? `${e.qual}.*` : "*";
    case "bin":
      return `(${exprText(e.l)} ${e.op} ${exprText(e.r)})`;
    case "un":
      return e.op === "NOT" ? `NOT(${exprText(e.e)})` : `${e.op}${exprText(e.e)}`;
    case "isnull":
      return `${exprText(e.e)} IS ${e.not ? "NOT " : ""}NULL`;
    case "isbool":
      return `${exprText(e.e)} IS ${e.not ? "NOT " : ""}${e.value ? "TRUE" : "FALSE"}`;
    case "like":
      return `${exprText(e.e)} ${e.not ? "NOT " : ""}LIKE ${exprText(e.pat)}`;
    case "in":
      return `${exprText(e.e)} ${e.not ? "NOT " : ""}IN (${e.list.map(exprText).join(", ")})`;
    case "between":
      return `${exprText(e.e)} ${e.not ? "NOT " : ""}BETWEEN ${exprText(e.lo)} AND ${exprText(e.hi)}`;
    case "case":
      return `CASE${e.operand ? " " + exprText(e.operand) : ""} ${e.whens.map(([w, t]) => `WHEN ${exprText(w)} THEN ${exprText(t)}`).join(" ")}${e.else ? ` ELSE ${exprText(e.else)}` : ""} END`;
    case "cast":
      return `CAST(${exprText(e.e)} AS ${e.typeText})`;
    case "call": {
      const inner = e.star ? "*" : `${e.distinct ? "DISTINCT " : ""}${e.args.map(exprText).join(", ")}`;
      return `${e.name}(${inner})${e.filter ? ` FILTER (WHERE ${exprText(e.filter)})` : ""}`;
    }
    case "interval":
      return e.text;
    case "extract":
      return `EXTRACT(${e.unit} FROM ${exprText(e.e)})`;
    case "descriptor":
      return `DESCRIPTOR(${e.col})`;
    case "tableref":
      return `TABLE ${e.name}${e.partitionBy.length ? ` PARTITION BY ${e.partitionBy.join(", ")}` : ""}`;
  }
}

/** Structural equality, ignoring positions (used to match GROUP BY expressions). */
export function sameExpr(a: Expr, b: Expr): boolean {
  return JSON.stringify(strip(a)) === JSON.stringify(strip(b));
}

function strip(x: unknown): unknown {
  if (Array.isArray(x)) return x.map(strip);
  if (x && typeof x === "object") {
    const o: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(x)) if (k !== "pos" && k !== "end") o[k] = strip(v);
    return o;
  }
  return x;
}
