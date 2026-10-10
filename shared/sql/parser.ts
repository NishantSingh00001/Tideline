// Recursive-descent parser for the Flink SQL subset Tideline supports:
//
//   [EXPLAIN] SELECT [DISTINCT] items
//   FROM recentchange | TABLE(TUMBLE|HOP|CUMULATE|SESSION(...)) | (subquery)
//   [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT n]
//
// with Flink's syntax for window table functions, ROW_NUMBER() OVER (...)
// for window Top-N, aggregate FILTER clauses, CASE, CAST and INTERVALs.

import type { SqlType, Value } from "../engine/types.ts";
import type { Expr, FromItem, OrderItem, Over, Select, SelectItem, Statement } from "./ast.ts";
import { SqlError, tokenize, type Pos, type Token } from "./lexer.ts";
import { FLINK_RESERVED } from "./reserved.ts";

const RESERVED = new Set(
  "SELECT FROM WHERE GROUP BY HAVING ORDER LIMIT AS AND OR NOT NULL TRUE FALSE CASE WHEN THEN ELSE END IS IN LIKE BETWEEN DISTINCT ALL TABLE CAST INTERVAL OVER PARTITION FILTER ASC DESC JOIN ON UNION EXPLAIN DESCRIPTOR EXTRACT INNER LEFT RIGHT FULL CROSS INSERT UPDATE DELETE CREATE DROP ALTER WITH VALUES SET".split(" "),
);

/** Can't be an unquoted column name or alias (Flink's reserved words plus our syntax words). */
const reserved = (word: string) => {
  const up = word.toUpperCase();
  return RESERVED.has(up) || FLINK_RESERVED.has(up);
};

const TVFS = new Set(["TUMBLE", "HOP", "CUMULATE", "SESSION"]);

const UNIT_MS: Record<string, number> = {
  MILLISECOND: 1,
  MILLISECONDS: 1,
  SECOND: 1000,
  SECONDS: 1000,
  MINUTE: 60_000,
  MINUTES: 60_000,
  HOUR: 3_600_000,
  HOURS: 3_600_000,
  DAY: 86_400_000,
  DAYS: 86_400_000,
};

const TYPE_NAMES: Record<string, SqlType> = {
  STRING: "STRING",
  VARCHAR: "STRING",
  CHAR: "STRING",
  INT: "INT",
  INTEGER: "INT",
  SMALLINT: "INT",
  TINYINT: "INT",
  BIGINT: "BIGINT",
  DOUBLE: "DOUBLE",
  FLOAT: "DOUBLE",
  REAL: "DOUBLE",
  DECIMAL: "DECIMAL",
  BOOLEAN: "BOOLEAN",
  TIMESTAMP_LTZ: "TIMESTAMP_LTZ",
  TIMESTAMP: "TIMESTAMP_LTZ",
};

export function parse(src: string): Statement {
  return new Parser(tokenize(src)).statement();
}

class Parser {
  private i = 0;
  constructor(private toks: Token[]) {}

  private get t(): Token {
    return this.toks[this.i];
  }
  private peek(n = 1): Token {
    return this.toks[Math.min(this.i + n, this.toks.length - 1)];
  }
  private isKw(word: string, tok: Token = this.t) {
    return tok.kind === "ident" && tok.text.toUpperCase() === word;
  }
  private isOp(op: string, tok: Token = this.t) {
    return tok.kind === "op" && tok.text === op;
  }
  private next(): Token {
    const tok = this.t;
    if (tok.kind !== "eof") this.i++;
    return tok;
  }
  private fail(expected?: string, tok: Token = this.t): never {
    const what = tok.kind === "eof" ? "<EOF>" : tok.raw;
    throw new SqlError(
      `SQL parse failed. Encountered "${what}" at line ${tok.pos.line}, column ${tok.pos.col}.${expected ? ` Was expecting ${expected}.` : ""}`,
      tok.pos,
      tok.end,
      "parse",
    );
  }
  private kw(word: string) {
    if (!this.isKw(word)) this.fail(`"${word}"`);
    return this.next();
  }
  private op(op: string) {
    if (!this.isOp(op)) this.fail(`"${op}"`);
    return this.next();
  }
  private acceptKw(word: string) {
    if (this.isKw(word)) {
      this.next();
      return true;
    }
    return false;
  }
  private acceptOp(op: string) {
    if (this.isOp(op)) {
      this.next();
      return true;
    }
    return false;
  }
  private ident(what = "an identifier"): Token {
    const tok = this.t;
    if (tok.kind === "qident") return this.next();
    if (tok.kind === "ident" && !reserved(tok.text)) return this.next();
    this.fail(what);
  }
  private end(): number {
    return this.toks[Math.max(0, this.i - 1)].end;
  }

  statement(): Statement {
    let explain = false;
    if (this.acceptKw("EXPLAIN")) {
      explain = true;
      if (this.acceptKw("PLAN")) this.kw("FOR");
    }
    if (!this.isKw("SELECT")) {
      if (this.t.kind === "ident" && ["INSERT", "CREATE", "DROP", "ALTER", "UPDATE", "DELETE", "SET"].includes(this.t.text.toUpperCase()))
        throw new SqlError(
          `Only SELECT queries run here: Tideline reads the recentchange table that is already defined. (${this.t.text.toUpperCase()} statements work in the Flink pipeline in the repository.)`,
          this.t.pos,
          this.t.end,
          "parse",
        );
      this.fail('"SELECT"');
    }
    const query = this.select();
    this.acceptOp(";");
    if (this.t.kind !== "eof") this.fail("the end of the query");
    return { explain, query };
  }

  select(): Select {
    const start = this.kw("SELECT").pos;
    let distinct = false;
    if (this.acceptKw("DISTINCT")) distinct = true;
    else this.acceptKw("ALL");
    const items: SelectItem[] = [this.selectItem()];
    while (this.acceptOp(",")) items.push(this.selectItem());
    if (!this.isKw("FROM")) this.fail('"FROM" or ","');
    this.next();
    const from = this.fromItem();
    if (this.isOp(",") || this.isKw("JOIN") || this.isKw("INNER") || this.isKw("LEFT") || this.isKw("CROSS"))
      throw new SqlError("Joins aren't supported here: every query reads the single recentchange table.", this.t.pos, this.t.end);
    let where: Expr | undefined;
    if (this.acceptKw("WHERE")) where = this.expr();
    const groupBy: Expr[] = [];
    if (this.acceptKw("GROUP")) {
      this.kw("BY");
      groupBy.push(this.expr());
      while (this.acceptOp(",")) groupBy.push(this.expr());
    }
    let having: Expr | undefined;
    if (this.acceptKw("HAVING")) having = this.expr();
    const orderBy: OrderItem[] = [];
    if (this.acceptKw("ORDER")) {
      this.kw("BY");
      orderBy.push(...this.orderList());
    }
    let limit: number | undefined;
    if (this.acceptKw("LIMIT")) {
      const tok = this.t;
      if (tok.kind !== "number" || !/^\d+$/.test(tok.text)) this.fail("a whole number");
      this.next();
      limit = Number(tok.text);
    }
    if (this.isKw("UNION")) throw new SqlError("UNION isn't supported here.", this.t.pos, this.t.end);
    return { distinct, items, from, where, groupBy, having, orderBy, limit, pos: start };
  }

  private orderList(): OrderItem[] {
    const out: OrderItem[] = [];
    do {
      const e = this.expr();
      let desc = false;
      if (this.acceptKw("DESC")) desc = true;
      else this.acceptKw("ASC");
      if (this.acceptKw("NULLS")) {
        if (!this.acceptKw("FIRST")) this.kw("LAST");
      }
      out.push({ e, desc });
    } while (this.acceptOp(","));
    return out;
  }

  private selectItem(): SelectItem {
    if (this.isOp("*")) {
      const tok = this.next();
      return { e: { k: "star", pos: tok.pos, end: tok.end } };
    }
    if ((this.t.kind === "ident" || this.t.kind === "qident") && this.isOp(".", this.peek()) && this.isOp("*", this.peek(2))) {
      const q = this.next();
      this.next();
      const st = this.next();
      return { e: { k: "star", qual: q.text, pos: q.pos, end: st.end } };
    }
    const e = this.expr();
    let alias: string | undefined;
    if (this.acceptKw("AS")) alias = this.ident("an alias").text;
    else if (this.t.kind === "qident" || (this.t.kind === "ident" && !reserved(this.t.text))) alias = this.next().text;
    return { e, alias };
  }

  private alias(): string | undefined {
    if (this.acceptKw("AS")) return this.ident("an alias").text;
    if (this.t.kind === "qident" || (this.t.kind === "ident" && !reserved(this.t.text))) return this.next().text;
    return undefined;
  }

  private fromItem(): FromItem {
    const start = this.t;
    if (this.isKw("TABLE")) {
      this.next();
      this.op("(");
      const fnTok = this.t;
      const fn = fnTok.text.toUpperCase();
      if (fnTok.kind !== "ident" || !TVFS.has(fn))
        throw new SqlError(
          `SQL parse failed. Encountered "${fnTok.raw}" at line ${fnTok.pos.line}, column ${fnTok.pos.col}. Was expecting a window table function: TUMBLE, HOP, CUMULATE or SESSION.`,
          fnTok.pos,
          fnTok.end,
          "parse",
        );
      this.next();
      this.op("(");
      const args: Expr[] = [];
      if (!this.isOp(")")) {
        args.push(this.tvfArg());
        while (this.acceptOp(",")) args.push(this.tvfArg());
      }
      this.op(")");
      this.op(")");
      const end = this.end();
      return { k: "tvf", fn: fn as "TUMBLE", args, alias: this.alias(), pos: start.pos, end };
    }
    if (this.isOp("(")) {
      this.next();
      if (!this.isKw("SELECT")) this.fail('"SELECT"');
      const query = this.select();
      this.op(")");
      const end = this.end();
      return { k: "sub", query, alias: this.alias(), pos: start.pos, end };
    }
    const name = this.ident("a table name");
    return { k: "table", name: name.text, alias: this.alias(), pos: name.pos, end: name.end };
  }

  private tvfArg(): Expr {
    // Named arguments: DATA => TABLE t, TIMECOL => DESCRIPTOR(ts), SIZE => INTERVAL ...
    if (this.t.kind === "ident" && this.isOp("=>", this.peek())) {
      this.next();
      this.next();
    }
    if (this.isKw("TABLE")) {
      const tok = this.next();
      const name = this.ident("a table name");
      const partitionBy: string[] = [];
      if (this.acceptKw("PARTITION")) {
        this.kw("BY");
        partitionBy.push(this.ident("a column").text);
        while (this.isOp(",") && !this.isKw("DESCRIPTOR", this.peek()) && !this.isKw("INTERVAL", this.peek())) {
          this.next();
          partitionBy.push(this.ident("a column").text);
        }
      }
      return { k: "tableref", name: name.text, partitionBy, pos: tok.pos, end: this.end() };
    }
    if (this.isKw("DESCRIPTOR")) {
      const tok = this.next();
      this.op("(");
      const col = this.ident("a column").text;
      this.op(")");
      return { k: "descriptor", col, pos: tok.pos, end: this.end() };
    }
    return this.expr();
  }

  // ---- expressions, lowest precedence first

  expr(): Expr {
    return this.or();
  }

  private or(): Expr {
    let l = this.and();
    while (this.isKw("OR")) {
      this.next();
      const r = this.and();
      l = { k: "bin", op: "OR", l, r, pos: l.pos, end: r.end };
    }
    return l;
  }

  private and(): Expr {
    let l = this.not();
    while (this.isKw("AND")) {
      this.next();
      const r = this.not();
      l = { k: "bin", op: "AND", l, r, pos: l.pos, end: r.end };
    }
    return l;
  }

  private not(): Expr {
    if (this.isKw("NOT")) {
      const tok = this.next();
      const e = this.not();
      return { k: "un", op: "NOT", e, pos: tok.pos, end: e.end };
    }
    return this.predicate();
  }

  private predicate(): Expr {
    const l = this.additive();
    const tok = this.t;
    if (tok.kind === "op" && ["=", "<>", "<", "<=", ">", ">="].includes(tok.text)) {
      this.next();
      const r = this.additive();
      return { k: "bin", op: tok.text, l, r, pos: l.pos, end: r.end };
    }
    if (this.isKw("IS")) {
      this.next();
      const not = this.acceptKw("NOT");
      if (this.acceptKw("NULL")) return { k: "isnull", e: l, not, pos: l.pos, end: this.end() };
      if (this.acceptKw("TRUE")) return { k: "isbool", e: l, value: true, not, pos: l.pos, end: this.end() };
      if (this.acceptKw("FALSE")) return { k: "isbool", e: l, value: false, not, pos: l.pos, end: this.end() };
      this.fail('"NULL", "TRUE" or "FALSE"');
    }
    let not = false;
    if (this.isKw("NOT") && (this.isKw("LIKE", this.peek()) || this.isKw("IN", this.peek()) || this.isKw("BETWEEN", this.peek()))) {
      this.next();
      not = true;
    }
    if (this.acceptKw("LIKE")) {
      const pat = this.additive();
      return { k: "like", e: l, pat, not, pos: l.pos, end: pat.end };
    }
    if (this.acceptKw("IN")) {
      this.op("(");
      if (this.isKw("SELECT")) throw new SqlError("IN (subquery) isn't supported here; list the values instead.", this.t.pos, this.t.end);
      const list = [this.expr()];
      while (this.acceptOp(",")) list.push(this.expr());
      this.op(")");
      return { k: "in", e: l, list, not, pos: l.pos, end: this.end() };
    }
    if (this.acceptKw("BETWEEN")) {
      this.acceptKw("SYMMETRIC") || this.acceptKw("ASYMMETRIC");
      const lo = this.additive();
      this.kw("AND");
      const hi = this.additive();
      return { k: "between", e: l, lo, hi, not, pos: l.pos, end: hi.end };
    }
    if (not) this.fail('"LIKE", "IN" or "BETWEEN"');
    return l;
  }

  private additive(): Expr {
    let l = this.multiplicative();
    while (this.t.kind === "op" && ["+", "-", "||"].includes(this.t.text)) {
      const op = this.next().text;
      const r = this.multiplicative();
      l = { k: "bin", op, l, r, pos: l.pos, end: r.end };
    }
    return l;
  }

  private multiplicative(): Expr {
    let l = this.unary();
    while (this.t.kind === "op" && ["*", "/", "%"].includes(this.t.text)) {
      const op = this.next().text;
      const r = this.unary();
      l = { k: "bin", op, l, r, pos: l.pos, end: r.end };
    }
    return l;
  }

  private unary(): Expr {
    if (this.isOp("-") || this.isOp("+")) {
      const tok = this.next();
      const e = this.unary();
      if (tok.text === "-" && e.k === "lit" && typeof e.v === "number") return { ...e, v: -e.v, pos: tok.pos };
      return { k: "un", op: tok.text as "-", e, pos: tok.pos, end: e.end };
    }
    return this.primary();
  }

  private primary(): Expr {
    const tok = this.t;
    if (tok.kind === "number") {
      this.next();
      const v = Number(tok.text);
      const t: SqlType = /[eE]/.test(tok.text) ? "DOUBLE" : tok.text.includes(".") ? "DECIMAL" : Math.abs(v) <= 2147483647 ? "INT" : "BIGINT";
      return { k: "lit", v, t, pos: tok.pos, end: tok.end };
    }
    if (tok.kind === "string") {
      this.next();
      return { k: "lit", v: tok.text, t: "STRING", pos: tok.pos, end: tok.end };
    }
    if (this.isOp("(")) {
      this.next();
      if (this.isKw("SELECT")) throw new SqlError("Scalar subqueries aren't supported here.", this.t.pos, this.t.end);
      const e = this.expr();
      this.op(")");
      return { ...e, pos: tok.pos, end: this.end() } as Expr;
    }
    if (tok.kind === "qident") {
      this.next();
      return this.columnRest(tok);
    }
    if (tok.kind !== "ident") this.fail("an expression");
    const up = tok.text.toUpperCase();
    if (up === "NULL" || up === "TRUE" || up === "FALSE") {
      this.next();
      const v: Value = up === "NULL" ? null : up === "TRUE";
      return { k: "lit", v, t: up === "NULL" ? "NULL" : "BOOLEAN", pos: tok.pos, end: tok.end };
    }
    if (up === "CASE") return this.caseExpr();
    if (up === "INTERVAL") return this.interval();
    if (up === "CAST" && this.isOp("(", this.peek())) return this.cast();
    if (up === "EXTRACT" && this.isOp("(", this.peek())) {
      this.next();
      this.op("(");
      const unit = this.next().text.toUpperCase();
      if (!["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DOW"].includes(unit)) this.fail("a time unit (HOUR, MINUTE, …)", this.toks[this.i - 1]);
      this.kw("FROM");
      const e = this.expr();
      this.op(")");
      return { k: "extract", unit, e, pos: tok.pos, end: this.end() };
    }
    const syntax = ["SELECT", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "AND", "OR", "AS", "BY", "ON", "JOIN", "UNION", "WHEN", "THEN", "ELSE", "END", "IS", "IN", "LIKE", "BETWEEN"];
    if (this.isOp("(", this.peek()) && !syntax.includes(up)) return this.call();
    if (reserved(up)) this.fail("an expression");
    this.next();
    return this.columnRest(tok);
  }

  private columnRest(tok: Token): Expr {
    if (this.isOp(".")) {
      this.next();
      const name = this.ident("a column name");
      return { k: "col", name: name.text, qual: tok.text, pos: tok.pos, end: name.end };
    }
    return { k: "col", name: tok.text, pos: tok.pos, end: tok.end };
  }

  private caseExpr(): Expr {
    const tok = this.next();
    let operand: Expr | undefined;
    if (!this.isKw("WHEN")) operand = this.expr();
    const whens: [Expr, Expr][] = [];
    while (this.acceptKw("WHEN")) {
      const w = this.expr();
      this.kw("THEN");
      whens.push([w, this.expr()]);
    }
    if (!whens.length) this.fail('"WHEN"');
    let els: Expr | undefined;
    if (this.acceptKw("ELSE")) els = this.expr();
    this.kw("END");
    return { k: "case", operand, whens, else: els, pos: tok.pos, end: this.end() };
  }

  private interval(): Expr {
    const tok = this.next();
    const lit = this.t;
    if (lit.kind !== "string" && lit.kind !== "number") this.fail("a quoted number, as in INTERVAL '10' SECOND");
    this.next();
    const unitTok = this.t;
    const unit = unitTok.text.toUpperCase();
    if (unitTok.kind !== "ident" || !(unit in UNIT_MS)) this.fail("a unit: SECOND, MINUTE, HOUR or DAY", unitTok);
    this.next();
    const n = Number(lit.text);
    if (!Number.isFinite(n) || n < 0) throw new SqlError(`Illegal interval literal '${lit.text}'.`, lit.pos, lit.end);
    const ms = Math.round(n * UNIT_MS[unit]);
    return { k: "interval", ms, text: `INTERVAL '${lit.text}' ${unit}`, pos: tok.pos, end: unitTok.end };
  }

  private cast(): Expr {
    const tok = this.next();
    this.op("(");
    const e = this.expr();
    this.kw("AS");
    const typeTok = this.t;
    const name = typeTok.text.toUpperCase();
    if (typeTok.kind !== "ident" || !(name in TYPE_NAMES)) this.fail("a type: STRING, INT, BIGINT, DOUBLE, DECIMAL, BOOLEAN or TIMESTAMP_LTZ", typeTok);
    this.next();
    let typeText = name;
    if (this.acceptOp("(")) {
      const parts = [this.next().text];
      while (this.acceptOp(",")) parts.push(this.next().text);
      this.op(")");
      typeText += `(${parts.join(", ")})`;
    }
    this.op(")");
    return { k: "cast", e, type: TYPE_NAMES[name], typeText, pos: tok.pos, end: this.end() };
  }

  private call(): Expr {
    const nameTok = this.next();
    const name = nameTok.text.toUpperCase();
    this.op("(");
    // SUBSTRING(s FROM a [FOR b]) and POSITION(a IN b)
    if (name === "SUBSTRING" || name === "POSITION" || name === "TRIM") return this.specialCall(name, nameTok);
    let distinct = false;
    let star = false;
    const args: Expr[] = [];
    if (this.isOp("*")) {
      this.next();
      star = true;
    } else if (!this.isOp(")")) {
      if (this.acceptKw("DISTINCT")) distinct = true;
      else this.acceptKw("ALL");
      args.push(this.expr());
      while (this.acceptOp(",")) args.push(this.expr());
    }
    this.op(")");
    let filter: Expr | undefined;
    if (this.isKw("FILTER") && this.isOp("(", this.peek())) {
      this.next();
      this.op("(");
      this.kw("WHERE");
      filter = this.expr();
      this.op(")");
    }
    let over: Over | undefined;
    if (this.acceptKw("OVER")) {
      this.op("(");
      over = { partitionBy: [], orderBy: [] };
      if (this.acceptKw("PARTITION")) {
        this.kw("BY");
        over.partitionBy.push(this.expr());
        while (this.acceptOp(",")) over.partitionBy.push(this.expr());
      }
      if (this.acceptKw("ORDER")) {
        this.kw("BY");
        over.orderBy = this.orderList();
      }
      if (this.isKw("ROWS") || this.isKw("RANGE"))
        throw new SqlError("OVER window frames (ROWS/RANGE BETWEEN …) aren't supported here; ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) is.", this.t.pos, this.t.end);
      this.op(")");
    }
    return { k: "call", name, args, distinct, star, filter, over, pos: nameTok.pos, end: this.end() };
  }

  private specialCall(name: string, nameTok: Token): Expr {
    const pos: Pos = nameTok.pos;
    if (name === "POSITION") {
      const needle = this.additive();
      this.kw("IN");
      const hay = this.expr();
      this.op(")");
      return { k: "call", name, args: [needle, hay], distinct: false, star: false, pos, end: this.end() };
    }
    if (name === "TRIM") {
      let mode = "BOTH";
      if (this.isKw("BOTH") || this.isKw("LEADING") || this.isKw("TRAILING")) mode = this.next().text.toUpperCase();
      let chars: Expr = { k: "lit", v: " ", t: "STRING", pos, end: nameTok.end };
      let target: Expr;
      if (this.acceptKw("FROM")) target = this.expr();
      else {
        const first = this.expr();
        if (this.acceptKw("FROM")) {
          chars = first;
          target = this.expr();
        } else target = first;
      }
      this.op(")");
      const modeLit: Expr = { k: "lit", v: mode, t: "STRING", pos, end: nameTok.end };
      return { k: "call", name, args: [target, chars, modeLit], distinct: false, star: false, pos, end: this.end() };
    }
    // SUBSTRING
    const s = this.expr();
    const args: Expr[] = [s];
    if (this.acceptKw("FROM") || this.acceptOp(",")) {
      args.push(this.expr());
      if (this.acceptKw("FOR") || this.acceptOp(",")) args.push(this.expr());
    }
    this.op(")");
    return { k: "call", name: "SUBSTRING", args, distinct: false, star: false, pos, end: this.end() };
  }
}
