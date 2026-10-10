// SQL tokenizer. Keeps line/column positions so errors read like Flink's:
// `SQL parse failed. Encountered "FORM" at line 1, column 18.`

export interface Pos {
  line: number;
  col: number;
  offset: number;
}

export type TokKind = "ident" | "qident" | "string" | "number" | "op" | "eof";

export interface Token {
  kind: TokKind;
  /** For ident/qident: the name as written; for string: the unescaped value; for op: the operator. */
  text: string;
  /** Raw source text (used in error messages). */
  raw: string;
  pos: Pos;
  end: number;
}

export class SqlError extends Error {
  constructor(
    message: string,
    public pos?: Pos,
    public endOffset?: number,
    public kind: "parse" | "validation" | "runtime" = "validation",
  ) {
    super(message);
  }
}

const OPS = ["<>", "!=", "<=", ">=", "||", "=>", "=", "<", ">", "+", "-", "*", "/", "%", "(", ")", ",", ".", ";"];

export function tokenize(src: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  let line = 1;
  let lineStart = 0;
  const pos = (): Pos => ({ line, col: i - lineStart + 1, offset: i });
  const newline = (at: number) => {
    line++;
    lineStart = at + 1;
  };

  while (i < src.length) {
    const ch = src[i];
    if (ch === "\n") {
      newline(i);
      i++;
      continue;
    }
    if (/\s/.test(ch)) {
      i++;
      continue;
    }
    // -- line comment
    if (ch === "-" && src[i + 1] === "-") {
      while (i < src.length && src[i] !== "\n") i++;
      continue;
    }
    // /* block comment */
    if (ch === "/" && src[i + 1] === "*") {
      const start = pos();
      i += 2;
      while (i < src.length && !(src[i] === "*" && src[i + 1] === "/")) {
        if (src[i] === "\n") newline(i);
        i++;
      }
      if (i >= src.length) throw new SqlError("SQL parse failed. Unterminated comment.", start, src.length, "parse");
      i += 2;
      continue;
    }
    const p = pos();
    // 'string' with '' escapes
    if (ch === "'") {
      let s = "";
      i++;
      for (;;) {
        if (i >= src.length) throw new SqlError("SQL parse failed. Unterminated string literal.", p, src.length, "parse");
        if (src[i] === "'") {
          if (src[i + 1] === "'") {
            s += "'";
            i += 2;
            continue;
          }
          i++;
          break;
        }
        if (src[i] === "\n") newline(i);
        s += src[i++];
      }
      out.push({ kind: "string", text: s, raw: src.slice(p.offset, i), pos: p, end: i });
      continue;
    }
    // `quoted identifier`
    if (ch === "`") {
      let s = "";
      i++;
      for (;;) {
        if (i >= src.length) throw new SqlError("SQL parse failed. Unterminated quoted identifier.", p, src.length, "parse");
        if (src[i] === "`") {
          if (src[i + 1] === "`") {
            s += "`";
            i += 2;
            continue;
          }
          i++;
          break;
        }
        s += src[i++];
      }
      out.push({ kind: "qident", text: s, raw: src.slice(p.offset, i), pos: p, end: i });
      continue;
    }
    // numbers: 12, 12.5, .5, 1e3
    if (/[0-9]/.test(ch) || (ch === "." && /[0-9]/.test(src[i + 1] || ""))) {
      const m = /^(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?/.exec(src.slice(i))!;
      i += m[0].length;
      out.push({ kind: "number", text: m[0], raw: m[0], pos: p, end: i });
      continue;
    }
    if (/[A-Za-z_À-￿]/.test(ch)) {
      const m = /^[A-Za-z_À-￿][A-Za-z0-9_$À-￿]*/.exec(src.slice(i))!;
      i += m[0].length;
      out.push({ kind: "ident", text: m[0], raw: m[0], pos: p, end: i });
      continue;
    }
    const op = OPS.find((o) => src.startsWith(o, i));
    if (op) {
      i += op.length;
      out.push({ kind: "op", text: op === "!=" ? "<>" : op, raw: op, pos: p, end: i });
      continue;
    }
    throw new SqlError(`SQL parse failed. Lexical error at line ${p.line}, column ${p.col}. Encountered: "${ch}"`, p, i + 1, "parse");
  }
  out.push({ kind: "eof", text: "", raw: "<EOF>", pos: pos(), end: i });
  return out;
}
