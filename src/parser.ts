import { tokenize, Token } from './tokenizer';
import * as AST from './ast';
import { parseComparisonExpression, parsePrimaryExpression } from './parser/expressions';
import { DEFAULT_MAX_DEPTH } from './constants';
import {
  type DmlParser,
  parseDeleteStatement,
  parseInsertStatement,
  parseSetItem as parseDmlSetItem,
  parseUpdateStatement,
} from './parser/dml';
const CLAUSE_KEYWORDS = new Set([
  'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
  'UNION', 'INTERSECT', 'EXCEPT', 'ON', 'SET', 'VALUES',
  'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'JOIN',
  'INTO', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER',
  'DROP', 'WITH', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR',
  'RETURNING', 'FETCH', 'WINDOW', 'LATERAL', 'FOR', 'USING', 'ESCAPE',
]);

// Lookup table for multi-word SQL type names.
// Key = last consumed word, value = set of valid next words.
const TYPE_CONTINUATIONS: Record<string, Set<string>> = {
  DOUBLE: new Set(['PRECISION']),
  CHARACTER: new Set(['VARYING']),
  CHAR: new Set(['VARYING']),
  NATIONAL: new Set(['CHARACTER']),
  TIMESTAMP: new Set(['WITH', 'WITHOUT']),
  TIME: new Set(['WITH', 'WITHOUT', 'ZONE']),
  WITH: new Set(['TIME']),
  WITHOUT: new Set(['TIME']),
};

/**
 * Options for {@link parse} and the {@link Parser} constructor.
 */
export interface ParseOptions {
  /**
   * When `true` (default), unparseable statements are preserved as
   * `RawStatement` nodes instead of throwing.
   *
   * @default true
   */
  recover?: boolean;

  /**
   * Maximum allowed nesting depth. Exceeding this limit throws an error
   * to prevent stack overflow on deeply nested or adversarial input.
   *
   * @default 200
   */
  maxDepth?: number;

  /**
   * Optional callback invoked when recovery mode captures an unparseable
   * statement as a `RawExpression`.
   *
   * Not called when `recover` is `false` or when parsing fails with
   * `MaxDepthError` (which always throws).
   */
  onRecover?: (error: ParseError, raw: AST.RawExpression | null) => void;

  /**
   * Optional callback invoked when recovery cannot produce raw text for a
   * failed statement (for example, the error occurs at end-of-input).
   *
   * This makes statement drops explicit to callers in recovery mode.
   */
  onDropStatement?: (error: ParseError) => void;
}

/**
 * Thrown when the parser encounters unexpected tokens and recovery is disabled.
 *
 * Carries the offending {@link Token} plus the human-readable description of
 * what was expected, making it straightforward to build rich diagnostics.
 *
 * @example
 * ```typescript
 * import { parse, ParseError } from '@vcoppola/sqlfmt';
 *
 * try {
 *   parse('SELECT FROM;', { recover: false });
 * } catch (err) {
 *   if (err instanceof ParseError) {
 *     console.error(`${err.line}:${err.column} - expected ${err.expected}`);
 *   }
 * }
 * ```
 */
export class ParseError extends Error {
  /** The token that triggered the error. */
  readonly token: Token;
  /** Human-readable description of what the parser expected. */
  readonly expected: string;
  /** One-based line number of the offending token. */
  readonly line: number;
  /** One-based column number of the offending token. */
  readonly column: number;

  constructor(expected: string, token: Token) {
    const got = token.type === 'eof' ? 'end of input' : `"${token.value}" (${token.type})`;
    super(`Expected ${expected}, got ${got}`);
    this.name = 'ParseError';
    this.expected = expected;
    this.token = token;
    this.line = token.line;
    this.column = token.column;
  }
}

/**
 * Thrown when the parser exceeds the configured maximum nesting depth.
 *
 * This is a subclass of {@link ParseError} so callers catching `ParseError`
 * still receive it, but it can be distinguished via `instanceof` when depth
 * violations need special handling (e.g. never recovered to RawStatement).
 */
export class MaxDepthError extends ParseError {
  /** The configured nesting depth limit that was exceeded. */
  readonly maxDepth: number;

  constructor(maxDepth: number, token: Token) {
    super(`maximum nesting depth ${maxDepth}`, token);
    this.name = 'MaxDepthError';
    this.maxDepth = maxDepth;
  }
}

/**
 * Recursive-descent SQL parser.
 *
 * Consumes an array of {@link Token} objects (produced by {@link tokenize}) and
 * builds an AST of {@link AST.Node} trees. Supports SELECT, INSERT, UPDATE,
 * DELETE, MERGE, CTEs, DDL (CREATE/ALTER/DROP TABLE, CREATE INDEX, CREATE VIEW),
 * GRANT/REVOKE, and PostgreSQL-specific syntax.
 *
 * The parser can operate in two modes controlled by {@link ParseOptions.recover}:
 * - **Recover mode** (default): unparseable statements are preserved as
 *   `RawStatement` nodes so the formatter can pass them through unchanged.
 * - **Strict mode**: parsing failures throw a {@link ParseError}.
 *
 * @example
 * import { tokenize, Parser } from '@vcoppola/sqlfmt';
 *
 * const tokens = tokenize('SELECT 1; SELECT 2;');
 * const parser = new Parser(tokens);
 * const ast = parser.parseStatements();
 * // ast is an array of AST.Node (two SelectStatement nodes)
 */
export class Parser {
  private tokens: Token[];
  private pos: number = 0;
  private blankLinesBeforeToken = new Map<number, number>();
  private readonly recover: boolean;
  private readonly maxDepth: number;
  private readonly onRecover?: (error: ParseError, raw: AST.RawExpression | null) => void;
  private readonly onDropStatement?: (error: ParseError) => void;
  private depth: number = 0;

  constructor(tokens: Token[], options: ParseOptions = {}) {
    // Record blank lines preceding each non-whitespace token, then filter whitespace.
    for (let i = 0; i < tokens.length; i++) {
      if (tokens[i].type === 'whitespace') {
        const nlCount = (tokens[i].value.match(/\n/g) || []).length;
        if (nlCount >= 2) {
          let j = i + 1;
          while (j < tokens.length && tokens[j].type === 'whitespace') j++;
          if (j < tokens.length) {
            this.blankLinesBeforeToken.set(tokens[j].position, nlCount - 1);
          }
        }
      }
    }
    this.tokens = tokens.filter(t => t.type !== 'whitespace');
    this.recover = options.recover ?? true;
    this.maxDepth = options.maxDepth ?? DEFAULT_MAX_DEPTH;
    this.onRecover = options.onRecover;
    this.onDropStatement = options.onDropStatement;
  }

  /**
   * Parse all statements from the token stream.
   *
   * **Recovery mode** (`this.recover === true`, the default):
   * When a statement fails to parse, the error is caught and the parser
   * rewinds to the start of that statement. The tokens are then consumed
   * as a {@link AST.RawExpression} (raw text) up to the next semicolon.
   * This allows the formatter to pass through unrecognized SQL verbatim
   * rather than aborting the entire input.
   *
   * Recovery is limited to ordinary {@link ParseError}s. The following
   * errors always propagate regardless of recovery mode:
   *   - {@link MaxDepthError} — nesting depth exceeded (security guard)
   *   - Non-ParseError exceptions (e.g. runtime TypeError)
   *
   * **Strict mode** (`this.recover === false`):
   * All ParseErrors propagate immediately, letting the caller handle them.
   *
   * API consumers should be aware that in recovery mode the returned array
   * may contain `RawExpression` nodes alongside structured statement nodes.
   * These raw nodes preserve the original text and can be detected via
   * `node.type === 'raw'`.
   */
  parseStatements(): AST.Node[] {
    const stmts: AST.Node[] = [];
    while (!this.isAtEnd()) {
      this.skipSemicolons();
      if (this.isAtEnd()) break;
      const stmtStart = this.pos;
      try {
        const stmt = this.parseStatement();
        if (stmt) stmts.push(stmt);
      } catch (err) {
        if (!this.recover) throw err;
        if (!(err instanceof ParseError)) throw err;
        if (err instanceof MaxDepthError) throw err;
        // Recovery: rewind and consume as raw text until next semicolon
        this.pos = stmtStart;
        const raw = this.parseRawStatement();
        this.onRecover?.(err, raw);
        if (raw) {
          stmts.push(raw);
        } else {
          this.onDropStatement?.(err);
        }
      }
      this.skipSemicolons();
    }
    return stmts;
  }

  private skipSemicolons(): void {
    while (this.check(';')) this.advance();
  }

  private parseStatement(): AST.Node | null {
    const comments = this.consumeComments();

    if (this.isAtEnd()) {
      if (comments.length === 0) return null;
      return this.commentsToRaw(comments);
    }

    // Parenthesized top-level query expression
    if (this.check('(')) {
      const query = this.tryParseQueryExpressionAtCurrent(comments);
      if (query) return query;
    }

    const kw = this.peekUpper();

    if (kw === 'WITH') return this.parseCTE(comments);
    if (kw === 'SELECT') return this.parseUnionOrSelect(comments);
    if (kw === 'INSERT') return this.parseInsert(comments);
    if (kw === 'UPDATE') return this.parseUpdate(comments);
    if (kw === 'DELETE') return this.parseDelete(comments);
    if (kw === 'CREATE') return this.parseCreate(comments);
    if (kw === 'ALTER') return this.parseAlter(comments);
    if (kw === 'DROP') return this.parseDrop(comments);
    if (kw === 'MERGE') return this.parseMerge(comments);
    if (kw === 'GRANT' || kw === 'REVOKE') return this.parseGrant(comments);
    if (kw === 'TRUNCATE') return this.parseTruncate(comments);
    if (kw === 'VALUES') return this.parseStandaloneValues(comments);

    // Unknown statement — consume until semicolon
    const raw = this.parseRawStatement();
    if (!raw) {
      if (comments.length === 0) return null;
      return this.commentsToRaw(comments);
    }
    if (comments.length === 0) return raw;
    return {
      type: 'raw',
      text: `${this.commentsToRaw(comments).text}\n${raw.text}`.trim(),
    };
  }

  private commentsToRaw(comments: AST.CommentNode[]): AST.RawExpression {
    return {
      type: 'raw',
      text: comments.map(c => c.text).join('\n'),
    };
  }

  private parseRawStatement(): AST.RawExpression | null {
    const start = this.pos;
    while (!this.isAtEnd() && !this.check(';')) {
      this.advance();
    }
    const end = this.pos;

    let text = '';
    for (let i = start; i < end; i++) {
      const token = this.tokens[i];
      text += token.value;
      if (i < end - 1) text += ' ';
    }
    text = text.trim();

    if (this.check(';')) {
      this.advance();
      if (text) text += ';';
      else text = ';';
    }

    if (!text) return null;
    return { type: 'raw', text };
  }

  private parseUnionOrSelect(comments: AST.CommentNode[]): AST.Node {
    const first = this.parseSelectOrParenSelect();
    // Parser constructs AST nodes incrementally; type-assert to assign readonly field
    (first as any).leadingComments = comments;

    if (this.checkUnionKeyword()) {
      const members: { statement: AST.SelectStatement; parenthesized: boolean }[] = [
        { statement: first, parenthesized: first.parenthesized || false }
      ];
      const operators: string[] = [];

      while (this.checkUnionKeyword()) {
        const op = this.consumeUnionKeyword();
        operators.push(op);
        const next = this.parseSelectOrParenSelect();
        members.push({ statement: next, parenthesized: next.parenthesized || false });
      }

      return {
        type: 'union',
        members,
        operators,
        leadingComments: comments,
      } as AST.UnionStatement;
    }

    return first;
  }

  private parseQueryExpression(comments: AST.CommentNode[] = []): AST.QueryExpression {
    return this.withDepth(() => {
      if (this.peekUpper() === 'WITH') {
        return this.parseCTE(comments);
      }
      if (this.peekUpper() === 'SELECT' || this.check('(')) {
        const query = this.parseUnionOrSelect(comments);
        if (query.type === 'select' || query.type === 'union') return query;
      }
      throw new ParseError('query expression', this.peek());
    });
  }

  private tryParse<T>(fn: () => T): T | null {
    const checkpoint = this.pos;
    try {
      return fn();
    } catch (err) {
      if (err instanceof ParseError) {
        this.pos = checkpoint;
        return null;
      }
      throw err;
    }
  }

  private tryParseQueryExpressionAtCurrent(comments: AST.CommentNode[] = []): AST.QueryExpression | null {
    return this.tryParse(() => this.parseQueryExpression(comments));
  }

  private checkUnionKeyword(): boolean {
    const kw = this.peekUpper();
    return kw === 'UNION' || kw === 'INTERSECT' || kw === 'EXCEPT';
  }

  private consumeUnionKeyword(): string {
    const kw = this.advance().upper;
    if ((kw === 'UNION' || kw === 'INTERSECT' || kw === 'EXCEPT')
        && (this.peekUpper() === 'ALL' || this.peekUpper() === 'DISTINCT')) {
      const modifier = this.advance().upper;
      return `${kw} ${modifier}`;
    }
    return kw;
  }

  private parseSelectOrParenSelect(): AST.SelectStatement {
    if (this.check('(')) {
      this.advance(); // consume (
      const select = this.parseSelect();
      (select as { parenthesized: boolean }).parenthesized = true;
      this.expect(')');
      return select;
    }
    return this.parseSelect();
  }

  private parseSelect(): AST.SelectStatement {
    this.expect('SELECT');

    let distinct = false;
    if (this.peekUpper() === 'DISTINCT') {
      this.advance();
      distinct = true;
    }

    const columns = this.parseColumnList();
    let from: AST.FromClause | undefined;
    let additionalFromItems: AST.FromClause[] | undefined;
    const joins: AST.JoinClause[] = [];
    let where: AST.WhereClause | undefined;
    let groupBy: AST.GroupByClause | undefined;
    let having: AST.HavingClause | undefined;
    let orderBy: AST.OrderByClause | undefined;
    let limit: AST.LimitClause | undefined;
    let offset: AST.OffsetClause | undefined;
    let fetch: { count: AST.Expression; withTies?: boolean } | undefined;
    let lockingClause: string | undefined;
    let windowClause: { name: string; spec: AST.WindowSpec }[] | undefined;

    if (this.peekUpper() === 'FROM') {
      this.advance();
      from = this.parseFromItem();

      // Parse comma-separated additional FROM items
      const extraFromItems: AST.FromClause[] = [];
      while (this.check(',')) {
        this.advance();
        extraFromItems.push(this.parseFromItem());
      }
      if (extraFromItems.length > 0) additionalFromItems = extraFromItems;

      // Parse JOINs
      while (this.isJoinKeyword()) {
        joins.push(this.parseJoin());
      }
    }

    if (this.peekUpper() === 'WHERE') {
      this.advance();
      const condition = this.parseExpression();
      let whereComment: AST.CommentNode | undefined;
      if (this.peekType() === 'line_comment') {
        const t = this.advance();
        whereComment = { type: 'comment', style: 'line', text: t.value };
      }
      where = { condition, trailingComment: whereComment };
    }

    if (this.peekUpper() === 'GROUP' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      groupBy = this.parseGroupByClause();
    }

    if (this.peekUpper() === 'HAVING') {
      this.advance();
      having = { condition: this.parseExpression() };
    }

    if (this.peekUpper() === 'WINDOW') {
      windowClause = this.parseWindowClause();
    }

    if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      orderBy = { items: this.parseOrderByItems() };
    }

    if (this.peekUpper() === 'LIMIT') {
      this.advance();
      limit = { count: this.parsePrimary() };
    }

    if (this.peekUpper() === 'OFFSET') {
      this.advance();
      const offsetCount = this.parsePrimary();
      let rowsKeyword = false;
      if (this.peekUpper() === 'ROWS') {
        this.advance();
        rowsKeyword = true;
      }
      offset = { count: offsetCount, rowsKeyword };
    }

    if (this.peekUpper() === 'FETCH') {
      fetch = this.parseFetchClause();
    }

    if (this.peekUpper() === 'FOR') {
      lockingClause = this.parseForClause();
    }

    return {
      type: 'select',
      distinct,
      columns,
      from,
      additionalFromItems,
      joins,
      where,
      groupBy,
      having,
      orderBy,
      limit,
      offset,
      fetch,
      lockingClause,
      windowClause,
      leadingComments: [],
    };
  }

  private parseFromItem(): AST.FromClause {
    let lateral = false;
    if (this.peekUpper() === 'LATERAL') {
      this.advance();
      lateral = true;
    }

    const table = this.parseTableExpr();
    let tablesample: AST.FromClause['tablesample'];

    // TABLESAMPLE
    if (this.peekUpper() === 'TABLESAMPLE') {
      this.advance();
      const method = this.advance().upper; // BERNOULLI, SYSTEM, etc.
      this.expect('(');
      const args = this.parseExpressionList();
      this.expect(')');
      let repeatable: AST.Expression | undefined;
      if (this.peekUpper() === 'REPEATABLE') {
        this.advance();
        this.expect('(');
        repeatable = this.parseExpression();
        this.expect(')');
      }
      tablesample = { method, args, repeatable };
    }

    const { alias, aliasColumns } = this.parseOptionalAlias({ allowColumnList: true, stopKeywords: ['TABLESAMPLE'] });

    return { table, alias, aliasColumns, lateral, tablesample };
  }

  // Shared alias parsing for FROM items, JOINs, SELECT columns, and RETURNING.
  // opts.allowColumnList: whether to parse (col1, col2) after alias (FROM/JOIN only)
  // opts.stopKeywords: additional keywords that prevent implicit alias detection
  private parseOptionalAlias(opts: { allowColumnList?: boolean; stopKeywords?: string[] } = {}): { alias?: string; aliasColumns?: string[] } {
    let alias: string | undefined;
    let aliasColumns: string[] | undefined;

    if (this.peekUpper() === 'AS') {
      this.advance();
      alias = this.advance().value;
      if (opts.allowColumnList) aliasColumns = this.tryParseAliasColumnList();
    } else if (
      this.peekType() === 'identifier'
      && !CLAUSE_KEYWORDS.has(this.peekUpper())
      && !this.isJoinKeyword()
      && !this.check(',')
      && !this.check(')')
      && !this.check(';')
      && !(opts.stopKeywords && opts.stopKeywords.includes(this.peekUpper()))
    ) {
      alias = this.advance().value;
      if (opts.allowColumnList) aliasColumns = this.tryParseAliasColumnList();
    }

    return { alias, aliasColumns };
  }

  // Parse an optional parenthesized column alias list: (col1, col2, ...)
  private tryParseAliasColumnList(): string[] | undefined {
    if (!this.check('(')) return undefined;
    this.advance();
    const cols: string[] = [];
    while (!this.check(')') && !this.isAtEnd()) {
      cols.push(this.advance().value);
      if (this.check(',')) this.advance();
    }
    this.expect(')');
    return cols;
  }

  private parseTableExpr(): AST.Expression {
    if (this.check('(')) {
      const subquery = this.tryParseSubqueryAtCurrent();
      if (subquery) {
        return subquery;
      }
      this.advance();
      const expr = this.parseExpression();
      this.expect(')');
      return { type: 'paren', expr } as AST.ParenExpr;
    }
    return this.parsePrimary();
  }

  private tryParseSubqueryAtCurrent(): AST.SubqueryExpr | null {
    if (!this.check('(')) return null;
    return this.tryParse(() => {
      this.advance();
      const query = this.parseQueryExpression();
      this.expect(')');
      return { type: 'subquery', query } as AST.SubqueryExpr;
    });
  }

  private parseSubquery(): AST.SubqueryExpr {
    this.expect('(');
    const query = this.parseQueryExpression();
    this.expect(')');
    return { type: 'subquery', query };
  }

  private isJoinKeyword(): boolean {
    const kw = this.peekUpper();
    if (kw === 'JOIN') return true;
    if (kw === 'INNER' || kw === 'LEFT' || kw === 'RIGHT' || kw === 'FULL' || kw === 'CROSS' || kw === 'NATURAL') {
      const next1 = this.peekUpperAt(1);
      if (next1 === 'JOIN') return true;
      if (next1 === 'OUTER' || next1 === 'INNER') {
        const next2 = this.peekUpperAt(2);
        return next2 === 'JOIN';
      }
      return false;
    }
    return false;
  }

  private parseJoin(): AST.JoinClause {
    let joinType = '';
    while (this.peekUpper() !== 'JOIN') {
      joinType += this.advance().upper + ' ';
    }
    joinType += this.advance().upper; // consume JOIN

    let lateral = false;
    if (this.peekUpper() === 'LATERAL') {
      this.advance();
      lateral = true;
    }

    const table = this.parseTableExpr();
    // ON and USING are already in CLAUSE_KEYWORDS, so no extra stop keywords needed
    const { alias, aliasColumns } = this.parseOptionalAlias({ allowColumnList: true });

    let on: AST.Expression | undefined;
    let usingClause: string[] | undefined;
    let trailingComment: AST.CommentNode | undefined;

    if (this.peekUpper() === 'ON') {
      this.advance();
      on = this.parseExpression();
    } else if (this.peekUpper() === 'USING') {
      this.advance();
      this.expect('(');
      usingClause = [];
      while (!this.check(')') && !this.isAtEnd()) {
        usingClause.push(this.advance().value);
        if (this.check(',')) this.advance();
      }
      this.expect(')');
    }

    if (this.peekType() === 'line_comment') {
      const t = this.advance();
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: t.value,
      };
    }

    return { joinType: joinType.trim(), table, alias, aliasColumns, lateral, on, usingClause, trailingComment };
  }

  private parseGroupByClause(): AST.GroupByClause {
    const items: AST.Expression[] = [];
    const groupingSetsArr: { type: 'grouping_sets' | 'rollup' | 'cube'; sets: AST.Expression[][] }[] = [];

    // Check for GROUPING SETS, ROLLUP, CUBE
    const kw = this.peekUpper();
    if (kw === 'GROUPING' && this.peekUpperAt(1) === 'SETS') {
      groupingSetsArr.push(this.parseGroupingSetsSpec('grouping_sets'));
    } else if (kw === 'ROLLUP') {
      groupingSetsArr.push(this.parseGroupingSetsSpec('rollup'));
    } else if (kw === 'CUBE') {
      groupingSetsArr.push(this.parseGroupingSetsSpec('cube'));
    } else {
      // Normal GROUP BY items
      items.push(this.parseExpression());
      while (this.check(',')) {
        this.advance();
        // Check if next is GROUPING SETS, ROLLUP, or CUBE
        const nextKw = this.peekUpper();
        if ((nextKw === 'GROUPING' && this.peekUpperAt(1) === 'SETS') || nextKw === 'ROLLUP' || nextKw === 'CUBE') {
          const specType = nextKw === 'GROUPING' ? 'grouping_sets' : nextKw.toLowerCase() as 'rollup' | 'cube';
          groupingSetsArr.push(this.parseGroupingSetsSpec(specType));
        } else {
          items.push(this.parseExpression());
        }
      }
    }

    return { items, groupingSets: groupingSetsArr.length > 0 ? groupingSetsArr : undefined };
  }

  private parseGroupingSetsSpec(specType: 'grouping_sets' | 'rollup' | 'cube'): { type: 'grouping_sets' | 'rollup' | 'cube'; sets: AST.Expression[][] } {
    if (specType === 'grouping_sets') {
      this.advance(); // GROUPING
      this.advance(); // SETS
    } else {
      this.advance(); // ROLLUP or CUBE
    }
    this.expect('(');

    const sets: AST.Expression[][] = [];
    while (!this.check(')') && !this.isAtEnd()) {
      if (this.check('(')) {
        this.advance();
        const group: AST.Expression[] = [];
        if (!this.check(')')) {
          group.push(this.parseExpression());
          while (this.check(',')) {
            this.advance();
            group.push(this.parseExpression());
          }
        }
        this.expect(')');
        sets.push(group);
      } else {
        sets.push([this.parseExpression()]);
      }
      if (this.check(',')) this.advance();
    }
    this.expect(')');

    return { type: specType, sets };
  }

  private parseWindowClause(): { name: string; spec: AST.WindowSpec }[] {
    this.advance(); // consume WINDOW
    const defs: { name: string; spec: AST.WindowSpec }[] = [];

    defs.push(this.parseWindowDef());
    while (this.check(',')) {
      this.advance();
      defs.push(this.parseWindowDef());
    }

    return defs;
  }

  private parseWindowDef(): { name: string; spec: AST.WindowSpec } {
    const name = this.advance().value;
    this.expect('AS');
    this.expect('(');
    const spec = this.parseWindowSpec();
    this.expect(')');
    return { name, spec };
  }

  private parseWindowSpec(): AST.WindowSpec {
    let partitionBy: AST.Expression[] | undefined;
    let orderBy: AST.OrderByItem[] | undefined;
    let frame: string | undefined;
    let exclude: string | undefined;

    if (this.peekUpper() === 'PARTITION' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      partitionBy = this.parseExpressionList();
    }

    if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      orderBy = this.parseOrderByItems();
    }

    // Frame clause
    if (this.peekUpper() === 'ROWS' || this.peekUpper() === 'RANGE' || this.peekUpper() === 'GROUPS') {
      let frameStr = this.advance().upper;
      if (this.peekUpper() === 'BETWEEN') {
        frameStr += ' ' + this.advance().upper;
        // Consume frame bound before AND
        while (this.peekUpper() !== 'AND') {
          frameStr += ' ' + this.advance().upper;
        }
        frameStr += ' ' + this.advance().upper; // AND
        // Consume rest of frame until ) or EXCLUDE
        while (!this.check(')') && this.peekUpper() !== 'EXCLUDE') {
          frameStr += ' ' + this.advance().upper;
        }
      } else {
        // e.g., ROWS UNBOUNDED PRECEDING
        while (!this.check(')') && this.peekUpper() !== 'EXCLUDE' && !this.isAtEnd()) {
          frameStr += ' ' + this.advance().upper;
        }
      }
      frame = frameStr;

      // EXCLUDE clause
      if (this.peekUpper() === 'EXCLUDE') {
        this.advance();
        let excludeStr = '';
        // CURRENT ROW, NO OTHERS, GROUP, TIES
        while (!this.check(')') && !this.isAtEnd()) {
          if (excludeStr) excludeStr += ' ';
          excludeStr += this.advance().upper;
        }
        exclude = excludeStr;
      }
    }

    return { partitionBy, orderBy, frame, exclude };
  }

  private parseFetchClause(): { count: AST.Expression; withTies?: boolean } {
    this.advance(); // FETCH
    // FIRST or NEXT
    if (this.peekUpper() === 'FIRST' || this.peekUpper() === 'NEXT') {
      this.advance();
    }
    const count = this.parsePrimary();
    // ROWS or ROW
    if (this.peekUpper() === 'ROWS' || this.peekUpper() === 'ROW') {
      this.advance();
    }
    let withTies = false;
    if (this.peekUpper() === 'ONLY') {
      this.advance();
    } else if (this.peekUpper() === 'WITH') {
      this.advance();
      if (this.peekUpper() === 'TIES') {
        this.advance();
        withTies = true;
      }
    }
    return { count, withTies };
  }

  private parseForClause(): string {
    this.expect('FOR');

    const parts: string[] = [];
    if (this.peekUpper() === 'UPDATE' || this.peekUpper() === 'SHARE') {
      parts.push(this.advance().upper);
    } else if (this.peekUpper() === 'NO' && this.peekUpperAt(1) === 'KEY' && this.peekUpperAt(2) === 'UPDATE') {
      parts.push(this.advance().upper, this.advance().upper, this.advance().upper);
    } else if (this.peekUpper() === 'KEY' && this.peekUpperAt(1) === 'SHARE') {
      parts.push(this.advance().upper, this.advance().upper);
    } else {
      throw new ParseError('UPDATE, SHARE, NO KEY UPDATE, or KEY SHARE', this.peek());
    }

    if (this.peekUpper() === 'OF') {
      this.advance();
      const ofTables: string[] = [this.advance().value];
      while (this.check(',')) {
        this.advance();
        ofTables.push(this.advance().value);
      }
      parts.push(`OF ${ofTables.join(', ')}`);
    }

    if (this.peekUpper() === 'NOWAIT') {
      parts.push(this.advance().upper);
    } else if (this.peekUpper() === 'SKIP' && this.peekUpperAt(1) === 'LOCKED') {
      parts.push(this.advance().upper, this.advance().upper);
    }

    return parts.join(' ');
  }

  private parseOrderByItems(): AST.OrderByItem[] {
    const items: AST.OrderByItem[] = [];
    items.push(this.parseOrderByItem());
    while (this.check(',')) {
      this.advance();
      if (this.peekType() === 'line_comment' && !items[items.length - 1].trailingComment) {
        const t = this.advance();
        const last = items[items.length - 1];
        items[items.length - 1] = { ...last, trailingComment: { type: 'comment', style: 'line', text: t.value } };
      }
      items.push(this.parseOrderByItem());
    }
    return items;
  }

  private parseOrderByItem(): AST.OrderByItem {
    const expr = this.parseExpression();
    let direction: 'ASC' | 'DESC' | undefined;
    if (this.peekUpper() === 'ASC') { this.advance(); direction = 'ASC'; }
    else if (this.peekUpper() === 'DESC') { this.advance(); direction = 'DESC'; }
    let nulls: 'FIRST' | 'LAST' | undefined;
    if (this.peekUpper() === 'NULLS') {
      this.advance();
      if (this.peekUpper() === 'FIRST') {
        this.advance();
        nulls = 'FIRST';
      } else if (this.peekUpper() === 'LAST') {
        this.advance();
        nulls = 'LAST';
      } else {
        throw new ParseError('FIRST or LAST', this.peek());
      }
    }
    let trailingComment: AST.CommentNode | undefined;
    if (this.peekType() === 'line_comment') {
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: this.advance().value,
      };
    }
    return { expr, direction, nulls, trailingComment };
  }

  // Expression parser using precedence climbing
  private parseExpression(): AST.Expression {
    return this.withDepth(() => this.parseOr());
  }

  private parseOr(): AST.Expression {
    let left = this.parseAnd();
    while (this.peekUpper() === 'OR') {
      this.advance();
      const right = this.parseAnd();
      left = { type: 'binary', left, operator: 'OR', right };
    }
    return left;
  }

  private parseAnd(): AST.Expression {
    let left = this.parseNot();
    // BETWEEN consumes its own AND token at comparison precedence, so plain AND
    // here always means boolean conjunction.
    while (this.peekUpper() === 'AND') {
      this.advance();
      const right = this.parseNot();
      left = { type: 'binary', left, operator: 'AND', right };
    }
    return left;
  }

  private parseNot(): AST.Expression {
    if (this.peekUpper() === 'NOT') {
      this.advance();

      if (this.peekUpper() === 'IN' || this.peekUpper() === 'LIKE' || this.peekUpper() === 'ILIKE' || this.peekUpper() === 'BETWEEN' || this.peekUpper() === 'EXISTS' || this.peekUpper() === 'SIMILAR') {
        this.pos--;
        return this.parseComparison();
      }

      const operand = this.parseNot();
      return { type: 'unary', operator: 'NOT', operand };
    }
    return this.parseComparison();
  }

  private parseComparison(): AST.Expression {
    return parseComparisonExpression({
      parseAddSub: () => this.parseAddSub(),
      peekUpper: () => this.peekUpper(),
      peekUpperAt: (offset: number) => this.peekUpperAt(offset),
      advance: () => this.advance(),
      expect: (value: string) => this.expect(value),
      tryParseQueryExpressionAtCurrent: () => this.tryParseQueryExpressionAtCurrent(),
      parseExpressionList: () => this.parseExpressionList(),
      isRegexOperator: () => this.isRegexOperator(),
      checkComparisonOperator: () => this.checkComparisonOperator(),
    });
  }

  private isRegexOperator(): boolean {
    const v = this.peek().value;
    return v === '~' || v === '~*' || v === '!~' || v === '!~*';
  }

  private checkComparisonOperator(): boolean {
    const t = this.peek();
    if (t.type === 'operator') {
      return ['=', '<>', '!=', '<', '>', '<=', '>='].includes(t.value);
    }
    return false;
  }

  private parseAddSub(): AST.Expression {
    let left = this.parseMulDiv();
    while (this.peek().type === 'operator' && (this.peek().value === '+' || this.peek().value === '-')) {
      const op = this.advance().value;
      const right = this.parseMulDiv();
      left = { type: 'binary', left, operator: op, right };
    }
    return left;
  }

  private parseMulDiv(): AST.Expression {
    let left = this.parseJsonOps();
    while (this.peek().type === 'operator' && (this.peek().value === '*' || this.peek().value === '/' || this.peek().value === '||')) {
      const op = this.advance().value;
      const right = this.parseJsonOps();
      left = { type: 'binary', left, operator: op, right };
    }
    return left;
  }

  // JSON, Array, Bitwise operators
  private parseJsonOps(): AST.Expression {
    let left = this.parseUnaryExpr();

    while (true) {
      const t = this.peek();
      if (t.type !== 'operator') break;
      const v = t.value;

      // JSON operators
      if (v === '->' || v === '->>' || v === '#>' || v === '#>>') {
        this.advance();
        const right = this.parseUnaryExpr();
        left = { type: 'binary', left, operator: v, right };
        continue;
      }

      // Array / JSONB containment
      if (v === '@>' || v === '<@' || v === '&&') {
        this.advance();
        const right = this.parseUnaryExpr();
        left = { type: 'binary', left, operator: v, right };
        continue;
      }

      // JSONB existence/path operators
      if (v === '?' || v === '?|' || v === '?&' || v === '@?' || v === '@@') {
        this.advance();
        const right = this.parseUnaryExpr();
        left = { type: 'binary', left, operator: v, right };
        continue;
      }

      // Bitwise operators
      if (v === '&' || v === '|' || v === '#' || v === '<<' || v === '>>') {
        this.advance();
        const right = this.parseUnaryExpr();
        left = { type: 'binary', left, operator: v, right };
        continue;
      }

      break;
    }

    return left;
  }

  private parseUnaryExpr(): AST.Expression {
    if (this.peek().type === 'operator' && this.peek().value === '-') {
      this.advance();
      const operand = this.parsePrimary();
      return { type: 'unary', operator: '-', operand };
    }
    // ~ as unary prefix (bitwise NOT)
    if (this.peek().type === 'operator' && this.peek().value === '~') {
      this.advance();
      const operand = this.parsePrimary();
      return { type: 'unary', operator: '~', operand };
    }
    return this.parsePrimaryWithPostfix();
  }

  // Parse primary then handle postfix :: cast
  private parsePrimaryWithPostfix(): AST.Expression {
    let expr = this.parsePrimary();

    // Handle :: casts (can be chained)
    while (this.peek().type === 'operator' && this.peek().value === '::') {
      this.advance();
      const targetType = this.consumeTypeSpecifier();
      expr = { type: 'pg_cast', expr, targetType } as AST.PgCastExpr;
    }

    // Handle array subscript: expr[idx] or expr[lo:hi]
    while (this.check('[')) {
      this.advance(); // consume [
      if (this.check(':')) {
        this.advance();
        const upper = this.check(']') ? undefined : this.parseExpression();
        this.expect(']');
        expr = {
          type: 'array_subscript',
          array: expr,
          isSlice: true,
          upper,
        } as AST.ArraySubscriptExpr;
      } else {
        const lower = this.parseExpression();
        if (this.check(':')) {
          this.advance();
          const upper = this.check(']') ? undefined : this.parseExpression();
          this.expect(']');
          expr = {
            type: 'array_subscript',
            array: expr,
            isSlice: true,
            lower,
            upper,
          } as AST.ArraySubscriptExpr;
          continue;
        }
        this.expect(']');
        expr = {
          type: 'array_subscript',
          array: expr,
          isSlice: false,
          lower,
        } as AST.ArraySubscriptExpr;
      }
    }

    return expr;
  }

  private parsePrimary(): AST.Expression {
    return parsePrimaryExpression({
      peek: () => this.peek(),
      peekUpperAt: (offset: number) => this.peekUpperAt(offset),
      peekAt: (offset: number) => this.peekAt(offset),
      peekTypeAt: (offset: number) => this.peekTypeAt(offset),
      advance: () => this.advance(),
      check: (value: string) => this.check(value),
      expect: (value: string) => this.expect(value),
      parseSubquery: () => this.parseSubquery(),
      tryParseSubqueryAtCurrent: () => this.tryParseSubqueryAtCurrent(),
      parseExpression: () => this.parseExpression(),
      parseCaseExpr: () => this.parseCaseExpr(),
      parseCast: () => this.parseCast(),
      parseExtract: () => this.parseExtract(),
      parsePositionExpr: () => this.parsePositionExpr(),
      parseSubstringExpr: () => this.parseSubstringExpr(),
      parseOverlayExpr: () => this.parseOverlayExpr(),
      parseTrimExpr: () => this.parseTrimExpr(),
      parseIdentifierOrFunction: () => this.parseIdentifierOrFunction(),
    });
  }

  private parseIdentifierOrFunction(): AST.Expression {
    const name = this.advance();
    let fullName = name.value;
    const quoted = name.value.startsWith('"');

    // Qualified name: a.b or a.*
    while (this.check('.')) {
      this.advance(); // consume .
      if (this.check('*')) {
        this.advance();
        return { type: 'star', qualifier: fullName };
      }
      const next = this.advance();
      fullName += '.' + next.value;
    }

    // Function call
    if (this.check('(') && !CLAUSE_KEYWORDS.has(name.upper)) {
      this.advance(); // consume (

      let distinct = false;
      if (this.peekUpper() === 'DISTINCT') {
        this.advance();
        distinct = true;
      }

      const args: AST.Expression[] = [];
      if (!this.check(')')) {
        if (this.check('*')) {
          this.advance();
          args.push({ type: 'star' });
        } else {
          args.push(this.parseExpression());
          while (this.check(',')) {
            this.advance();
            args.push(this.parseExpression());
          }
        }
      }

      // Handle ORDER BY inside aggregate: e.g., ARRAY_AGG(x ORDER BY y), STRING_AGG(x, ',' ORDER BY y)
      let innerOrderBy: AST.OrderByItem[] | undefined;
      if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
        this.advance(); this.advance();
        innerOrderBy = this.parseOrderByItems();
      }

      this.expect(')');

      // FILTER (WHERE ...)
      let filter: AST.Expression | undefined;
      if (this.peekUpper() === 'FILTER') {
        this.advance();
        this.expect('(');
        this.expect('WHERE');
        filter = this.parseExpression();
        this.expect(')');
      }

      // WITHIN GROUP (ORDER BY ...)
      let withinGroup: { orderBy: AST.OrderByItem[] } | undefined;
      if (this.peekUpper() === 'WITHIN') {
        this.advance();
        this.expect('GROUP');
        this.expect('(');
        this.expect('ORDER');
        this.expect('BY');
        const withinOrderBy = this.parseOrderByItems();
        this.expect(')');
        withinGroup = { orderBy: withinOrderBy };
      }

      const funcExpr: AST.FunctionCallExpr = {
        type: 'function_call',
        name: fullName,
        args,
        distinct,
        orderBy: innerOrderBy,
        filter,
        withinGroup,
      };

      // Window function: OVER (...)
      if (this.peekUpper() === 'OVER') {
        return this.parseWindowFunction(funcExpr);
      }

      return funcExpr;
    }

    return { type: 'identifier', value: fullName, quoted };
  }

  private parseWindowFunction(func: AST.FunctionCallExpr): AST.WindowFunctionExpr {
    this.expect('OVER');

    // Check for named window reference: OVER window_name
    if (!this.check('(')) {
      const windowName = this.advance().value;
      return { type: 'window_function', func, windowName };
    }

    this.expect('(');
    const spec = this.parseWindowSpec();
    this.expect(')');

    return {
      type: 'window_function',
      func,
      partitionBy: spec.partitionBy,
      orderBy: spec.orderBy,
      frame: spec.frame,
      exclude: spec.exclude,
    };
  }

  private parseCaseExpr(): AST.CaseExpr {
    this.expect('CASE');

    let operand: AST.Expression | undefined;
    if (this.peekUpper() !== 'WHEN') {
      operand = this.parseExpression();
    }

    const whenClauses: { condition: AST.Expression; result: AST.Expression }[] = [];
    while (this.peekUpper() === 'WHEN') {
      this.advance();
      const condition = this.parseExpression();
      this.expect('THEN');
      const result = this.parseExpression();
      whenClauses.push({ condition, result });
    }

    let elseResult: AST.Expression | undefined;
    if (this.peekUpper() === 'ELSE') {
      this.advance();
      elseResult = this.parseExpression();
    }

    this.expect('END');
    return { type: 'case', operand, whenClauses, elseResult };
  }

  private parseCast(): AST.CastExpr {
    this.expect('CAST');
    this.expect('(');
    const expr = this.parseExpression();
    this.expect('AS');
    const targetType = this.consumeTypeSpecifier();
    this.expect(')');
    return { type: 'cast', expr, targetType };
  }

  private parseExtract(): AST.ExtractExpr {
    this.expect('EXTRACT');
    this.expect('(');
    const field = this.advance().upper;
    this.expect('FROM');
    const source = this.parseExpression();
    this.expect(')');
    return { type: 'extract', field, source };
  }

  private parsePositionExpr(): AST.Expression {
    this.advance(); // POSITION
    this.expect('(');
    const substr = this.parseAddSub();
    this.expect('IN');
    const str = this.parseAddSub();
    this.expect(')');
    return { type: 'position', substring: substr, source: str } as AST.PositionExpr;
  }

  private parseSubstringExpr(): AST.Expression {
    this.advance(); // SUBSTRING
    this.expect('(');
    const str = this.parseExpression();
    this.expect('FROM');
    const start = this.parseExpression();
    let len: AST.Expression | undefined;
    if (this.peekUpper() === 'FOR') {
      this.advance();
      len = this.parseExpression();
    }
    this.expect(')');
    return { type: 'substring', source: str, start, length: len } as AST.SubstringExpr;
  }

  private parseOverlayExpr(): AST.Expression {
    this.advance(); // OVERLAY
    this.expect('(');
    const str = this.parseExpression();
    this.expect('PLACING');
    const replacement = this.parseExpression();
    this.expect('FROM');
    const start = this.parseExpression();
    let len: AST.Expression | undefined;
    if (this.peekUpper() === 'FOR') {
      this.advance();
      len = this.parseExpression();
    }
    this.expect(')');
    return {
      type: 'overlay',
      source: str,
      replacement,
      start,
      length: len,
    } as AST.OverlayExpr;
  }

  private parseTrimExpr(): AST.Expression {
    this.advance(); // TRIM
    this.expect('(');

    let side: AST.TrimExpr['side'];
    if (this.peekUpper() === 'LEADING' || this.peekUpper() === 'TRAILING' || this.peekUpper() === 'BOTH') {
      side = this.advance().upper as AST.TrimExpr['side'];
    }

    let trimChar: AST.Expression | undefined;
    let str: AST.Expression;
    let fromSyntax = false;

    // TRIM([LEADING|TRAILING|BOTH] [trim_char] FROM str)
    if (this.peekUpper() === 'FROM') {
      fromSyntax = true;
      this.advance();
      str = this.parseExpression();
    } else {
      const firstExpr = this.parseExpression();
      if (this.peekUpper() === 'FROM') {
        trimChar = firstExpr;
        fromSyntax = true;
        this.advance();
        str = this.parseExpression();
      } else {
        // TRIM(str) (common shorthand)
        str = firstExpr;
      }
    }

    this.expect(')');

    return { type: 'trim', side, trimChar, source: str, fromSyntax } as AST.TrimExpr;
  }

  private parseExpressionList(): AST.Expression[] {
    const list: AST.Expression[] = [];
    list.push(this.parseExpression());
    while (this.check(',')) {
      this.advance();
      list.push(this.parseExpression());
    }
    return list;
  }

  private parseReturningList(): AST.Expression[] {
    const list: AST.Expression[] = [];
    list.push(this.parseReturningItem());
    while (this.check(',')) {
      this.advance();
      list.push(this.parseReturningItem());
    }
    return list;
  }

  private parseReturningItem(): AST.Expression {
    const expr = this.parseExpression();
    const { alias } = this.parseOptionalAlias();
    if (alias) {
      return { type: 'aliased', expr, alias } as AST.AliasedExpr;
    }
    return expr;
  }

  private parseColumnList(): AST.ColumnExpr[] {
    const columns: AST.ColumnExpr[] = [];
    columns.push(this.parseColumnExpr());

    while (this.check(',')) {
      this.advance();
      if (this.peekType() === 'line_comment' && !columns[columns.length - 1].trailingComment) {
        const t = this.advance();
        const last = columns[columns.length - 1];
        columns[columns.length - 1] = { ...last, trailingComment: { type: 'comment', style: 'line', text: t.value } };
      }
      columns.push(this.parseColumnExpr());
    }

    return columns;
  }

  private parseColumnExpr(): AST.ColumnExpr {
    const expr = this.parseExpression();
    let trailingComment: AST.CommentNode | undefined;

    const { alias } = this.parseOptionalAlias();

    if (this.peekType() === 'line_comment') {
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: this.advance().value,
      };
    }

    return { expr, alias, trailingComment };
  }

  private createDmlContext(): DmlParser {
    return {
      expect: (value: string) => this.expect(value),
      advance: () => this.advance(),
      check: (value: string) => this.check(value),
      peekUpper: () => this.peekUpper(),
      peekUpperAt: (offset: number) => this.peekUpperAt(offset),
      isAtEnd: () => this.isAtEnd(),
      parseExpression: () => this.parseExpression(),
      parseExpressionList: () => this.parseExpressionList(),
      parseReturningList: () => this.parseReturningList(),
      parseFromItem: () => this.parseFromItem(),
      parseQueryExpression: () => this.parseQueryExpression(),
    };
  }

  // INSERT INTO table (cols) VALUES (...), (...) | SELECT ...
  private parseInsert(comments: AST.CommentNode[]): AST.InsertStatement {
    return parseInsertStatement(this.createDmlContext(), comments);
  }

  // UPDATE table SET col = val, ... [FROM ...] WHERE ...
  private parseUpdate(comments: AST.CommentNode[]): AST.UpdateStatement {
    return parseUpdateStatement(this.createDmlContext(), comments);
  }

  private parseSetItem(): AST.SetItem {
    return parseDmlSetItem(this.createDmlContext());
  }

  // DELETE FROM table WHERE ...
  private parseDelete(comments: AST.CommentNode[]): AST.DeleteStatement {
    return parseDeleteStatement(this.createDmlContext(), comments);
  }

  // CREATE TABLE | CREATE INDEX | CREATE VIEW
  private parseCreate(comments: AST.CommentNode[]): AST.Node {
    this.advance(); // CREATE

    // OR REPLACE
    let orReplace = false;
    if (this.peekUpper() === 'OR' && this.peekUpperAt(1) === 'REPLACE') {
      this.advance(); this.advance();
      orReplace = true;
    }

    // UNIQUE (for CREATE UNIQUE INDEX)
    let unique = false;
    if (this.peekUpper() === 'UNIQUE') {
      this.advance();
      unique = true;
    }

    // MATERIALIZED
    let materialized = false;
    if (this.peekUpper() === 'MATERIALIZED') {
      this.advance();
      materialized = true;
    }

    const kw = this.peekUpper();

    if (kw === 'TABLE') {
      return this.parseCreateTable(comments);
    }

    if (kw === 'INDEX') {
      return this.parseCreateIndex(comments, unique);
    }

    if (kw === 'VIEW') {
      return this.parseCreateView(comments, orReplace, materialized);
    }

    // Fallback - consume as raw
    let raw = 'CREATE';
    if (orReplace) raw += ' OR REPLACE';
    if (unique) raw += ' UNIQUE';
    if (materialized) raw += ' MATERIALIZED';
    while (!this.isAtEnd() && !this.check(';')) {
      raw += ' ' + this.advance().value;
    }
    return { type: 'raw', text: raw } as AST.RawExpression;
  }

  private parseCreateTable(comments: AST.CommentNode[]): AST.CreateTableStatement {
    this.expect('TABLE');

    let ifNotExists = false;
    if (this.peekUpper() === 'IF' && this.peekUpperAt(1) === 'NOT' && this.peekUpperAt(2) === 'EXISTS') {
      this.advance(); this.advance(); this.advance();
      ifNotExists = true;
    }

    const tableName = this.advance().value;
    const fullName = ifNotExists ? 'IF NOT EXISTS ' + tableName : tableName;

    this.expect('(');
    const elements = this.parseTableElements();
    this.expect(')');

    return { type: 'create_table', tableName: fullName, elements, leadingComments: comments };
  }

  private parseCreateIndex(comments: AST.CommentNode[], unique: boolean): AST.CreateIndexStatement {
    this.advance(); // INDEX

    let concurrently = false;
    if (this.peekUpper() === 'CONCURRENTLY') {
      this.advance();
      concurrently = true;
    }

    let ifNotExists = false;
    if (this.peekUpper() === 'IF' && this.peekUpperAt(1) === 'NOT' && this.peekUpperAt(2) === 'EXISTS') {
      this.advance(); this.advance(); this.advance();
      ifNotExists = true;
    }

    const name = this.advance().value;

    this.expect('ON');
    const table = this.advance().value;

    let using: string | undefined;
    if (this.peekUpper() === 'USING') {
      this.advance();
      using = this.advance().upper;
    }

    // Column list
    this.expect('(');
    const columns: AST.Expression[] = [];
    columns.push(this.parseIndexColumn());
    while (this.check(',')) {
      this.advance();
      columns.push(this.parseIndexColumn());
    }
    this.expect(')');

    let where: AST.Expression | undefined;
    if (this.peekUpper() === 'WHERE') {
      this.advance();
      where = this.parseExpression();
    }

    return { type: 'create_index', unique, concurrently, ifNotExists, name, table, using, columns, where, leadingComments: comments };
  }

  private parseIndexColumn(): AST.Expression {
    const expr = this.parseExpression();
    if (this.peekUpper() === 'ASC' || this.peekUpper() === 'DESC') {
      const dir = this.advance().upper as 'ASC' | 'DESC';
      return { type: 'ordered_expr', expr, direction: dir } as AST.OrderedExpr;
    }
    return expr;
  }

  private parseCreateView(comments: AST.CommentNode[], orReplace: boolean, materialized: boolean): AST.CreateViewStatement {
    this.advance(); // VIEW

    let ifNotExists = false;
    if (this.peekUpper() === 'IF' && this.peekUpperAt(1) === 'NOT' && this.peekUpperAt(2) === 'EXISTS') {
      this.advance(); this.advance(); this.advance();
      ifNotExists = true;
    }

    const name = this.advance().value;

    this.expect('AS');

    // Parse the query — must be a SELECT, UNION, or CTE
    const query = this.parseStatement();
    if (query && query.type !== 'select' && query.type !== 'union' && query.type !== 'cte') {
      throw new ParseError('SELECT, UNION, or WITH query in CREATE VIEW', this.peek());
    }

    let withData: boolean | undefined;
    if (this.peekUpper() === 'WITH') {
      this.advance();
      if (this.peekUpper() === 'DATA') {
        this.advance();
        withData = true;
      } else if (this.peekUpper() === 'NO' && this.peekUpperAt(1) === 'DATA') {
        this.advance(); this.advance();
        withData = false;
      }
    }

    return { type: 'create_view', orReplace, materialized, ifNotExists, name, query: query as AST.Statement, withData, leadingComments: comments };
  }

  private parseMerge(comments: AST.CommentNode[]): AST.MergeStatement {
    this.advance(); // MERGE
    this.expect('INTO');
    const targetTable = this.advance().value;
    let targetAlias: string | undefined;
    if (this.peekUpper() === 'AS') {
      this.advance();
      targetAlias = this.advance().value;
    } else if (this.peekType() === 'identifier' && this.peekUpper() !== 'USING') {
      targetAlias = this.advance().value;
    }

    this.expect('USING');
    const sourceTable = this.advance().value;
    let sourceAlias: string | undefined;
    if (this.peekUpper() === 'AS') {
      this.advance();
      sourceAlias = this.advance().value;
    } else if (this.peekType() === 'identifier' && this.peekUpper() !== 'ON') {
      sourceAlias = this.advance().value;
    }

    this.expect('ON');
    const onExpr = this.parseExpression();

    const whenClauses: AST.MergeWhenClause[] = [];
    while (this.peekUpper() === 'WHEN') {
      this.advance(); // WHEN

      let matched = true;
      if (this.peekUpper() === 'NOT') {
        this.advance();
        matched = false;
      }
      this.expect('MATCHED');

      let condition: AST.Expression | undefined;
      if (this.peekUpper() === 'AND') {
        this.advance();
        condition = this.parseExpression();
      }

      this.expect('THEN');

      const actionKw = this.peekUpper();
      if (actionKw === 'DELETE') {
        this.advance();
        whenClauses.push({ matched, condition, action: 'delete' });
      } else if (actionKw === 'UPDATE') {
        this.advance();
        this.expect('SET');
        const setItems: { column: string; value: AST.Expression }[] = [];
        setItems.push(this.parseSetItem());
        while (this.check(',')) {
          this.advance();
          setItems.push(this.parseSetItem());
        }
        whenClauses.push({ matched, condition, action: 'update', setItems });
      } else if (actionKw === 'INSERT') {
        this.advance();
        let insertCols: string[] | undefined;
        if (this.check('(')) {
          this.advance();
          insertCols = [];
          while (!this.check(')') && !this.isAtEnd()) {
            insertCols.push(this.advance().value);
            if (this.check(',')) this.advance();
          }
          this.expect(')');
        }
        this.expect('VALUES');
        this.expect('(');
        const insertVals = this.parseExpressionList();
        this.expect(')');
        whenClauses.push({ matched, condition, action: 'insert', columns: insertCols, values: insertVals });
      }
    }

    return {
      type: 'merge',
      target: { table: targetTable, alias: targetAlias },
      source: { table: sourceTable, alias: sourceAlias },
      on: onExpr,
      whenClauses,
      leadingComments: comments,
    };
  }

  private parseGrant(comments: AST.CommentNode[]): AST.GrantStatement {
    const kind = this.advance().upper as 'GRANT' | 'REVOKE';
    let grantOptionFor = false;
    if (
      kind === 'REVOKE'
      && this.peekUpper() === 'GRANT'
      && this.peekUpperAt(1) === 'OPTION'
      && this.peekUpperAt(2) === 'FOR'
    ) {
      this.advance();
      this.advance();
      this.advance();
      grantOptionFor = true;
    }

    const privilegeTokens = this.collectTokensUntilTopLevelKeyword(new Set(['ON']));
    this.expect('ON');
    const recipientKeyword = kind === 'GRANT' ? 'TO' : 'FROM';
    const objectTokens = this.collectTokensUntilTopLevelKeyword(new Set([recipientKeyword]));
    this.expect(recipientKeyword);

    const recipientTokens: Token[] = [];
    while (!this.isAtEnd() && !this.check(';')) {
      if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'GRANT' && this.peekUpperAt(2) === 'OPTION') break;
      if (this.peekUpper() === 'GRANTED' && this.peekUpperAt(1) === 'BY') break;
      if (this.peekUpper() === 'CASCADE' || this.peekUpper() === 'RESTRICT') break;
      recipientTokens.push(this.advance());
    }

    let withGrantOption = false;
    let grantedBy: string | undefined;
    let cascade = false;
    let restrict = false;
    while (!this.isAtEnd() && !this.check(';')) {
      if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'GRANT' && this.peekUpperAt(2) === 'OPTION') {
        this.advance();
        this.advance();
        this.advance();
        withGrantOption = true;
        continue;
      }
      if (this.peekUpper() === 'GRANTED' && this.peekUpperAt(1) === 'BY') {
        this.advance();
        this.advance();
        const grantedByTokens = this.collectTokensUntilTopLevelKeyword(new Set(['CASCADE', 'RESTRICT']));
        grantedBy = this.tokensToSql(grantedByTokens);
        continue;
      }
      if (this.peekUpper() === 'CASCADE') {
        this.advance();
        cascade = true;
        continue;
      }
      if (this.peekUpper() === 'RESTRICT') {
        this.advance();
        restrict = true;
        continue;
      }
      break;
    }

    const privileges = this.splitTopLevelByComma(privilegeTokens).map(toks => this.tokensToSql(toks)).filter(Boolean);
    const object = this.tokensToSql(objectTokens);
    const recipients = this.splitTopLevelByComma(recipientTokens).map(toks => this.tokensToSql(toks)).filter(Boolean);

    return {
      type: 'grant',
      kind,
      grantOptionFor: grantOptionFor || undefined,
      privileges,
      object,
      recipientKeyword: recipientKeyword as 'TO' | 'FROM',
      recipients,
      withGrantOption: withGrantOption || undefined,
      grantedBy,
      cascade: cascade || undefined,
      restrict: restrict || undefined,
      leadingComments: comments,
    };
  }

  private parseTruncate(comments: AST.CommentNode[]): AST.TruncateStatement {
    this.advance(); // TRUNCATE
    let tableKeyword = false;
    if (this.peekUpper() === 'TABLE') {
      this.advance();
      tableKeyword = true;
    }

    const tables: string[] = [];
    tables.push(this.advance().value);
    while (this.check(',')) {
      this.advance();
      tables.push(this.advance().value);
    }

    let restartIdentity = false;
    let cascade = false;

    if (this.peekUpper() === 'RESTART') {
      this.advance();
      this.expect('IDENTITY');
      restartIdentity = true;
    }
    if (this.peekUpper() === 'CASCADE') {
      this.advance();
      cascade = true;
    }

    return { type: 'truncate', table: tables.join(', '), tableKeyword, restartIdentity, cascade, leadingComments: comments };
  }

  private parseStandaloneValues(comments: AST.CommentNode[]): AST.StandaloneValuesStatement {
    this.advance(); // VALUES
    const rows: AST.ValuesRow[] = [];

    this.expect('(');
    const firstVals = this.parseExpressionList();
    this.expect(')');
    rows.push({ values: firstVals });

    while (this.check(',')) {
      this.advance();
      this.expect('(');
      const vals = this.parseExpressionList();
      this.expect(')');
      rows.push({ values: vals });
    }

    return { type: 'standalone_values', rows, leadingComments: comments };
  }

  private parseTableElements(): AST.TableElement[] {
    const elements: AST.TableElement[] = [];

    while (!this.check(')')) {
      const elem = this.parseTableElement();
      elements.push(elem);
      if (this.check(',')) this.advance();
    }

    return elements;
  }

  private parseTableElement(): AST.TableElement {
    if (this.peekUpper() === 'PRIMARY' && this.peekUpperAt(1) === 'KEY') {
      this.advance(); this.advance();
      let raw = 'PRIMARY KEY';
      this.expect('(');
      raw += ' (';
      const cols: string[] = [];
      while (!this.check(')')) {
        cols.push(this.advance().value);
        if (this.check(',')) { this.advance(); }
      }
      this.expect(')');
      raw += cols.join(', ') + ')';
      return { elementType: 'primary_key', raw, name: cols.join(', ') };
    }

    if (this.peekUpper() === 'CONSTRAINT') {
      this.advance();
      const constraintName = this.advance().value;

      if (this.peekUpper() === 'CHECK') {
        this.advance();
        this.expect('(');
        const checkExpr = this.parseExpression();
        this.expect(')');
        const body = 'CHECK';
        return {
          elementType: 'constraint',
          raw: `CONSTRAINT ${constraintName} ${body}`,
          constraintName,
          constraintBody: body,
          constraintType: 'check',
          checkExpr,
        };
      }

      if (this.peekUpper() === 'FOREIGN' && this.peekUpperAt(1) === 'KEY') {
        this.advance(); this.advance();
        this.expect('(');
        const fkCols: string[] = [];
        while (!this.check(')')) {
          fkCols.push(this.advance().value);
          if (this.check(',')) this.advance();
        }
        this.expect(')');

        this.expect('REFERENCES');
        const refTable = this.advance().value;
        this.expect('(');
        const refCols: string[] = [];
        while (!this.check(')')) {
          refCols.push(this.advance().value);
          if (this.check(',')) this.advance();
        }
        this.expect(')');

        let actions = '';
        while (this.peekUpper() === 'ON') {
          this.advance();
          const actionType = this.advance().upper;
          const actionValue = this.advance().upper;
          actions += `ON ${actionType} ${actionValue}`;
          if (this.peekUpper() === 'NULL' || this.peekUpper() === 'DEFAULT') {
            actions += ' ' + this.advance().upper;
          }
          if (this.peekUpper() === 'ON') actions += '\n        ';
        }

        return {
          elementType: 'foreign_key',
          raw: `CONSTRAINT ${constraintName} FOREIGN KEY (${fkCols.join(', ')}) REFERENCES ${refTable} (${refCols.join(', ')})${actions ? ' ' + actions : ''}`,
          constraintName,
          fkColumns: fkCols.join(', '),
          fkRefTable: refTable,
          fkRefColumns: refCols.join(', '),
          fkActions: actions || undefined,
        };
      }

      let raw = `CONSTRAINT ${constraintName}`;
      while (!this.check(',') && !this.check(')') && !this.isAtEnd()) {
        raw += ' ' + this.advance().value;
      }
      return { elementType: 'constraint', raw, constraintName };
    }

    const colName = this.advance().value;
    let dataType = this.advance().upper;
    if (this.check('(')) {
      dataType += this.advance().value;
      while (!this.check(')')) {
        const t = this.advance();
        dataType += t.value;
        if (this.check(',')) {
          dataType += this.advance().value;
          dataType += ' ';
        }
      }
      dataType += this.advance().value; // )
    }

    let constraints = '';
    while (!this.check(',') && !this.check(')') && !this.isAtEnd()) {
      const kw = this.peekUpper();
      if (kw === 'CONSTRAINT') break;
      constraints += (constraints ? ' ' : '') + this.advance().upper;
    }

    return {
      elementType: 'column',
      raw: `${colName} ${dataType}${constraints ? ' ' + constraints : ''}`,
      name: colName,
      dataType,
      constraints: constraints || undefined,
    };
  }

  // ALTER TABLE
  private parseAlter(comments: AST.CommentNode[]): AST.AlterTableStatement {
    this.expect('ALTER');
    const objectTypeToken = this.advance();
    if (objectTypeToken.type !== 'keyword' && objectTypeToken.type !== 'identifier') {
      throw new ParseError('object type', objectTypeToken);
    }
    const objectType = objectTypeToken.upper;
    const objectName = this.advance().value;
    const actions: AST.AlterAction[] = [];
    while (!this.check(';') && !this.isAtEnd()) {
      actions.push(this.parseAlterAction());
      if (this.check(',')) {
        this.advance();
      } else {
        break;
      }
    }
    return {
      type: 'alter_table',
      objectType,
      objectName,
      actions,
      leadingComments: comments,
    };
  }

  private parseAlterAction(): AST.AlterAction {
    const start = this.pos;
    const rename = this.tryParseAlterRenameAction();
    if (rename) return rename;
    this.pos = start;

    const addColumn = this.tryParseAlterAddColumnAction();
    if (addColumn) return addColumn;
    this.pos = start;

    const dropColumn = this.tryParseAlterDropColumnAction();
    if (dropColumn) return dropColumn;
    this.pos = start;

    const setSchema = this.tryParseAlterSetSchemaAction();
    if (setSchema) return setSchema;
    this.pos = start;

    const setTablespace = this.tryParseAlterSetTablespaceAction();
    if (setTablespace) return setTablespace;
    this.pos = start;

    return this.parseRawAlterAction();
  }

  private tryParseAlterRenameAction(): AST.AlterAction | null {
    if (this.peekUpper() !== 'RENAME') return null;
    this.advance();
    if (this.peekUpper() === 'TO') {
      this.advance();
      const newName = this.advance().value;
      return { type: 'rename_to', newName };
    }
    if (this.peekUpper() === 'COLUMN') {
      this.advance();
      const columnName = this.advance().value;
      this.expect('TO');
      const newName = this.advance().value;
      return { type: 'rename_column', columnName, newName };
    }
    return null;
  }

  private tryParseAlterAddColumnAction(): AST.AlterAction | null {
    if (this.peekUpper() !== 'ADD') return null;
    this.advance();
    if (this.peekUpper() === 'CONSTRAINT') return null;

    if (this.peekUpper() === 'COLUMN') this.advance();

    let ifNotExists = false;
    if (this.peekUpper() === 'IF' && this.peekUpperAt(1) === 'NOT' && this.peekUpperAt(2) === 'EXISTS') {
      this.advance();
      this.advance();
      this.advance();
      ifNotExists = true;
    }

    if (this.peekType() !== 'identifier' && this.peekType() !== 'keyword') return null;
    const columnName = this.advance().value;
    const definitionTokens = this.consumeTokensUntilActionBoundary();
    const definition = this.tokensToSql(definitionTokens);
    return {
      type: 'add_column',
      ifNotExists: ifNotExists || undefined,
      columnName,
      definition: definition || undefined,
    };
  }

  private tryParseAlterDropColumnAction(): AST.AlterAction | null {
    if (this.peekUpper() !== 'DROP') return null;
    this.advance();

    if (this.peekUpper() === 'COLUMN') this.advance();

    let ifExists = false;
    if (this.peekUpper() === 'IF' && this.peekUpperAt(1) === 'EXISTS') {
      this.advance();
      this.advance();
      ifExists = true;
    }

    if (this.peekType() !== 'identifier' && this.peekType() !== 'keyword') return null;
    const columnName = this.advance().value;

    let behavior: 'CASCADE' | 'RESTRICT' | undefined;
    if (this.peekUpper() === 'CASCADE') {
      this.advance();
      behavior = 'CASCADE';
    } else if (this.peekUpper() === 'RESTRICT') {
      this.advance();
      behavior = 'RESTRICT';
    }

    return { type: 'drop_column', ifExists: ifExists || undefined, columnName, behavior };
  }

  private tryParseAlterSetSchemaAction(): AST.AlterAction | null {
    if (this.peekUpper() !== 'SET' || this.peekUpperAt(1) !== 'SCHEMA') return null;
    this.advance();
    this.advance();
    const schema = this.advance().value;
    return { type: 'set_schema', schema };
  }

  private tryParseAlterSetTablespaceAction(): AST.AlterAction | null {
    if (this.peekUpper() !== 'SET' || this.peekUpperAt(1) !== 'TABLESPACE') return null;
    this.advance();
    this.advance();
    const tablespace = this.advance().value;
    return { type: 'set_tablespace', tablespace };
  }

  private parseRawAlterAction(): AST.AlterRawAction {
    const tokens = this.consumeTokensUntilActionBoundary();
    return {
      type: 'raw',
      text: this.tokensToSql(tokens),
    };
  }

  // DROP TABLE [IF EXISTS] name
  private parseDrop(comments: AST.CommentNode[]): AST.DropTableStatement {
    this.expect('DROP');
    const objectTypeToken = this.advance();
    if (objectTypeToken.type !== 'keyword' && objectTypeToken.type !== 'identifier') {
      throw new ParseError('object type', objectTypeToken);
    }
    const objectType = objectTypeToken.upper;

    let ifExists = false;
    if (this.peekUpper() === 'IF' && this.peekUpperAt(1) === 'EXISTS') {
      this.advance(); this.advance();
      ifExists = true;
    }

    const objectName = this.advance().value;

    return {
      type: 'drop_table',
      objectType,
      ifExists,
      objectName,
      leadingComments: comments,
    };
  }

  // CTE: WITH [RECURSIVE] name AS (...), name AS (...) SELECT ...
  private parseCTE(comments: AST.CommentNode[]): AST.CTEStatement {
    this.expect('WITH');

    let recursive = false;
    if (this.peekUpper() === 'RECURSIVE') {
      this.advance();
      recursive = true;
    }

    const ctes: AST.CTEDefinition[] = [];

    ctes.push(this.parseCTEDefinition());
    while (this.check(',')) {
      this.advance();
      ctes.push(this.parseCTEDefinition());
    }

    // Parse the main query
    let mainQuery: AST.SelectStatement | AST.UnionStatement;
    const mainComments = this.consumeComments();

    if (this.check('(')) {
      const result = this.tryParseQueryExpressionAtCurrent(mainComments);
      if (result && (result.type === 'select' || result.type === 'union')) {
        mainQuery = result;
      } else {
        const first = this.parseSelect();
        (first as any).leadingComments = mainComments;

        if (this.checkUnionKeyword()) {
          const members: { statement: AST.SelectStatement; parenthesized: boolean }[] = [
            { statement: first, parenthesized: false }
          ];
          const operators: string[] = [];
          while (this.checkUnionKeyword()) {
            operators.push(this.consumeUnionKeyword());
            const next = this.parseSelectOrParenSelect();
            members.push({ statement: next, parenthesized: next.parenthesized || false });
          }
          mainQuery = { type: 'union', members, operators, leadingComments: mainComments };
        } else {
          mainQuery = first;
        }
      }
    } else {
      const first = this.parseSelect();
      (first as any).leadingComments = mainComments;

      if (this.checkUnionKeyword()) {
        const members: { statement: AST.SelectStatement; parenthesized: boolean }[] = [
          { statement: first, parenthesized: false }
        ];
        const operators: string[] = [];
        while (this.checkUnionKeyword()) {
          operators.push(this.consumeUnionKeyword());
          const next = this.parseSelectOrParenSelect();
          members.push({ statement: next, parenthesized: next.parenthesized || false });
        }
        mainQuery = { type: 'union', members, operators, leadingComments: mainComments };
      } else {
        mainQuery = first;
      }
    }

    return { type: 'cte', recursive, ctes, mainQuery, leadingComments: comments };
  }

  private parseCTEDefinition(): AST.CTEDefinition {
    const leadingComments = this.consumeComments();
    const name = this.advance().value;

    // Optional column list: name (col1, col2, ...) AS (...)
    let columnList: string[] | undefined;
    if (this.check('(') && this.peekUpperAt(0) === '(' && !this.looksLikeCTEBodyStart()) {
      this.advance(); // consume (
      columnList = [];
      while (!this.check(')') && !this.isAtEnd()) {
        const col = this.advance();
        columnList.push(col.value);
        if (this.check(',')) this.advance();
      }
      this.expect(')');
    }

    this.expect('AS');

    // MATERIALIZED / NOT MATERIALIZED hints
    let materializedHint: 'materialized' | 'not_materialized' | undefined;
    if (this.peekUpper() === 'MATERIALIZED') {
      this.advance();
      materializedHint = 'materialized';
    } else if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'MATERIALIZED') {
      this.advance(); this.advance();
      materializedHint = 'not_materialized';
    }

    this.expect('(');

    let query: AST.SelectStatement | AST.UnionStatement | AST.ValuesClause;
    if (this.peekUpper() === 'VALUES' || (this.peekType() === 'line_comment' && this.looksLikeValuesAhead())) {
      query = this.parseValuesClause();
    } else {
      const first = this.parseSelect();
      if (this.checkUnionKeyword()) {
        const members: { statement: AST.SelectStatement; parenthesized: boolean }[] = [
          { statement: first, parenthesized: false }
        ];
        const operators: string[] = [];
        while (this.checkUnionKeyword()) {
          operators.push(this.consumeUnionKeyword());
          const next = this.parseSelectOrParenSelect();
          members.push({ statement: next, parenthesized: next.parenthesized || false });
        }
        query = { type: 'union', members, operators, leadingComments: [] };
      } else {
        query = first;
      }
    }

    this.expect(')');
    return { name, columnList, materialized: materializedHint, query, leadingComments };
  }

  private looksLikeCTEBodyStart(): boolean {
    let depth = 0;
    for (let i = this.pos; i < this.tokens.length; i++) {
      const t = this.tokens[i];
      if (t.type === 'line_comment' || t.type === 'block_comment') continue;
      if (t.value === '(') { depth++; continue; }
      if (depth === 1) {
        if (t.upper === 'SELECT' || t.upper === 'VALUES') return true;
        if (t.value === ')') { depth--; continue; }
        return false;
      }
      if (depth > 1) {
        if (t.value === ')') { depth--; continue; }
        continue;
      }
      if (t.value === ')') return false;
    }
    return false;
  }

  private looksLikeValuesAhead(): boolean {
    for (let i = this.pos; i < this.tokens.length; i++) {
      const t = this.tokens[i];
      if (t.type === 'line_comment' || t.type === 'block_comment') continue;
      if (t.upper === 'VALUES') return true;
      if (t.upper === 'SELECT') return false;
      return false;
    }
    return false;
  }

  private parseValuesClause(): AST.ValuesClause {
    const leadingComments = this.consumeComments();

    if (this.peekUpper() === 'VALUES') {
      this.advance();
    }

    const rows: AST.ValuesRow[] = [];
    while (!this.check(')') && !this.isAtEnd()) {
      const rowComments = this.consumeComments();
      if (this.check('(')) {
        this.advance();
        const values = this.parseExpressionList();
        this.expect(')');

        let trailingComment: AST.CommentNode | undefined;
        if (this.peekType() === 'line_comment') {
          const t = this.advance();
          trailingComment = {
            type: 'comment',
            style: 'line',
            text: t.value,
          };
        }

        rows.push({ values, trailingComment, leadingComments: rowComments });

        if (this.check(',')) this.advance();
      } else if (this.check(')')) {
        if (rows.length > 0 && rowComments.length > 0) {
          rows.push({ values: [], leadingComments: rowComments });
        }
        break;
      } else {
        break;
      }
    }

    return { type: 'values', rows, leadingComments };
  }

  private consumeTypeNameToken(): string {
    const first = this.advance();
    const parts: string[] = [first.type === 'keyword' ? first.upper : first.value];

    while (true) {
      const nextToken = this.peek();
      if (nextToken.type !== 'keyword' && nextToken.type !== 'identifier') break;
      const nextUpper = nextToken.upper;
      const validNext = TYPE_CONTINUATIONS[parts[parts.length - 1]];
      const shouldConsume = validNext !== undefined && validNext.has(nextUpper);
      if (!shouldConsume) break;
      const consumed = this.advance();
      parts.push(consumed.type === 'keyword' ? consumed.upper : consumed.value);
    }

    return parts.join(' ');
  }

  // Consume a full type specifier: type name + optional (params) + optional []
  private consumeTypeSpecifier(): string {
    let typeName = this.consumeTypeNameToken();
    if (this.check('(')) {
      typeName += this.advance().value;
      while (!this.check(')') && !this.isAtEnd()) {
        const t = this.advance();
        typeName += t.value;
        if (this.check(',')) {
          typeName += this.advance().value;
          typeName += ' ';
        }
      }
      typeName += this.expect(')').value;
    }
    if (this.check('[')) {
      typeName += this.advance().value;
      if (this.check(']')) {
        typeName += this.advance().value;
      }
    }
    return typeName;
  }

  private collectTokensUntilTopLevelKeyword(stopKeywords: Set<string>): Token[] {
    const tokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd() && !this.check(';')) {
      const t = this.peek();
      if (depth === 0 && t.type === 'keyword' && stopKeywords.has(t.upper)) break;
      this.advance();
      tokens.push(t);
      if (this.isOpenGroupToken(t)) depth++;
      else if (this.isCloseGroupToken(t)) depth = Math.max(0, depth - 1);
    }
    return tokens;
  }

  private splitTopLevelByComma(tokens: Token[]): Token[][] {
    const groups: Token[][] = [];
    let current: Token[] = [];
    let depth = 0;
    for (const t of tokens) {
      if (depth === 0 && t.value === ',') {
        groups.push(current);
        current = [];
        continue;
      }
      current.push(t);
      if (this.isOpenGroupToken(t)) depth++;
      else if (this.isCloseGroupToken(t)) depth = Math.max(0, depth - 1);
    }
    if (current.length > 0) groups.push(current);
    return groups;
  }

  private consumeTokensUntilActionBoundary(): Token[] {
    const tokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd() && !this.check(';')) {
      const t = this.peek();
      if (depth === 0 && t.value === ',') break;
      this.advance();
      tokens.push(t);
      if (this.isOpenGroupToken(t)) depth++;
      else if (this.isCloseGroupToken(t)) depth = Math.max(0, depth - 1);
    }
    return tokens;
  }

  private isOpenGroupToken(token: Token): boolean {
    return token.value === '(' || token.value === '[' || token.value === '{';
  }

  private isCloseGroupToken(token: Token): boolean {
    return token.value === ')' || token.value === ']' || token.value === '}';
  }

  private tokenToSqlValue(token: Token): string {
    if (token.upper === 'TABLES') return 'TABLES';
    return token.type === 'keyword' ? token.upper : token.value;
  }

  private tokensToSql(tokens: Token[]): string {
    if (tokens.length === 0) return '';
    const parts = tokens.map(t => this.tokenToSqlValue(t));
    let out = '';
    for (let i = 0; i < parts.length; i++) {
      const curr = parts[i];
      const prev = i > 0 ? parts[i - 1] : '';
      if (i === 0) {
        out += curr;
        continue;
      }

      const noSpaceBefore = curr === ',' || curr === ')' || curr === ']' || curr === ';' || curr === '.' || curr === '(';
      const noSpaceAfterPrev = prev === '(' || prev === '[' || prev === '.' || prev === '::';
      const noSpaceAroundPair = curr === '::' || curr === '[' || prev === '::';
      if (noSpaceBefore || noSpaceAfterPrev || noSpaceAroundPair) {
        out += curr;
      } else {
        out += ' ' + curr;
      }
    }
    return out.trim();
  }

  // Helper methods

  private consumeComments(): AST.CommentNode[] {
    const comments: AST.CommentNode[] = [];
    while (this.peekType() === 'line_comment' || this.peekType() === 'block_comment') {
      const t = this.advance();
      comments.push({
        type: 'comment',
        style: t.type === 'line_comment' ? 'line' : 'block',
        text: t.value,
        blankLinesBefore: this.blankLinesBeforeToken.get(t.position) || 0,
      });
    }
    return comments;
  }

  private static readonly EOF_TOKEN: Token = { type: 'eof', value: '', upper: '', position: -1, line: 0, column: 0 };

  private peekAt(offset: number): Token {
    const idx = this.pos + offset;
    if (idx < 0 || idx >= this.tokens.length) return Parser.EOF_TOKEN;
    return this.tokens[idx];
  }

  private peek(): Token {
    return this.peekAt(0);
  }

  private peekType(): Token['type'] {
    return this.peekAt(0).type;
  }

  private peekUpper(): string {
    return this.peekAt(0).upper;
  }

  private peekUpperAt(offset: number): string {
    return this.peekAt(offset).upper;
  }

  private peekTypeAt(offset: number): Token['type'] {
    return this.peekAt(offset).type;
  }

  private check(value: string): boolean {
    const token = this.peek();
    return token.value === value || token.upper === value;
  }

  private advance(): Token {
    const token = this.peek();
    if (token.type === 'eof') {
      throw new ParseError('unexpected end of input', token);
    }
    this.pos++;
    return token;
  }

  private expect(value: string): Token {
    const token = this.peek();
    if (token.value !== value && token.upper !== value) {
      throw new ParseError(value, token);
    }
    return this.advance();
  }

  private isAtEnd(): boolean {
    return this.pos >= this.tokens.length || this.tokens[this.pos].type === 'eof';
  }

  private withDepth<T>(fn: () => T): T {
    this.depth++;
    if (this.depth > this.maxDepth) {
      this.depth--;
      throw new MaxDepthError(this.maxDepth, this.peek());
    }
    try {
      return fn();
    } finally {
      this.depth--;
    }
  }
}

/**
 * Parse a SQL string into an array of AST nodes.
 *
 * This is a convenience wrapper that tokenizes the input and runs the
 * {@link Parser}. Each top-level SQL statement becomes one node in the
 * returned array.
 *
 * By default, **recovery mode** is enabled: statements that cannot be parsed
 * are captured as `RawExpression` nodes (type `'raw'`) so the formatter can
 * pass them through unchanged. If recovery cannot produce raw text (for
 * example, an error at end-of-input), the statement is dropped and callers can
 * observe it via `onDropStatement`.
 *
 * To get strict parsing where every syntax error throws, set `recover: false`.
 *
 * Note: {@link MaxDepthError} always throws even in recovery mode, because
 * exceeding the nesting limit is a security boundary, not a syntax issue.
 *
 * @param input    Raw SQL text containing one or more statements.
 * @param options  Parser options (recovery mode, max nesting depth).
 * @returns An array of {@link AST.Node} trees, one per statement. In
 *   recovery mode (default), some entries may be `RawExpression` nodes
 *   for statements that could not be parsed. Returns an empty array for
 *   blank input.
 * @throws {TokenizeError} When the input contains unterminated literals or comments.
 * @throws {ParseError} When `recover` is `false` and a statement cannot be parsed.
 * @throws {MaxDepthError} When nesting depth exceeds `maxDepth` (always, regardless
 *   of recovery mode).
 *
 * @example
 * import { parse } from '@vcoppola/sqlfmt';
 *
 * const ast = parse('SELECT id, name FROM users WHERE active = TRUE;');
 * // ast[0].type === 'select'
 *
 * @example
 * // Strict mode -- throws on parse errors instead of recovering
 * const ast = parse('SELECT ...', { recover: false });
 *
 * @example
 * // Detect unrecognized statements in recovery mode
 * const nodes = parse('SELECT 1; FOOBAR baz;');
 * for (const node of nodes) {
 *   if (node.type === 'raw') console.warn('Unparsed:', node);
 * }
 */
export function parse(input: string, options: ParseOptions = {}): AST.Node[] {
  if (!input.trim()) return [];
  const parser = new Parser(tokenize(input), options);
  return parser.parseStatements();
}
