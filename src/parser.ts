import { tokenize, Token } from './tokenizer';
import * as AST from './ast';

const DEFAULT_MAX_DEPTH = 100;
const CLAUSE_KEYWORDS = new Set([
  'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
  'UNION', 'INTERSECT', 'EXCEPT', 'ON', 'SET', 'VALUES',
  'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'JOIN',
  'INTO', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER',
  'DROP', 'WITH', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR',
  'RETURNING', 'FETCH', 'WINDOW', 'LATERAL', 'FOR', 'USING', 'ESCAPE',
]);

const TYPE_CONTINUATION_RULES: ReadonlyArray<{
  predicate: (parts: string[], next: string) => boolean;
}> = [
  {
    predicate: (parts, next) => parts[parts.length - 1] === 'DOUBLE' && next === 'PRECISION',
  },
  {
    predicate: (parts, next) => (parts[parts.length - 1] === 'CHARACTER' || parts[parts.length - 1] === 'CHAR') && next === 'VARYING',
  },
  {
    predicate: (parts, next) => parts[parts.length - 1] === 'NATIONAL' && next === 'CHARACTER',
  },
  {
    predicate: (parts, next) => (parts[parts.length - 1] === 'TIMESTAMP' || parts[parts.length - 1] === 'TIME') && (next === 'WITH' || next === 'WITHOUT'),
  },
  {
    predicate: (parts, next) => parts[parts.length - 1] === 'WITH' && next === 'TIME',
  },
  {
    predicate: (parts, next) => parts[parts.length - 1] === 'WITHOUT' && next === 'TIME',
  },
  {
    predicate: (parts, next) => parts[parts.length - 1] === 'TIME' && (next === 'ZONE' || next === 'PRECISION'),
  },
];

export interface ParseOptions {
  recover?: boolean;
  maxDepth?: number;
}

export class ParseError extends Error {
  readonly token: Token;
  readonly expected: string;

  constructor(expected: string, token: Token) {
    const got = token.type === 'eof' ? 'EOF' : `${token.type} "${token.value}"`;
    super(`Expected ${expected}, got ${got}`);
    this.name = 'ParseError';
    this.expected = expected;
    this.token = token;
  }
}

export class Parser {
  private tokens: Token[];
  private pos: number = 0;
  private blankLinesBeforeToken = new Map<number, number>();
  private readonly recover: boolean;
  private readonly maxDepth: number;
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
  }

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
        if (err.expected.startsWith('maximum nesting depth')) throw err;
        this.pos = stmtStart;
        const raw = this.parseRawStatement();
        if (raw) stmts.push(raw);
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

    if (this.isAtEnd()) return null;

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
    return raw;
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
    first.leadingComments = comments;

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

  private tryParseQueryExpressionAtCurrent(comments: AST.CommentNode[] = []): AST.QueryExpression | null {
    const checkpoint = this.pos;
    try {
      return this.parseQueryExpression(comments);
    } catch (err) {
      if (err instanceof ParseError) {
        this.pos = checkpoint;
        return null;
      }
      throw err;
    }
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
      select.parenthesized = true;
      this.expect(')');
      return select;
    }
    return this.parseSelect();
  }

  private parseSelect(): AST.SelectStatement {
    this.expectKeyword('SELECT');

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
      while (this.check(',')) {
        this.advance();
        if (!additionalFromItems) additionalFromItems = [];
        additionalFromItems.push(this.parseFromItem());
      }

      // Parse JOINs
      while (this.isJoinKeyword()) {
        joins.push(this.parseJoin());
      }
    }

    if (this.peekUpper() === 'WHERE') {
      this.advance();
      where = { condition: this.parseExpression() };
      if (this.peekType() === 'line_comment') {
        const t = this.advance();
        where.trailingComment = {
          type: 'comment',
          style: 'line',
          text: t.value,
        };
      }
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
      let rowsKeyword = false;
      offset = { count: this.parsePrimary(), rowsKeyword };
      // consume optional ROWS keyword
      if (this.peekUpper() === 'ROWS') {
        this.advance();
        offset.rowsKeyword = true;
      }
    }

    if (this.peekUpper() === 'FETCH') {
      fetch = this.parseFetchClause();
    }

    if (this.peekUpper() === 'FOR') {
      lockingClause = this.parseForClause();
    }

    const result: AST.SelectStatement = {
      type: 'select',
      distinct,
      columns,
      from,
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

    if (additionalFromItems) result.additionalFromItems = additionalFromItems;

    return result;
  }

  private parseFromItem(): AST.FromClause {
    let lateral = false;
    if (this.peekUpper() === 'LATERAL') {
      this.advance();
      lateral = true;
    }

    const table = this.parseTableExpr();
    let alias: string | undefined;
    let aliasColumns: string[] | undefined;
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

    if (this.peekUpper() === 'AS') {
      this.advance();
      alias = this.advance().value;
      // Check for column alias list: AS alias(col1, col2)
      if (this.check('(')) {
        this.advance();
        const cols: string[] = [];
        while (!this.check(')') && !this.isAtEnd()) {
          cols.push(this.advance().value);
          if (this.check(',')) this.advance();
        }
        this.expect(')');
        aliasColumns = cols;
      }
    } else if (this.peekType() === 'identifier' && !this.isClauseKeyword() && !this.isJoinKeyword() && !this.check(',') && !this.check(')') && !this.check(';') && this.peekUpper() !== 'TABLESAMPLE') {
      alias = this.advance().value;
      // Check for column alias list
      if (this.check('(')) {
        this.advance();
        const cols: string[] = [];
        while (!this.check(')') && !this.isAtEnd()) {
          cols.push(this.advance().value);
          if (this.check(',')) this.advance();
        }
        this.expect(')');
        aliasColumns = cols;
      }
    }

    return { table, alias, aliasColumns, lateral, tablesample };
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
    const checkpoint = this.pos;
    try {
      if (!this.check('(')) return null;
      this.advance();
      const query = this.parseQueryExpression();
      this.expect(')');
      return { type: 'subquery', query };
    } catch (err) {
      if (err instanceof ParseError) {
        this.pos = checkpoint;
        return null;
      }
      throw err;
    }
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
    let alias: string | undefined;
    let aliasColumns: string[] | undefined;

    if (this.peekUpper() === 'AS') {
      this.advance();
      alias = this.advance().value;
      if (this.check('(')) {
        this.advance();
        const cols: string[] = [];
        while (!this.check(')') && !this.isAtEnd()) {
          cols.push(this.advance().value);
          if (this.check(',')) this.advance();
        }
        this.expect(')');
        aliasColumns = cols;
      }
    } else if (this.peekType() === 'identifier' && !this.isClauseKeyword() && !this.isJoinKeyword() && this.peekUpper() !== 'ON' && this.peekUpper() !== 'USING' && !this.check(',') && !this.check(')') && !this.check(';')) {
      alias = this.advance().value;
      if (this.check('(')) {
        this.advance();
        const cols: string[] = [];
        while (!this.check(')') && !this.isAtEnd()) {
          cols.push(this.advance().value);
          if (this.check(',')) this.advance();
        }
        this.expect(')');
        aliasColumns = cols;
      }
    }

    let on: AST.Expression | undefined;
    let usingClause: string[] | undefined;

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

    return { joinType: joinType.trim(), table, alias, aliasColumns, lateral, on, usingClause };
  }

  private parseGroupByClause(): AST.GroupByClause {
    const items: AST.Expression[] = [];
    let groupingSets: AST.GroupByClause['groupingSets'];

    // Check for GROUPING SETS, ROLLUP, CUBE
    const kw = this.peekUpper();
    if (kw === 'GROUPING' && this.peekUpperAt(1) === 'SETS') {
      groupingSets = [];
      groupingSets.push(this.parseGroupingSetsSpec('grouping_sets'));
    } else if (kw === 'ROLLUP') {
      groupingSets = [];
      groupingSets.push(this.parseGroupingSetsSpec('rollup'));
    } else if (kw === 'CUBE') {
      groupingSets = [];
      groupingSets.push(this.parseGroupingSetsSpec('cube'));
    } else {
      // Normal GROUP BY items
      items.push(this.parseExpression());
      while (this.check(',')) {
        this.advance();
        // Check if next is GROUPING SETS, ROLLUP, or CUBE
        const nextKw = this.peekUpper();
        if ((nextKw === 'GROUPING' && this.peekUpperAt(1) === 'SETS') || nextKw === 'ROLLUP' || nextKw === 'CUBE') {
          if (!groupingSets) groupingSets = [];
          const specType = nextKw === 'GROUPING' ? 'grouping_sets' : nextKw.toLowerCase() as 'rollup' | 'cube';
          groupingSets.push(this.parseGroupingSetsSpec(specType));
        } else {
          items.push(this.parseExpression());
        }
      }
    }

    return { items, groupingSets };
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
    this.expectKeyword('AS');
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
    if (this.peekUpper() === 'ROWS' || this.peekUpper() === 'RANGE') {
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
    this.expectKeyword('FOR');

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
    return { expr, direction, nulls };
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
    let left = this.parseAddSub();

    // IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE
    // IS [NOT] DISTINCT FROM
    if (this.peekUpper() === 'IS') {
      this.advance();
      if (this.peekUpper() === 'NOT') {
        this.advance();
        if (this.peekUpper() === 'NULL') {
          this.advance();
          return { type: 'is', expr: left, value: 'NOT NULL' };
        }
        if (this.peekUpper() === 'DISTINCT' && this.peekUpperAt(1) === 'FROM') {
          this.advance(); this.advance();
          const right = this.parseAddSub();
          return { type: 'is_distinct_from', left, right, negated: true } as AST.IsDistinctFromExpr;
        }
        if (this.peekUpper() === 'TRUE') {
          this.advance();
          return { type: 'is', expr: left, value: 'NOT TRUE' };
        }
        if (this.peekUpper() === 'FALSE') {
          this.advance();
          return { type: 'is', expr: left, value: 'NOT FALSE' };
        }
      } else if (this.peekUpper() === 'NULL') {
        this.advance();
        return { type: 'is', expr: left, value: 'NULL' };
      } else if (this.peekUpper() === 'TRUE') {
        this.advance();
        return { type: 'is', expr: left, value: 'TRUE' };
      } else if (this.peekUpper() === 'FALSE') {
        this.advance();
        return { type: 'is', expr: left, value: 'FALSE' };
      } else if (this.peekUpper() === 'DISTINCT' && this.peekUpperAt(1) === 'FROM') {
        this.advance(); this.advance();
        const right = this.parseAddSub();
        return { type: 'is_distinct_from', left, right, negated: false } as AST.IsDistinctFromExpr;
      }
    }

    // [NOT] BETWEEN
    let negated = false;
    if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'BETWEEN') {
      this.advance();
      negated = true;
    }
    if (this.peekUpper() === 'BETWEEN') {
      this.advance();
      const low = this.parseAddSub();
      this.expectKeyword('AND');
      const high = this.parseAddSub();
      return { type: 'between', expr: left, low, high, negated };
    }

    // [NOT] IN
    negated = false;
    if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'IN') {
      this.advance();
      negated = true;
    }
    if (this.peekUpper() === 'IN') {
      this.advance();
      this.expect('(');
      const query = this.tryParseQueryExpressionAtCurrent();
      if (query) {
        this.expect(')');
        return { type: 'in', expr: left, values: { type: 'subquery', query }, negated };
      }
      const values = this.parseExpressionList();
      this.expect(')');
      return { type: 'in', expr: left, values, negated };
    }

    // [NOT] LIKE
    negated = false;
    if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'LIKE') {
      this.advance();
      negated = true;
    }
    if (this.peekUpper() === 'LIKE') {
      this.advance();
      const pattern = this.parseAddSub();
      let escape: AST.Expression | undefined;
      if (this.peekUpper() === 'ESCAPE') {
        this.advance();
        escape = this.parseAddSub();
      }
      return { type: 'like', expr: left, pattern, negated, escape };
    }

    // [NOT] ILIKE
    negated = false;
    if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'ILIKE') {
      this.advance();
      negated = true;
    }
    if (this.peekUpper() === 'ILIKE') {
      this.advance();
      const pattern = this.parseAddSub();
      let escape: AST.Expression | undefined;
      if (this.peekUpper() === 'ESCAPE') {
        this.advance();
        escape = this.parseAddSub();
      }
      return { type: 'ilike', expr: left, pattern, negated, escape };
    }

    // [NOT] SIMILAR TO
    negated = false;
    if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'SIMILAR') {
      this.advance();
      negated = true;
    }
    if (this.peekUpper() === 'SIMILAR' && this.peekUpperAt(1) === 'TO') {
      this.advance(); this.advance();
      const pattern = this.parseAddSub();
      return { type: 'similar_to', expr: left, pattern, negated };
    }

    // Regex operators: ~, ~*, !~, !~*
    if (this.isRegexOperator()) {
      const op = this.advance().value;
      const right = this.parseAddSub();
      return { type: 'regex_match', left, operator: op, right } as AST.RegexExpr;
    }

    // JSON/Array/JSONB/Bitwise operators at comparison level
    // These need to be at a lower precedence than comparison operators
    // Actually, comparison operators should be handled here
    if (this.checkComparisonOperator()) {
      const op = this.advance().value;
      const right = this.parseAddSub();
      return { type: 'binary', left, operator: op, right };
    }

    return left;
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
      let targetType = this.consumeTypeNameToken();
      // Handle parameterized types like NUMERIC(10, 2)
      if (this.check('(')) {
        targetType += this.advance().value;
        while (!this.check(')') && !this.isAtEnd()) {
          const t = this.advance();
          targetType += t.value;
          if (this.check(',')) {
            targetType += this.advance().value;
            targetType += ' ';
          }
        }
        targetType += this.advance().value; // closing )
      }
      // Handle array suffix []
      if (this.check('[')) {
        targetType += this.advance().value;
        if (this.check(']')) {
          targetType += this.advance().value;
        }
      }
      expr = { type: 'pg_cast', expr, targetType } as AST.PgCastExpr;
    }

    // Handle array subscript: expr[idx] or expr[lo:hi]
    while (this.check('[')) {
      this.advance(); // consume [
      const idx = this.parseExpression();
      if (this.check(':')) {
        // Slice: expr[lo:hi]
        this.advance();
        const hi = this.parseExpression();
        this.expect(']');
        expr = { type: 'raw', text: fmtExprForRaw(expr) + '[' + fmtExprForRaw(idx) + ':' + fmtExprForRaw(hi) + ']' } as AST.RawExpression;
      } else {
        this.expect(']');
        expr = { type: 'raw', text: fmtExprForRaw(expr) + '[' + fmtExprForRaw(idx) + ']' } as AST.RawExpression;
      }
    }

    return expr;
  }

  private parsePrimary(): AST.Expression {
    const token = this.peek();

    // EXISTS subquery
    if (token.upper === 'EXISTS') {
      this.advance();
      const subq = this.parseSubquery();
      return { type: 'exists', subquery: subq };
    }

    // NOT EXISTS
    if (token.upper === 'NOT' && this.peekUpperAt(1) === 'EXISTS') {
      this.advance();
      this.advance();
      const subq = this.parseSubquery();
      return { type: 'unary', operator: 'NOT', operand: { type: 'exists', subquery: subq } };
    }

    // CASE expression
    if (token.upper === 'CASE') {
      return this.parseCaseExpr();
    }

    // CAST expression
    if (token.upper === 'CAST') {
      return this.parseCast();
    }

    // EXTRACT(field FROM expr)
    if (token.upper === 'EXTRACT' && this.peekAt(1)?.value === '(') {
      return this.parseExtract();
    }

    // POSITION(substr IN str)
    if (token.upper === 'POSITION' && this.peekAt(1)?.value === '(') {
      return this.parsePositionExpr();
    }

    // SUBSTRING(str FROM start FOR len)
    if (token.upper === 'SUBSTRING' && this.peekAt(1)?.value === '(') {
      return this.parseSubstringExpr();
    }

    // OVERLAY(str PLACING replacement FROM start FOR len)
    if (token.upper === 'OVERLAY' && this.peekAt(1)?.value === '(') {
      return this.parseOverlayExpr();
    }

    // TRIM([LEADING|TRAILING|BOTH] char FROM str)
    if (token.upper === 'TRIM' && this.peekAt(1)?.value === '(') {
      return this.parseTrimExpr();
    }

    // INTERVAL 'value'
    if (token.upper === 'INTERVAL' && this.peekTypeAt(1) === 'string') {
      this.advance();
      const strToken = this.advance();
      return { type: 'interval', value: strToken.value };
    }

    // ARRAY[...] constructor
    if (token.upper === 'ARRAY' && this.peekAt(1)?.value === '[') {
      this.advance(); // consume ARRAY
      this.advance(); // consume [
      const elements: AST.Expression[] = [];
      if (!this.check(']')) {
        elements.push(this.parseExpression());
        while (this.check(',')) {
          this.advance();
          elements.push(this.parseExpression());
        }
      }
      this.expect(']');
      return { type: 'array_constructor', elements } as AST.ArrayConstructorExpr;
    }

    // ROW(...) constructor
    if (token.upper === 'ROW' && this.peekAt(1)?.value === '(') {
      this.advance(); // consume ROW
      this.advance(); // consume (
      const args: AST.Expression[] = [];
      if (!this.check(')')) {
        args.push(this.parseExpression());
        while (this.check(',')) {
          this.advance();
          args.push(this.parseExpression());
        }
      }
      this.expect(')');
      return { type: 'function_call', name: 'ROW', args, distinct: false } as AST.FunctionCallExpr;
    }

    // Paren expression or subquery
    if (token.value === '(') {
      const subquery = this.tryParseSubqueryAtCurrent();
      if (subquery) {
        return subquery;
      }
      this.advance();
      const expr = this.parseExpression();
      this.expect(')');
      return { type: 'paren', expr };
    }

    // Star
    if (token.value === '*') {
      this.advance();
      return { type: 'star' };
    }

    // Boolean literals
    if (token.upper === 'TRUE' || token.upper === 'FALSE') {
      this.advance();
      return { type: 'literal', value: token.upper, literalType: 'boolean' };
    }

    // NULL literal
    if (token.upper === 'NULL') {
      this.advance();
      return { type: 'null' };
    }

    // Number literal
    if (token.type === 'number') {
      this.advance();
      return { type: 'literal', value: token.value, literalType: 'number' };
    }

    // String literal
    if (token.type === 'string') {
      this.advance();
      return { type: 'literal', value: token.value, literalType: 'string' };
    }

    // Positional parameter: $1, $2, ...
    if (token.type === 'parameter') {
      this.advance();
      return { type: 'raw', text: token.value };
    }

    // DATE/TIME/TIMESTAMP type constructor: DATE 'YYYY-MM-DD'
    if ((token.upper === 'DATE' || token.upper === 'TIME' || token.upper === 'TIMESTAMP')
        && this.peekTypeAt(1) === 'string') {
      this.advance();
      const strToken = this.advance();
      return {
        type: 'typed_string',
        dataType: token.upper as 'DATE' | 'TIME' | 'TIMESTAMP',
        value: strToken.value,
      };
    }

    // CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP (no parens needed)
    if (token.upper === 'CURRENT_DATE' || token.upper === 'CURRENT_TIME' || token.upper === 'CURRENT_TIMESTAMP') {
      this.advance();
      return { type: 'raw', text: token.upper };
    }

    // Identifier (possibly qualified: a.b, possibly function call: f(...))
    if (token.type === 'identifier' || token.type === 'keyword') {
      return this.parseIdentifierOrFunction();
    }

    // Fallback: consume the token as raw
    this.advance();
    return { type: 'raw', text: token.value };
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
    if (this.check('(') && !this.isClauseKeywordValue(name.upper)) {
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

      const funcExpr: AST.FunctionCallExpr = { type: 'function_call', name: fullName, args, distinct };

      if (innerOrderBy) {
        funcExpr.orderBy = innerOrderBy;
      }

      // FILTER (WHERE ...)
      if (this.peekUpper() === 'FILTER') {
        this.advance();
        this.expect('(');
        this.expectKeyword('WHERE');
        funcExpr.filter = this.parseExpression();
        this.expect(')');
      }

      // WITHIN GROUP (ORDER BY ...)
      if (this.peekUpper() === 'WITHIN') {
        this.advance();
        this.expectKeyword('GROUP');
        this.expect('(');
        this.expectKeyword('ORDER');
        this.expectKeyword('BY');
        const withinOrderBy = this.parseOrderByItems();
        this.expect(')');
        funcExpr.withinGroup = { orderBy: withinOrderBy };
      }

      // Window function: OVER (...)
      if (this.peekUpper() === 'OVER') {
        return this.parseWindowFunction(funcExpr);
      }

      return funcExpr;
    }

    return { type: 'identifier', value: fullName, quoted };
  }

  private parseWindowFunction(func: AST.FunctionCallExpr): AST.WindowFunctionExpr {
    this.expectKeyword('OVER');

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
    this.expectKeyword('CASE');

    let operand: AST.Expression | undefined;
    if (this.peekUpper() !== 'WHEN') {
      operand = this.parseExpression();
    }

    const whenClauses: { condition: AST.Expression; result: AST.Expression }[] = [];
    while (this.peekUpper() === 'WHEN') {
      this.advance();
      const condition = this.parseExpression();
      this.expectKeyword('THEN');
      const result = this.parseExpression();
      whenClauses.push({ condition, result });
    }

    let elseResult: AST.Expression | undefined;
    if (this.peekUpper() === 'ELSE') {
      this.advance();
      elseResult = this.parseExpression();
    }

    this.expectKeyword('END');
    return { type: 'case', operand, whenClauses, elseResult };
  }

  private parseCast(): AST.CastExpr {
    this.expectKeyword('CAST');
    this.expect('(');
    const expr = this.parseExpression();
    this.expectKeyword('AS');
    let targetType = this.consumeTypeNameToken();
    if (this.check('(')) {
      targetType += this.advance().value;
      while (!this.check(')')) {
        const t = this.advance();
        targetType += t.value;
        if (this.check(',')) {
          targetType += this.advance().value;
          targetType += ' ';
        }
      }
      targetType += this.advance().value; // closing )
    }
    this.expect(')');
    return { type: 'cast', expr, targetType };
  }

  private parseExtract(): AST.ExtractExpr {
    this.expectKeyword('EXTRACT');
    this.expect('(');
    const field = this.advance().upper;
    this.expectKeyword('FROM');
    const source = this.parseExpression();
    this.expect(')');
    return { type: 'extract', field, source };
  }

  private parsePositionExpr(): AST.Expression {
    this.advance(); // POSITION
    this.expect('(');
    const substr = this.parseAddSub();
    this.expectKeyword('IN');
    const str = this.parseAddSub();
    this.expect(')');
    return { type: 'position', substring: substr, source: str } as AST.PositionExpr;
  }

  private parseSubstringExpr(): AST.Expression {
    this.advance(); // SUBSTRING
    this.expect('(');
    const str = this.parseExpression();
    this.expectKeyword('FROM');
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
    this.expectKeyword('PLACING');
    const replacement = this.parseExpression();
    this.expectKeyword('FROM');
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

    if (this.peekUpper() === 'AS') {
      this.advance();
      const alias = this.advance().value;
      return { type: 'raw', text: `${fmtExprForRaw(expr)} AS ${alias.startsWith('"') ? alias : alias.toLowerCase()}` };
    }

    if (this.peekType() === 'identifier' && !this.check(',') && !this.check(';')) {
      const alias = this.advance().value;
      return { type: 'raw', text: `${fmtExprForRaw(expr)} AS ${alias.startsWith('"') ? alias : alias.toLowerCase()}` };
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
        columns[columns.length - 1].trailingComment = {
          type: 'comment',
          style: 'line',
          text: t.value,
        };
      }
      columns.push(this.parseColumnExpr());
    }

    return columns;
  }

  private parseColumnExpr(): AST.ColumnExpr {
    const expr = this.parseExpression();
    let alias: string | undefined;
    let trailingComment: AST.CommentNode | undefined;

    if (this.peekUpper() === 'AS') {
      this.advance();
      alias = this.advance().value;
    } else if (this.peekType() === 'identifier' && !this.isClauseKeyword() && !this.check(',') && !this.check(')')) {
      alias = this.advance().value;
    }

    if (this.peekType() === 'line_comment') {
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: this.advance().value,
      };
    }

    return { expr, alias, trailingComment };
  }

  // INSERT INTO table (cols) VALUES (...), (...) | SELECT ...
  private parseInsert(comments: AST.CommentNode[]): AST.InsertStatement {
    this.expectKeyword('INSERT');
    this.expectKeyword('INTO');
    const table = this.advance().value;

    let columns: string[] = [];
    if (this.check('(')) {
      this.advance();
      while (!this.check(')')) {
        columns.push(this.advance().value);
        if (this.check(',')) this.advance();
      }
      this.expect(')');
    }

    let defaultValues = false;
    let values: AST.ValuesList[] | undefined;
    let selectQuery: AST.QueryExpression | undefined;

    if (this.peekUpper() === 'DEFAULT' && this.peekUpperAt(1) === 'VALUES') {
      this.advance();
      this.advance();
      defaultValues = true;
    } else if (this.peekUpper() === 'VALUES') {
      this.advance();
      values = [];
      values.push(this.parseValuesTuple());
      while (this.check(',')) {
        this.advance();
        values.push(this.parseValuesTuple());
      }
    } else if (this.peekUpper() === 'SELECT' || this.peekUpper() === 'WITH' || this.check('(')) {
      selectQuery = this.parseQueryExpression();
    }

    // ON CONFLICT
    let onConflict: AST.InsertStatement['onConflict'];
    if (this.peekUpper() === 'ON' && this.peekUpperAt(1) === 'CONFLICT') {
      this.advance(); this.advance();
      onConflict = this.parseOnConflict();
    }

    // RETURNING
    let returning: AST.Expression[] | undefined;
    if (this.peekUpper() === 'RETURNING') {
      this.advance();
      returning = this.parseReturningList();
    }

    return {
      type: 'insert',
      table,
      columns,
      defaultValues,
      values,
      selectQuery,
      onConflict,
      returning,
      leadingComments: comments,
    };
  }

  private parseOnConflict(): AST.InsertStatement['onConflict'] {
    let conflictColumns: string[] | undefined;
    let constraintName: string | undefined;

    if (this.peekUpper() === 'ON' && this.peekUpperAt(1) === 'CONSTRAINT') {
      this.advance(); // ON
      this.advance(); // CONSTRAINT
      constraintName = this.advance().value;
    } else if (this.check('(')) {
      this.advance();
      conflictColumns = [];
      while (!this.check(')') && !this.isAtEnd()) {
        conflictColumns.push(this.advance().value);
        if (this.check(',')) this.advance();
      }
      this.expect(')');
    }

    this.expectKeyword('DO');

    if (this.peekUpper() === 'NOTHING') {
      this.advance();
      return { columns: conflictColumns, constraintName, action: 'nothing' };
    }

    // DO UPDATE
    this.expectKeyword('UPDATE');
    this.expectKeyword('SET');
    const setItems: { column: string; value: AST.Expression }[] = [];
    setItems.push(this.parseSetItem());
    while (this.check(',')) {
      this.advance();
      setItems.push(this.parseSetItem());
    }

    let where: AST.Expression | undefined;
    if (this.peekUpper() === 'WHERE') {
      this.advance();
      where = this.parseExpression();
    }

    return { columns: conflictColumns, constraintName, action: 'update', setItems, where };
  }

  private parseValuesTuple(): AST.ValuesList {
    this.expect('(');
    const values = this.parseExpressionList();
    this.expect(')');
    return { values };
  }

  // UPDATE table SET col = val, ... [FROM ...] WHERE ...
  private parseUpdate(comments: AST.CommentNode[]): AST.UpdateStatement {
    this.expectKeyword('UPDATE');
    const table = this.advance().value;

    this.expectKeyword('SET');
    const setItems: AST.SetItem[] = [];
    setItems.push(this.parseSetItem());
    while (this.check(',')) {
      this.advance();
      setItems.push(this.parseSetItem());
    }

    let from: AST.FromClause | undefined;
    if (this.peekUpper() === 'FROM') {
      this.advance();
      from = this.parseFromItem();
    }

    let where: AST.WhereClause | undefined;
    if (this.peekUpper() === 'WHERE') {
      this.advance();
      where = { condition: this.parseExpression() };
    }

    let returning: AST.Expression[] | undefined;
    if (this.peekUpper() === 'RETURNING') {
      this.advance();
      returning = this.parseReturningList();
    }

    return { type: 'update', table, setItems, from, where, returning, leadingComments: comments };
  }

  private parseSetItem(): AST.SetItem {
    const column = this.advance().value;
    this.expect('=');
    const value = this.parseExpression();
    return { column, value };
  }

  // DELETE FROM table WHERE ...
  private parseDelete(comments: AST.CommentNode[]): AST.DeleteStatement {
    this.expectKeyword('DELETE');
    this.expectKeyword('FROM');
    const table = this.advance().value;

    let using: AST.FromClause[] | undefined;
    if (this.peekUpper() === 'USING') {
      this.advance();
      using = [this.parseFromItem()];
      while (this.check(',')) {
        this.advance();
        using.push(this.parseFromItem());
      }
    }

    let where: AST.WhereClause | undefined;
    if (this.peekUpper() === 'WHERE') {
      this.advance();
      where = { condition: this.parseExpression() };
    }

    let returning: AST.Expression[] | undefined;
    if (this.peekUpper() === 'RETURNING') {
      this.advance();
      returning = this.parseReturningList();
    }

    return { type: 'delete', from: table, using, where, returning, leadingComments: comments };
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
    this.expectKeyword('TABLE');

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

    this.expectKeyword('ON');
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
      const dir = this.advance().upper;
      return { type: 'raw', text: `${fmtExprForRaw(expr)} ${dir}` };
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

    this.expectKeyword('AS');

    // Parse the query
    const query = this.parseStatement();

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
    this.expectKeyword('INTO');
    const targetTable = this.advance().value;
    let targetAlias: string | undefined;
    if (this.peekUpper() === 'AS') {
      this.advance();
      targetAlias = this.advance().value;
    } else if (this.peekType() === 'identifier' && this.peekUpper() !== 'USING') {
      targetAlias = this.advance().value;
    }

    this.expectKeyword('USING');
    const sourceTable = this.advance().value;
    let sourceAlias: string | undefined;
    if (this.peekUpper() === 'AS') {
      this.advance();
      sourceAlias = this.advance().value;
    } else if (this.peekType() === 'identifier' && this.peekUpper() !== 'ON') {
      sourceAlias = this.advance().value;
    }

    this.expectKeyword('ON');
    const onExpr = this.parseExpression();

    const whenClauses: AST.MergeWhenClause[] = [];
    while (this.peekUpper() === 'WHEN') {
      this.advance(); // WHEN

      let matched = true;
      if (this.peekUpper() === 'NOT') {
        this.advance();
        matched = false;
      }
      this.expectKeyword('MATCHED');

      let condition: AST.Expression | undefined;
      if (this.peekUpper() === 'AND') {
        this.advance();
        condition = this.parseExpression();
      }

      this.expectKeyword('THEN');

      const actionKw = this.peekUpper();
      if (actionKw === 'DELETE') {
        this.advance();
        whenClauses.push({ matched, condition, action: 'delete' });
      } else if (actionKw === 'UPDATE') {
        this.advance();
        this.expectKeyword('SET');
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
        this.expectKeyword('VALUES');
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
    let raw = '';
    while (!this.isAtEnd() && !this.check(';')) {
      const t = this.advance();
      if (raw) raw += ' ';
      raw += t.type === 'keyword' ? t.upper : t.value;
    }
    return { type: 'grant', raw, leadingComments: comments };
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
      this.expectKeyword('IDENTITY');
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
        let body = 'CHECK';
        let depth = 0;
        if (this.check('(')) {
          body += this.advance().value;
          depth = 1;
          while (depth > 0 && !this.isAtEnd()) {
            const t = this.advance();
            if (t.value === '(') depth++;
            if (t.value === ')') depth--;
            if (depth > 0) {
              body += (t.type === 'keyword') ? t.upper : t.value;
              if (!this.check(')') && depth > 0) body += ' ';
            } else {
              body += ')';
            }
          }
        }
        return {
          elementType: 'constraint',
          raw: `CONSTRAINT ${constraintName} ${body}`,
          constraintName,
          constraintBody: body,
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

        this.expectKeyword('REFERENCES');
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
    this.expectKeyword('ALTER');
    const objectTypeToken = this.advance();
    if (objectTypeToken.type !== 'keyword' && objectTypeToken.type !== 'identifier') {
      throw new ParseError('object type', objectTypeToken);
    }
    const objectType = objectTypeToken.upper;
    const objectName = this.advance().value;

    let action = '';
    while (!this.check(';') && !this.isAtEnd()) {
      const t = this.advance();
      const val = t.type === 'keyword' ? t.upper : t.value;
      if (val === '(') {
        action += val;
      } else if (val === ')') {
        action += val;
      } else if (val === ',') {
        action += val;
      } else {
        if (action && !action.endsWith('(')) {
          action += ' ';
        }
        action += val;
      }
    }

    return {
      type: 'alter_table',
      objectType,
      objectName,
      tableName: objectName,
      action,
      leadingComments: comments,
    };
  }

  // DROP TABLE [IF EXISTS] name
  private parseDrop(comments: AST.CommentNode[]): AST.DropTableStatement {
    this.expectKeyword('DROP');
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
      tableName: objectName,
      leadingComments: comments,
    };
  }

  // CTE: WITH [RECURSIVE] name AS (...), name AS (...) SELECT ...
  private parseCTE(comments: AST.CommentNode[]): AST.CTEStatement {
    this.expectKeyword('WITH');

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
        first.leadingComments = mainComments;

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
      first.leadingComments = mainComments;

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

    this.expectKeyword('AS');

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
        return false;
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
          if (!rows[rows.length - 1].leadingComments) {
            rows[rows.length - 1].leadingComments = [];
          }
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
      const shouldConsume = TYPE_CONTINUATION_RULES.some(rule => rule.predicate(parts, nextUpper));
      if (!shouldConsume) break;
      const consumed = this.advance();
      parts.push(consumed.type === 'keyword' ? consumed.upper : consumed.value);
    }

    return parts.join(' ');
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

  private isClauseKeyword(): boolean {
    return this.isClauseKeywordValue(this.peekUpper());
  }

  private isClauseKeywordValue(val: string): boolean {
    return CLAUSE_KEYWORDS.has(val);
  }

  private peek(): Token {
    if (this.pos >= this.tokens.length) {
      return { type: 'eof', value: '', upper: '', position: -1 };
    }
    return this.tokens[this.pos];
  }

  private peekAt(offset: number): Token | undefined {
    const idx = this.pos + offset;
    if (idx >= this.tokens.length) return undefined;
    return this.tokens[idx];
  }

  private peekType(): string {
    return this.peek().type;
  }

  private peekUpper(): string {
    return this.peek().upper;
  }

  private peekUpperAt(offset: number): string {
    const idx = this.pos + offset;
    if (idx >= this.tokens.length) return '';
    return this.tokens[idx].upper;
  }

  private peekTypeAt(offset: number): string {
    const idx = this.pos + offset;
    if (idx >= this.tokens.length) return 'eof';
    return this.tokens[idx].type;
  }

  private check(value: string): boolean {
    const token = this.peek();
    return token.value === value || token.upper === value;
  }

  private advance(): Token {
    if (this.pos >= this.tokens.length) {
      throw new ParseError('more input', this.peek());
    }
    const token = this.tokens[this.pos];
    if (token.type === 'eof') {
      throw new ParseError('more input', token);
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

  private expectKeyword(keyword: string): Token {
    const token = this.peek();
    if (token.type !== 'keyword' || token.upper !== keyword) {
      throw new ParseError(keyword, token);
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
      throw new ParseError(`maximum nesting depth ${this.maxDepth}`, this.peek());
    }
    try {
      return fn();
    } finally {
      this.depth--;
    }
  }
}

export function parse(input: string, options: ParseOptions = {}): AST.Node[] {
  if (!input.trim()) return [];
  const parser = new Parser(tokenize(input), options);
  return parser.parseStatements();
}

function assertNever(x: never): never {
  throw new Error(`Unhandled expression type: ${(x as { type?: string }).type ?? 'unknown'}`);
}

// Helper for building raw text from AST during parse
function fmtExprForRaw(expr: AST.Expression): string {
  switch (expr.type) {
    case 'identifier':
      return expr.quoted ? expr.value : expr.value.toLowerCase();
    case 'literal':
      if (expr.literalType === 'boolean') return expr.value.toUpperCase();
      return expr.value;
    case 'null':
      return 'NULL';
    case 'interval':
      return `INTERVAL ${expr.value}`;
    case 'typed_string':
      return `${expr.dataType} ${expr.value}`;
    case 'star':
      return expr.qualifier ? expr.qualifier.toLowerCase() + '.*' : '*';
    case 'binary':
      return fmtExprForRaw(expr.left) + ' ' + expr.operator + ' ' + fmtExprForRaw(expr.right);
    case 'unary':
      if (expr.operator === '-') return '-' + fmtExprForRaw(expr.operand);
      if (expr.operator === '~') return '~' + fmtExprForRaw(expr.operand);
      return expr.operator + ' ' + fmtExprForRaw(expr.operand);
    case 'function_call': {
      const name = expr.name;
      const distinct = expr.distinct ? 'DISTINCT ' : '';
      const args = expr.args.map(fmtExprForRaw).join(', ');
      let out = name + '(' + distinct + args + ')';
      if (expr.orderBy && expr.orderBy.length > 0) {
        out += ' ORDER BY ' + expr.orderBy.map(i => {
          let s = fmtExprForRaw(i.expr);
          if (i.direction) s += ' ' + i.direction;
          if (i.nulls) s += ` NULLS ${i.nulls}`;
          return s;
        }).join(', ');
      }
      return out;
    }
    case 'subquery':
      return '(' + (expr.query.type === 'select' ? '[SELECT]' : '[QUERY]') + ')';
    case 'case': {
      let out = 'CASE';
      if (expr.operand) out += ' ' + fmtExprForRaw(expr.operand);
      for (const wc of expr.whenClauses) {
        out += ' WHEN ' + fmtExprForRaw(wc.condition) + ' THEN ' + fmtExprForRaw(wc.result);
      }
      if (expr.elseResult) out += ' ELSE ' + fmtExprForRaw(expr.elseResult);
      return out + ' END';
    }
    case 'between': {
      const neg = expr.negated ? 'NOT ' : '';
      return `${fmtExprForRaw(expr.expr)} ${neg}BETWEEN ${fmtExprForRaw(expr.low)} AND ${fmtExprForRaw(expr.high)}`;
    }
    case 'in': {
      const neg = expr.negated ? 'NOT ' : '';
      if ((expr.values as AST.SubqueryExpr).type === 'subquery') {
        return `${fmtExprForRaw(expr.expr)} ${neg}IN (${(expr.values as AST.SubqueryExpr).query.type})`;
      }
      return `${fmtExprForRaw(expr.expr)} ${neg}IN (${(expr.values as AST.Expression[]).map(fmtExprForRaw).join(', ')})`;
    }
    case 'is':
      return `${fmtExprForRaw(expr.expr)} IS ${expr.value}`;
    case 'like': {
      const neg = expr.negated ? 'NOT ' : '';
      let out = `${fmtExprForRaw(expr.expr)} ${neg}LIKE ${fmtExprForRaw(expr.pattern)}`;
      if (expr.escape) out += ` ESCAPE ${fmtExprForRaw(expr.escape)}`;
      return out;
    }
    case 'ilike': {
      const neg = expr.negated ? 'NOT ' : '';
      let out = `${fmtExprForRaw(expr.expr)} ${neg}ILIKE ${fmtExprForRaw(expr.pattern)}`;
      if (expr.escape) out += ` ESCAPE ${fmtExprForRaw(expr.escape)}`;
      return out;
    }
    case 'similar_to': {
      const neg = expr.negated ? 'NOT ' : '';
      return `${fmtExprForRaw(expr.expr)} ${neg}SIMILAR TO ${fmtExprForRaw(expr.pattern)}`;
    }
    case 'exists':
      return `EXISTS (${expr.subquery.query.type})`;
    case 'paren':
      return '(' + fmtExprForRaw(expr.expr) + ')';
    case 'cast':
      return 'CAST(' + fmtExprForRaw(expr.expr) + ' AS ' + expr.targetType + ')';
    case 'pg_cast':
      return fmtExprForRaw(expr.expr as AST.Expression) + '::' + expr.targetType;
    case 'extract':
      return 'EXTRACT(' + expr.field + ' FROM ' + fmtExprForRaw(expr.source) + ')';
    case 'position':
      return `POSITION(${fmtExprForRaw(expr.substring)} IN ${fmtExprForRaw(expr.source)})`;
    case 'substring': {
      let out = `SUBSTRING(${fmtExprForRaw(expr.source)} FROM ${fmtExprForRaw(expr.start)}`;
      if (expr.length) out += ` FOR ${fmtExprForRaw(expr.length)}`;
      return out + ')';
    }
    case 'overlay': {
      let out = `OVERLAY(${fmtExprForRaw(expr.source)} PLACING ${fmtExprForRaw(expr.replacement)} FROM ${fmtExprForRaw(expr.start)}`;
      if (expr.length) out += ` FOR ${fmtExprForRaw(expr.length)}`;
      return out + ')';
    }
    case 'trim': {
      let out = 'TRIM(';
      if (expr.side) {
        out += expr.side;
        if (expr.trimChar) out += ` ${fmtExprForRaw(expr.trimChar)} FROM ${fmtExprForRaw(expr.source)}`;
        else if (expr.fromSyntax) out += ` FROM ${fmtExprForRaw(expr.source)}`;
        else out += ` ${fmtExprForRaw(expr.source)}`;
      } else if (expr.trimChar) {
        out += `${fmtExprForRaw(expr.trimChar)} FROM ${fmtExprForRaw(expr.source)}`;
      } else if (expr.fromSyntax) {
        out += `FROM ${fmtExprForRaw(expr.source)}`;
      } else {
        out += fmtExprForRaw(expr.source);
      }
      return out + ')';
    }
    case 'array_constructor':
      return 'ARRAY[' + expr.elements.map(e => fmtExprForRaw(e as AST.Expression)).join(', ') + ']';
    case 'is_distinct_from': {
      const kw = expr.negated ? 'IS NOT DISTINCT FROM' : 'IS DISTINCT FROM';
      return `${fmtExprForRaw(expr.left as AST.Expression)} ${kw} ${fmtExprForRaw(expr.right as AST.Expression)}`;
    }
    case 'regex_match':
      return `${fmtExprForRaw(expr.left as AST.Expression)} ${expr.operator} ${fmtExprForRaw(expr.right as AST.Expression)}`;
    case 'window_function': {
      const func = fmtExprForRaw(expr.func);
      if (expr.windowName) return `${func} OVER ${expr.windowName}`;
      return `${func} OVER (...)`;
    }
    case 'raw':
      return expr.text;
  }

  return assertNever(expr);
}
