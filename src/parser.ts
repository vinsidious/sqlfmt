import { Token } from './tokenizer';
import * as AST from './ast';

class ParseError extends Error {
  token: Token;
  expected: string;

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

  constructor(tokens: Token[]) {
    // Annotate non-whitespace tokens with blankLinesBefore count, then filter whitespace
    for (let i = 0; i < tokens.length; i++) {
      if (tokens[i].type === 'whitespace') {
        const nlCount = (tokens[i].value.match(/\n/g) || []).length;
        if (nlCount >= 2 && i + 1 < tokens.length) {
          tokens[i + 1].blankLinesBefore = nlCount - 1;
        }
      }
    }
    this.tokens = tokens.filter(t => t.type !== 'whitespace');
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
        if (!(err instanceof ParseError)) throw err;
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
    if (this.peekUpper() === 'WITH') {
      return this.parseCTE(comments);
    }
    if (this.peekUpper() === 'SELECT' || this.check('(')) {
      const query = this.parseUnionOrSelect(comments);
      if (query.type === 'select' || query.type === 'union') return query;
    }
    throw new ParseError('query expression', this.peek());
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
    if (kw === 'UNION' && this.peekUpper() === 'ALL') {
      this.advance();
      return 'UNION ALL';
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

  private parseTableExpr(): AST.Expr {
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

    let on: AST.Expr | undefined;
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
    const items: AST.Expr[] = [];
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
    let partitionBy: AST.Expr[] | undefined;
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
    return { expr, direction };
  }

  // Expression parser using precedence climbing
  private parseExpression(): AST.Expr {
    return this.parseOr();
  }

  private parseOr(): AST.Expr {
    let left = this.parseAnd();
    while (this.peekUpper() === 'OR') {
      this.advance();
      const right = this.parseAnd();
      left = { type: 'binary', left, operator: 'OR', right };
    }
    return left;
  }

  private parseAnd(): AST.Expr {
    let left = this.parseNot();
    while (this.peekUpper() === 'AND' && !this.isPartOfBetween()) {
      this.advance();
      const right = this.parseNot();
      left = { type: 'binary', left, operator: 'AND', right };
    }
    return left;
  }

  private isPartOfBetween(): boolean {
    return false;
  }

  private parseNot(): AST.Expr {
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

  private parseComparison(): AST.Expr {
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
      return { type: 'like', expr: left, pattern, negated };
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
      return { type: 'ilike', expr: left, pattern, negated };
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

  private parseAddSub(): AST.Expr {
    let left = this.parseMulDiv();
    while (this.peek().type === 'operator' && (this.peek().value === '+' || this.peek().value === '-')) {
      const op = this.advance().value;
      const right = this.parseMulDiv();
      left = { type: 'binary', left, operator: op, right };
    }
    return left;
  }

  private parseMulDiv(): AST.Expr {
    let left = this.parseJsonOps();
    while (this.peek().type === 'operator' && (this.peek().value === '*' || this.peek().value === '/' || this.peek().value === '||')) {
      const op = this.advance().value;
      const right = this.parseJsonOps();
      left = { type: 'binary', left, operator: op, right };
    }
    return left;
  }

  // JSON, Array, Bitwise operators
  private parseJsonOps(): AST.Expr {
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

  private parseUnaryExpr(): AST.Expr {
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
  private parsePrimaryWithPostfix(): AST.Expr {
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

  private parsePrimary(): AST.Expr {
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
      return { type: 'raw', text: 'INTERVAL ' + strToken.value };
    }

    // ARRAY[...] constructor
    if (token.upper === 'ARRAY' && this.peekAt(1)?.value === '[') {
      this.advance(); // consume ARRAY
      this.advance(); // consume [
      const elements: AST.Expr[] = [];
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
      const args: AST.Expr[] = [];
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
      return { type: 'raw', text: 'NULL' };
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

    // DATE/TIME/TIMESTAMP type constructor: DATE 'YYYY-MM-DD'
    if ((token.upper === 'DATE' || token.upper === 'TIME' || token.upper === 'TIMESTAMP')
        && this.peekTypeAt(1) === 'string') {
      this.advance();
      const strToken = this.advance();
      return { type: 'raw', text: token.upper + ' ' + strToken.value };
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

  private parseIdentifierOrFunction(): AST.Expr {
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

      const args: AST.Expr[] = [];
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

    let operand: AST.Expr | undefined;
    if (this.peekUpper() !== 'WHEN') {
      operand = this.parseExpression();
    }

    const whenClauses: { condition: AST.Expr; result: AST.Expr }[] = [];
    while (this.peekUpper() === 'WHEN') {
      this.advance();
      const condition = this.parseExpression();
      this.expectKeyword('THEN');
      const result = this.parseExpression();
      whenClauses.push({ condition, result });
    }

    let elseResult: AST.Expr | undefined;
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

  private parsePositionExpr(): AST.Expr {
    this.advance(); // POSITION
    this.expect('(');
    const substr = this.parseAddSub();
    this.expectKeyword('IN');
    const str = this.parseAddSub();
    this.expect(')');
    return { type: 'raw', text: 'POSITION(' + fmtExprForRaw(substr) + ' IN ' + fmtExprForRaw(str) + ')' } as AST.RawExpression;
  }

  private parseSubstringExpr(): AST.Expr {
    this.advance(); // SUBSTRING
    this.expect('(');
    const str = this.parseExpression();
    this.expectKeyword('FROM');
    const start = this.parseExpression();
    let len: AST.Expr | undefined;
    if (this.peekUpper() === 'FOR') {
      this.advance();
      len = this.parseExpression();
    }
    this.expect(')');
    let text = 'SUBSTRING(' + fmtExprForRaw(str) + ' FROM ' + fmtExprForRaw(start);
    if (len) text += ' FOR ' + fmtExprForRaw(len);
    text += ')';
    return { type: 'raw', text } as AST.RawExpression;
  }

  private parseOverlayExpr(): AST.Expr {
    this.advance(); // OVERLAY
    this.expect('(');
    const str = this.parseExpression();
    this.expectKeyword('PLACING');
    const replacement = this.parseExpression();
    this.expectKeyword('FROM');
    const start = this.parseExpression();
    let len: AST.Expr | undefined;
    if (this.peekUpper() === 'FOR') {
      this.advance();
      len = this.parseExpression();
    }
    this.expect(')');
    let text = 'OVERLAY(' + fmtExprForRaw(str) + ' PLACING ' + fmtExprForRaw(replacement) + ' FROM ' + fmtExprForRaw(start);
    if (len) text += ' FOR ' + fmtExprForRaw(len);
    text += ')';
    return { type: 'raw', text } as AST.RawExpression;
  }

  private parseTrimExpr(): AST.Expr {
    this.advance(); // TRIM
    this.expect('(');

    let side = '';
    if (this.peekUpper() === 'LEADING' || this.peekUpper() === 'TRAILING' || this.peekUpper() === 'BOTH') {
      side = this.advance().upper;
    }

    const char = this.parseExpression();
    this.expectKeyword('FROM');
    const str = this.parseExpression();
    this.expect(')');

    let text = 'TRIM(';
    if (side) text += side + ' ';
    text += fmtExprForRaw(char) + ' FROM ' + fmtExprForRaw(str) + ')';
    return { type: 'raw', text } as AST.RawExpression;
  }

  private parseExpressionList(): AST.Expr[] {
    const list: AST.Expr[] = [];
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

    let values: AST.ValuesList[] | undefined;
    let selectQuery: AST.SelectStatement | undefined;

    if (this.peekUpper() === 'VALUES') {
      this.advance();
      values = [];
      values.push(this.parseValuesTuple());
      while (this.check(',')) {
        this.advance();
        values.push(this.parseValuesTuple());
      }
    } else if (this.peekUpper() === 'SELECT') {
      selectQuery = this.parseSelect();
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

    return { type: 'insert', table, columns, values, selectQuery, onConflict, returning, leadingComments: comments };
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

    return { type: 'delete', from: table, where, returning, leadingComments: comments };
  }

  // CREATE TABLE | CREATE INDEX | CREATE VIEW
  private parseCreate(comments: AST.CommentNode[]): AST.Node {
    const createPos = this.pos;
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
    this.expectKeyword('TABLE');

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

    return { type: 'truncate', table: tables.join(', '), restartIdentity, cascade, leadingComments: comments };
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
    this.expectKeyword('TABLE');
    const tableName = this.advance().value;

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

    return { type: 'alter_table', tableName, action, leadingComments: comments };
  }

  // DROP TABLE [IF EXISTS] name
  private parseDrop(comments: AST.CommentNode[]): AST.DropTableStatement {
    this.expectKeyword('DROP');
    this.expectKeyword('TABLE');

    let ifExists = false;
    if (this.peekUpper() === 'IF' && this.peekUpperAt(1) === 'EXISTS') {
      this.advance(); this.advance();
      ifExists = true;
    }

    const tableName = this.advance().value;

    return { type: 'drop_table', ifExists, tableName, leadingComments: comments };
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
    const t = this.advance();
    return t.type === 'keyword' ? t.upper : t.value;
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
        blankLinesBefore: t.blankLinesBefore || 0,
      });
    }
    return comments;
  }

  private isClauseKeyword(): boolean {
    return this.isClauseKeywordValue(this.peekUpper());
  }

  private isClauseKeywordValue(val: string): boolean {
    return [
      'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
      'UNION', 'INTERSECT', 'EXCEPT', 'ON', 'SET', 'VALUES',
      'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'JOIN',
      'INTO', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER',
      'DROP', 'WITH', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR',
      'RETURNING', 'FETCH', 'WINDOW', 'LATERAL',
    ].includes(val);
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
    return this.peek().value === value || this.peek().upper === value;
  }

  private advance(): Token {
    const token = this.tokens[this.pos];
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
    return this.expect(keyword);
  }

  private isAtEnd(): boolean {
    return this.pos >= this.tokens.length || this.tokens[this.pos].type === 'eof';
  }
}

// Helper for building raw text from AST during parse
function fmtExprForRaw(expr: AST.Expr): string {
  switch (expr.type) {
    case 'identifier':
      return expr.quoted ? expr.value : expr.value.toLowerCase();
    case 'literal':
      if (expr.literalType === 'boolean') return expr.value.toUpperCase();
      return expr.value;
    case 'star':
      return expr.qualifier ? expr.qualifier + '.*' : '*';
    case 'binary':
      return fmtExprForRaw(expr.left) + ' ' + expr.operator + ' ' + fmtExprForRaw(expr.right);
    case 'unary':
      if (expr.operator === '-') return '-' + fmtExprForRaw(expr.operand);
      if (expr.operator === '~') return '~' + fmtExprForRaw(expr.operand);
      return expr.operator + ' ' + fmtExprForRaw(expr.operand);
    case 'function_call': {
      const name = expr.name.toUpperCase();
      const distinct = expr.distinct ? 'DISTINCT ' : '';
      const args = expr.args.map(fmtExprForRaw).join(', ');
      return name + '(' + distinct + args + ')';
    }
    case 'paren':
      return '(' + fmtExprForRaw(expr.expr) + ')';
    case 'cast':
      return 'CAST(' + fmtExprForRaw(expr.expr) + ' AS ' + expr.targetType + ')';
    case 'pg_cast':
      return fmtExprForRaw(expr.expr as AST.Expr) + '::' + expr.targetType;
    case 'raw':
      return expr.text;
    case 'extract':
      return 'EXTRACT(' + expr.field + ' FROM ' + fmtExprForRaw(expr.source) + ')';
    case 'array_constructor':
      return 'ARRAY[' + expr.elements.map(e => fmtExprForRaw(e as AST.Expr)).join(', ') + ']';
    default:
      return String((expr as any).text || (expr as any).value || '');
  }
}
