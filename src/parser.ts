import { Token } from './tokenizer';
import * as AST from './ast';

export class Parser {
  private tokens: Token[];
  private pos: number = 0;

  constructor(tokens: Token[]) {
    // Annotate non-whitespace tokens with blankLinesBefore count, then filter whitespace
    for (let i = 0; i < tokens.length; i++) {
      if (tokens[i].type === 'whitespace') {
        const nlCount = (tokens[i].value.match(/\n/g) || []).length;
        if (nlCount >= 2 && i + 1 < tokens.length) {
          // nlCount - 1 = number of blank lines (e.g., 2 newlines = 1 blank line)
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
      const stmt = this.parseStatement();
      if (stmt) stmts.push(stmt);
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

    // Check for parenthesized query (could be UNION)
    if (this.check('(') && this.looksLikeParenthesizedSelect()) {
      return this.parseUnionOrSelect(comments);
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

    // Unknown statement — consume until semicolon
    let raw = '';
    while (!this.isAtEnd() && !this.check(';')) {
      raw += this.advance().value + ' ';
    }
    return { type: 'raw', text: raw.trim() } as AST.RawExpression;
  }

  private looksLikeParenthesizedSelect(): boolean {
    // Look ahead past '(' for SELECT
    let depth = 0;
    for (let i = this.pos; i < this.tokens.length; i++) {
      const t = this.tokens[i];
      if (t.type === 'line_comment' || t.type === 'block_comment') continue;
      if (t.value === '(') { depth++; continue; }
      if (depth === 1 && t.upper === 'SELECT') return true;
      if (depth === 1 && t.upper !== 'SELECT') return false;
      if (t.value === ')') return false;
    }
    return false;
  }

  private parseUnionOrSelect(comments: AST.CommentNode[]): AST.Node {
    const first = this.parseSelectOrParenSelect();
    first.leadingComments = comments;

    // Check for UNION / INTERSECT / EXCEPT
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
    const joins: AST.JoinClause[] = [];
    let where: AST.WhereClause | undefined;
    let groupBy: AST.GroupByClause | undefined;
    let having: AST.HavingClause | undefined;
    let orderBy: AST.OrderByClause | undefined;
    let limit: AST.LimitClause | undefined;
    let offset: AST.OffsetClause | undefined;

    if (this.peekUpper() === 'FROM') {
      this.advance();
      from = this.parseFromClause();

      // Parse JOINs
      while (this.isJoinKeyword()) {
        joins.push(this.parseJoin());
      }
    }

    if (this.peekUpper() === 'WHERE') {
      this.advance();
      where = { condition: this.parseExpression() };
      // Capture trailing comment on WHERE line (e.g., WHERE x > 5  -- comment)
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
      groupBy = { items: this.parseExpressionList() };
    }

    if (this.peekUpper() === 'HAVING') {
      this.advance();
      having = { condition: this.parseExpression() };
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
      offset = { count: this.parsePrimary() };
    }

    return {
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
      leadingComments: [],
    };
  }

  private parseColumnList(): AST.ColumnExpr[] {
    const columns: AST.ColumnExpr[] = [];
    columns.push(this.parseColumnExpr());

    while (this.check(',')) {
      this.advance(); // consume comma
      // A line comment after a comma belongs to the previous column
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
      // Implicit alias — identifier right after expression
      alias = this.advance().value;
    }

    // Check for trailing line comment
    if (this.peekType() === 'line_comment') {
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: this.advance().value,
      };
    }

    return { expr, alias, trailingComment };
  }

  private parseFromClause(): AST.FromClause {
    const table = this.parseTableExpr();
    let alias: string | undefined;

    if (this.peekUpper() === 'AS') {
      this.advance();
      alias = this.advance().value;
    } else if (this.peekType() === 'identifier' && !this.isClauseKeyword() && !this.isJoinKeyword() && !this.check(',') && !this.check(')') && !this.check(';')) {
      alias = this.advance().value;
    }

    return { table, alias };
  }

  private parseTableExpr(): AST.Expr {
    if (this.check('(')) {
      // Could be subquery
      if (this.looksLikeSubqueryAtCurrent()) {
        return this.parseSubquery();
      }
      // Otherwise paren expression
      this.advance();
      const expr = this.parseExpression();
      this.expect(')');
      return { type: 'paren', expr } as AST.ParenExpr;
    }
    return this.parsePrimary();
  }

  private looksLikeSubqueryAtCurrent(): boolean {
    let depth = 0;
    for (let i = this.pos; i < this.tokens.length; i++) {
      const t = this.tokens[i];
      if (t.type === 'line_comment' || t.type === 'block_comment') continue;
      if (t.value === '(') { depth++; continue; }
      if (depth === 1 && t.upper === 'SELECT') return true;
      if (depth === 1) return false;
      if (t.value === ')') return false;
    }
    return false;
  }

  private parseSubquery(): AST.SubqueryExpr {
    this.expect('(');
    const query = this.parseSelect();
    this.expect(')');
    return { type: 'subquery', query };
  }

  private isJoinKeyword(): boolean {
    const kw = this.peekUpper();
    if (kw === 'JOIN') return true;
    if (kw === 'INNER' || kw === 'LEFT' || kw === 'RIGHT' || kw === 'FULL' || kw === 'CROSS' || kw === 'NATURAL') {
      // Look ahead for JOIN
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
    // Consume join type keywords
    while (this.peekUpper() !== 'JOIN') {
      joinType += this.advance().upper + ' ';
    }
    joinType += this.advance().upper; // consume JOIN

    const table = this.parseTableExpr();
    let alias: string | undefined;

    if (this.peekUpper() === 'AS') {
      this.advance();
      alias = this.advance().value;
    } else if (this.peekType() === 'identifier' && !this.isClauseKeyword() && !this.isJoinKeyword() && this.peekUpper() !== 'ON' && !this.check(',') && !this.check(')') && !this.check(';')) {
      alias = this.advance().value;
    }

    let on: AST.Expr | undefined;
    if (this.peekUpper() === 'ON') {
      this.advance();
      on = this.parseExpression();
    }

    return { joinType: joinType.trim(), table, alias, on };
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
    // Walk back to check if the last non-comment token before AND was part of BETWEEN
    // Simple heuristic: if the previous parsed expression was via BETWEEN, the AND is part of it
    // Actually, BETWEEN handling is done in parseComparison, so AND at this level is always logical AND
    return false;
  }

  private parseNot(): AST.Expr {
    if (this.peekUpper() === 'NOT') {
      this.advance();

      // NOT IN, NOT LIKE, NOT BETWEEN, NOT EXISTS, NOT NULL
      if (this.peekUpper() === 'IN' || this.peekUpper() === 'LIKE' || this.peekUpper() === 'BETWEEN' || this.peekUpper() === 'EXISTS') {
        // Put NOT back — let comparison handle it
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

    // IS [NOT] NULL
    if (this.peekUpper() === 'IS') {
      this.advance();
      if (this.peekUpper() === 'NOT') {
        this.advance();
        if (this.peekUpper() === 'NULL') {
          this.advance();
          return { type: 'is', expr: left, value: 'NOT NULL' };
        }
      } else if (this.peekUpper() === 'NULL') {
        this.advance();
        return { type: 'is', expr: left, value: 'NULL' };
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
      if (this.peekUpper() === 'SELECT') {
        const query = this.parseSelect();
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

    // [NOT] EXISTS is handled as a primary expression

    // Comparison operators
    if (this.checkOperator()) {
      const op = this.advance().value;
      const right = this.parseAddSub();
      return { type: 'binary', left, operator: op, right };
    }

    return left;
  }

  private checkOperator(): boolean {
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
    let left = this.parseUnaryExpr();
    while (this.peek().type === 'operator' && (this.peek().value === '*' || this.peek().value === '/' || this.peek().value === '||')) {
      const op = this.advance().value;
      const right = this.parseUnaryExpr();
      left = { type: 'binary', left, operator: op, right };
    }
    return left;
  }

  private parseUnaryExpr(): AST.Expr {
    if (this.peek().type === 'operator' && this.peek().value === '-') {
      this.advance();
      const operand = this.parsePrimary();
      return { type: 'unary', operator: '-', operand };
    }
    return this.parsePrimary();
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

    // EXTRACT(field FROM expr) - special syntax
    if (token.upper === 'EXTRACT' && this.peekTypeAt(1) !== 'punctuation' ||
        (token.upper === 'EXTRACT' && this.peekUpperAt(1) === '(')) {
      return this.parseExtract();
    }

    // Paren expression or subquery
    if (token.value === '(') {
      if (this.looksLikeSubqueryAtCurrent()) {
        return this.parseSubquery();
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
      return { type: 'identifier', value: 'NULL', quoted: false };
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
      this.advance(); // consume DATE/TIME/TIMESTAMP
      const strToken = this.advance();
      return { type: 'raw', text: token.upper + ' ' + strToken.value };
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

      // Check for DISTINCT in function call (e.g., COUNT(DISTINCT x))
      let distinct = false;
      if (this.peekUpper() === 'DISTINCT') {
        this.advance();
        distinct = true;
      }

      const args: AST.Expr[] = [];
      if (!this.check(')')) {
        // Handle special case: * in COUNT(*)
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
      this.expect(')');

      const funcExpr: AST.FunctionCallExpr = { type: 'function_call', name: fullName, args, distinct };

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
    this.expect('(');

    let partitionBy: AST.Expr[] | undefined;
    let orderBy: AST.OrderByItem[] | undefined;
    let frame: string | undefined;

    if (this.peekUpper() === 'PARTITION' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      partitionBy = this.parseExpressionList();
    }

    if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      orderBy = this.parseOrderByItems();
    }

    // Frame clause: ROWS/RANGE BETWEEN ... AND ...
    if (this.peekUpper() === 'ROWS' || this.peekUpper() === 'RANGE') {
      let frameStr = this.advance().upper;
      if (this.peekUpper() === 'BETWEEN') {
        frameStr += ' ' + this.advance().upper;
        // Consume until AND
        while (this.peekUpper() !== 'AND') {
          frameStr += ' ' + this.advance().upper;
        }
        frameStr += ' ' + this.advance().upper; // AND
        // Consume rest until )
        while (!this.check(')')) {
          frameStr += ' ' + this.advance().upper;
        }
      }
      frame = frameStr;
    }

    this.expect(')');

    return { type: 'window_function', func, partitionBy, orderBy, frame };
  }

  private parseCaseExpr(): AST.CaseExpr {
    this.expectKeyword('CASE');

    let operand: AST.Expr | undefined;
    // Simple CASE: CASE expr WHEN ...
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
    // Parse target type (can be complex: DECIMAL(10, 2))
    let targetType = this.advance().upper;
    if (this.check('(')) {
      targetType += this.advance().value;
      while (!this.check(')')) {
        const t = this.advance();
        targetType += t.value;
        if (this.check(',')) {
          targetType += this.advance().value;
          // skip space
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
    // EXTRACT(field FROM expr)
    const field = this.advance().upper; // DAY, MONTH, YEAR, etc.
    this.expectKeyword('FROM');
    const source = this.parseExpression();
    this.expect(')');
    return { type: 'extract', field, source };
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

    // VALUES or SELECT
    if (this.peekUpper() === 'VALUES') {
      this.advance();
      const values: AST.ValuesList[] = [];
      values.push(this.parseValuesTuple());
      while (this.check(',')) {
        this.advance();
        values.push(this.parseValuesTuple());
      }
      return { type: 'insert', table, columns, values, leadingComments: comments };
    }

    if (this.peekUpper() === 'SELECT') {
      const selectQuery = this.parseSelect();
      return { type: 'insert', table, columns, selectQuery, leadingComments: comments };
    }

    return { type: 'insert', table, columns, leadingComments: comments };
  }

  private parseValuesTuple(): AST.ValuesList {
    this.expect('(');
    const values = this.parseExpressionList();
    this.expect(')');
    return { values };
  }

  // UPDATE table SET col = val, ... WHERE ...
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

    let where: AST.WhereClause | undefined;
    if (this.peekUpper() === 'WHERE') {
      this.advance();
      where = { condition: this.parseExpression() };
    }

    return { type: 'update', table, setItems, where, leadingComments: comments };
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

    return { type: 'delete', from: table, where, leadingComments: comments };
  }

  // CREATE TABLE name (...)
  private parseCreate(comments: AST.CommentNode[]): AST.CreateTableStatement {
    this.expectKeyword('CREATE');
    this.expectKeyword('TABLE');
    const tableName = this.advance().value;

    this.expect('(');
    const elements = this.parseTableElements();
    this.expect(')');

    return { type: 'create_table', tableName, elements, leadingComments: comments };
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
    // PRIMARY KEY
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

    // CONSTRAINT
    if (this.peekUpper() === 'CONSTRAINT') {
      this.advance();
      const constraintName = this.advance().value;

      // CHECK constraint
      if (this.peekUpper() === 'CHECK') {
        this.advance();
        let body = 'CHECK';
        let depth = 0;
        // Consume CHECK(...)
        if (this.check('(')) {
          body += this.advance().value;
          depth = 1;
          while (depth > 0 && !this.isAtEnd()) {
            const t = this.advance();
            if (t.value === '(') depth++;
            if (t.value === ')') depth--;
            if (depth > 0) {
              body += (t.type === 'keyword') ? t.upper : t.value;
              // Add space between tokens
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

      // FOREIGN KEY
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

        // ON DELETE / ON UPDATE
        let actions = '';
        while (this.peekUpper() === 'ON') {
          this.advance();
          const actionType = this.advance().upper; // DELETE or UPDATE
          const actionValue = this.advance().upper; // CASCADE, SET NULL, etc
          actions += `ON ${actionType} ${actionValue}`;
          // Handle multi-word values like SET NULL
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

      // Other constraint
      let raw = `CONSTRAINT ${constraintName}`;
      while (!this.check(',') && !this.check(')') && !this.isAtEnd()) {
        raw += ' ' + this.advance().value;
      }
      return { elementType: 'constraint', raw, constraintName };
    }

    // Regular column definition
    const colName = this.advance().value;
    // Type
    let dataType = this.advance().upper;
    // Type parameters like INT(5) or VARCHAR(100) or DECIMAL(10, 2)
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

    // Column constraints: NOT NULL, DEFAULT, UNIQUE, etc.
    let constraints = '';
    while (!this.check(',') && !this.check(')') && !this.isAtEnd()) {
      const kw = this.peekUpper();
      if (kw === 'CONSTRAINT' || kw === 'PRIMARY') break;
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

    // Consume the rest as raw action, preserving paren grouping (e.g., VARCHAR(255))
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

  // CTE: WITH name AS (...), name AS (...) SELECT ...
  private parseCTE(comments: AST.CommentNode[]): AST.CTEStatement {
    this.expectKeyword('WITH');

    let _recursive = false;
    if (this.peekUpper() === 'RECURSIVE') {
      this.advance();
      _recursive = true;
    }

    const ctes: AST.CTEDefinition[] = [];

    ctes.push(this.parseCTEDefinition());
    while (this.check(',')) {
      this.advance();
      // Comments between CTEs are consumed as leading comments of the next CTE
      ctes.push(this.parseCTEDefinition());
    }

    // Parse the main query
    let mainQuery: AST.SelectStatement | AST.UnionStatement;
    const mainComments = this.consumeComments();

    if (this.check('(') && this.looksLikeParenthesizedSelect()) {
      const result = this.parseUnionOrSelect(mainComments);
      mainQuery = result as AST.SelectStatement | AST.UnionStatement;
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

    return { type: 'cte', ctes, mainQuery, leadingComments: comments };
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
    this.expect('(');

    // CTE body can be VALUES, SELECT, or UNION
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
    return { name, columnList, query, leadingComments };
  }

  // Check if current ( is the start of CTE body (AS (...)) vs column list
  private looksLikeCTEBodyStart(): boolean {
    // If next non-comment token after ( is SELECT or VALUES, it's a body
    let depth = 0;
    for (let i = this.pos; i < this.tokens.length; i++) {
      const t = this.tokens[i];
      if (t.type === 'line_comment' || t.type === 'block_comment') continue;
      if (t.value === '(') { depth++; continue; }
      if (depth === 1) {
        // First real token inside parens
        if (t.upper === 'SELECT' || t.upper === 'VALUES') return true;
        return false;
      }
      if (t.value === ')') return false;
    }
    return false;
  }

  // Look ahead to see if VALUES keyword appears before any SELECT
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

    // May have comments before VALUES keyword
    if (this.peekUpper() === 'VALUES') {
      this.advance(); // consume VALUES
    }

    const rows: AST.ValuesRow[] = [];
    // Parse value rows, handling interleaved comments
    while (!this.check(')') && !this.isAtEnd()) {
      const rowComments = this.consumeComments();
      if (this.check('(')) {
        this.advance(); // consume (
        const values = this.parseExpressionList();
        this.expect(')');

        // Check for trailing comment
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
        // End of VALUES
        // Attach any trailing comments to the last row
        if (rows.length > 0 && rowComments.length > 0) {
          if (!rows[rows.length - 1].leadingComments) {
            rows[rows.length - 1].leadingComments = [];
          }
          // These are actually trailing comments after the last row
          // Add them as a new empty row with just comments, or attach to last row
          rows.push({ values: [], leadingComments: rowComments });
        }
        break;
      } else {
        break;
      }
    }

    return { type: 'values', rows, leadingComments };
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

  // Consume a trailing line comment if present (e.g., after WHERE condition)
  private consumeTrailingLineComment(): void {
    if (this.peekType() === 'line_comment') {
      this.advance();
    }
  }

  // Peek past any comments to find the next keyword
  private peekUpperSkipComments(): string {
    let i = this.pos;
    while (i < this.tokens.length) {
      const t = this.tokens[i];
      if (t.type === 'line_comment' || t.type === 'block_comment') {
        i++;
        continue;
      }
      return t.upper;
    }
    return '';
  }

  // After finding the target keyword, peek at offset past it (skipping comments)
  private peekUpperAfterAt(keyword: string, offset: number): string {
    let i = this.pos;
    // Skip to keyword
    while (i < this.tokens.length) {
      const t = this.tokens[i];
      if (t.type === 'line_comment' || t.type === 'block_comment') {
        i++;
        continue;
      }
      if (t.upper === keyword) break;
      return '';
    }
    // Now skip 'offset' non-comment tokens after it
    i++;
    let count = 0;
    while (i < this.tokens.length && count < offset) {
      if (this.tokens[i].type !== 'line_comment' && this.tokens[i].type !== 'block_comment') {
        count++;
        if (count === offset) return this.tokens[i].upper;
      }
      i++;
    }
    return '';
  }

  // Skip comment tokens to reach the target keyword
  private skipToKeyword(keyword: string): void {
    while (this.pos < this.tokens.length) {
      const t = this.tokens[this.pos];
      if (t.type === 'line_comment' || t.type === 'block_comment') {
        this.pos++;
        continue;
      }
      if (t.upper === keyword) break;
      break;
    }
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
    ].includes(val);
  }

  private peek(): Token {
    if (this.pos >= this.tokens.length) {
      return { type: 'eof', value: '', upper: '', position: -1 };
    }
    return this.tokens[this.pos];
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
      // Lenient: just return what we have
      return token;
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
