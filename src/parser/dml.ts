import * as AST from '../ast';
import type { Token } from '../tokenizer';

export interface DmlParser {
  expect(value: string): Token;
  advance(): Token;
  check(value: string): boolean;
  peekUpper(): string;
  peekUpperAt(offset: number): string;
  isAtEnd(): boolean;
  hasImplicitStatementBoundary?(): boolean;
  tokensToSql?(tokens: Token[]): string;
  isJoinKeyword(): boolean;
  parseJoin(): AST.JoinClause;
  parseExpression(): AST.Expression;
  parseExpressionList(): AST.Expression[];
  parseReturningList(): AST.Expression[];
  parseFromItem(): AST.FromClause;
  parseQueryExpression(): AST.QueryExpression;
  tryParseQueryExpressionAtCurrent?(): AST.QueryExpression | null;
  consumeComments?: () => AST.CommentNode[];
}

const SQLITE_INSERT_OR_ACTIONS = new Set<NonNullable<AST.InsertStatement['orConflictAction']>>([
  'ROLLBACK',
  'ABORT',
  'FAIL',
  'IGNORE',
  'REPLACE',
]);

export function parseInsertStatement(
  ctx: DmlParser,
  comments: AST.CommentNode[]
): AST.InsertStatement {
  ctx.expect('INSERT');
  let ignore = false;
  let orConflictAction: AST.InsertStatement['orConflictAction'];
  if (ctx.peekUpper() === 'OR') {
    ctx.advance();
    const action = ctx.peekUpper() as NonNullable<AST.InsertStatement['orConflictAction']>;
    if (!SQLITE_INSERT_OR_ACTIONS.has(action)) {
      ctx.expect('IGNORE');
    }
    orConflictAction = ctx.advance().upper as NonNullable<AST.InsertStatement['orConflictAction']>;
  } else if (ctx.peekUpper() === 'IGNORE') {
    ctx.advance();
    ignore = true;
  }
  if (ctx.peekUpper() === 'INTO') {
    ctx.advance();
  }
  let table = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance(); // consume dot
    table += '.' + ctx.advance().value;
  }
  const alias = parseOptionalTableAlias(
    ctx,
    new Set(['OVERRIDING', 'DEFAULT', 'VALUE', 'VALUES', 'SET', 'TABLE', 'SELECT', 'WITH', 'ON', 'RETURNING', 'EXEC', 'EXECUTE'])
  );

  let columns: string[] = [];
  let selectQuery: AST.QueryExpression | undefined;
  let setItems: AST.SetItem[] | undefined;
  let valuesAlias: AST.InsertStatement['valuesAlias'];
  let executeSource: string | undefined;
  let tableSource: AST.InsertStatement['tableSource'];
  if (ctx.check('(')) {
    const queryAtCurrent = ctx.tryParseQueryExpressionAtCurrent?.();
    if (queryAtCurrent) {
      selectQuery = queryAtCurrent;
    } else {
      columns = parseParenthesizedIdentifierList(ctx);
    }
  }

  let overriding: AST.InsertStatement['overriding'];
  if (ctx.peekUpper() === 'OVERRIDING') {
    ctx.advance();
    if (ctx.peekUpper() === 'SYSTEM' && ctx.peekUpperAt(1) === 'VALUE') {
      ctx.advance();
      ctx.advance();
      overriding = 'SYSTEM VALUE';
    } else if (ctx.peekUpper() === 'USER' && ctx.peekUpperAt(1) === 'VALUE') {
      ctx.advance();
      ctx.advance();
      overriding = 'USER VALUE';
    } else {
      ctx.expect('SYSTEM');
    }
  }

  let defaultValues = false;
  const valueClauseLeadingComments: AST.CommentNode[] = ctx.consumeComments?.() ?? [];
  let values: AST.ValuesList[] | undefined;

  if (selectQuery) {
    // already parsed from a parenthesized query source after table name
  } else if (matchesKeywords(ctx, 'DEFAULT', 'VALUES')) {
    ctx.advance();
    ctx.advance();
    defaultValues = true;
  } else if (ctx.peekUpper() === 'VALUES' || ctx.peekUpper() === 'VALUE') {
    ctx.advance();
    values = [];
    values.push(parseValuesTuple(ctx, ctx.consumeComments?.() ?? []));
    while (true) {
      ctx.consumeComments?.();
      if (!ctx.check(',')) break;
      ctx.advance();
      values.push(parseValuesTuple(ctx, ctx.consumeComments?.() ?? []));
    }
    valuesAlias = parseOptionalInsertSourceAlias(ctx);
  } else if (ctx.peekUpper() === 'SET') {
    ctx.advance();
    setItems = [];
    ctx.consumeComments?.();
    setItems.push(parseSetItem(ctx));
    ctx.consumeComments?.();
    while (ctx.check(',')) {
      ctx.advance();
      ctx.consumeComments?.();
      setItems.push(parseSetItem(ctx));
      ctx.consumeComments?.();
    }
    valuesAlias = parseOptionalInsertSourceAlias(ctx);
  } else if (ctx.peekUpper() === 'TABLE') {
    ctx.advance();
    const sourceTable = parseDottedName(ctx);
    let sourceAlias: string | undefined;
    let sourceAliasColumns: string[] | undefined;
    if (ctx.peekUpper() === 'AS') {
      ctx.advance();
      sourceAlias = ctx.advance().value;
      if (ctx.check('(')) {
        sourceAliasColumns = parseParenthesizedIdentifierList(ctx);
      }
    }
    tableSource = {
      table: sourceTable,
      alias: sourceAlias,
      aliasColumns: sourceAliasColumns && sourceAliasColumns.length > 0 ? sourceAliasColumns : undefined,
    };
  } else if (ctx.peekUpper() === 'SELECT' || ctx.peekUpper() === 'WITH' || ctx.check('(')) {
    selectQuery = ctx.parseQueryExpression();
  } else if (ctx.peekUpper() === 'EXEC' || ctx.peekUpper() === 'EXECUTE') {
    executeSource = parseInsertExecuteSource(ctx);
  }

  let onConflict: AST.InsertStatement['onConflict'];
  if (ctx.peekUpper() === 'ON' && ctx.peekUpperAt(1) === 'CONFLICT') {
    ctx.advance();
    ctx.advance();
    onConflict = parseInsertOnConflictClause(ctx);
  }

  let onDuplicateKeyUpdate: AST.SetItem[] | undefined;
  if (
    ctx.peekUpper() === 'ON'
    && ctx.peekUpperAt(1) === 'DUPLICATE'
    && ctx.peekUpperAt(2) === 'KEY'
    && ctx.peekUpperAt(3) === 'UPDATE'
  ) {
    ctx.advance();
    ctx.advance();
    ctx.advance();
    ctx.advance();
    onDuplicateKeyUpdate = [];
    ctx.consumeComments?.();
    onDuplicateKeyUpdate.push(parseSetItem(ctx));
    ctx.consumeComments?.();
    while (ctx.check(',')) {
      ctx.advance();
      ctx.consumeComments?.();
      onDuplicateKeyUpdate.push(parseSetItem(ctx));
      ctx.consumeComments?.();
    }
  }

  const returningClause = parseInsertReturningClause(ctx);

  return {
    type: 'insert',
    ignore: ignore || undefined,
    orConflictAction,
    table,
    alias,
    columns,
    overriding,
    valueClauseLeadingComments: valueClauseLeadingComments.length > 0 ? valueClauseLeadingComments : undefined,
    defaultValues,
    values,
    setItems,
    valuesAlias,
    executeSource,
    tableSource,
    selectQuery,
    onConflict,
    onDuplicateKeyUpdate,
    returning: returningClause.returning,
    returningInto: returningClause.returningInto,
    leadingComments: comments,
  };
}

function parseInsertExecuteSource(ctx: DmlParser): string {
  const tokens: Token[] = [ctx.advance()];
  let depth = 0;

  while (!ctx.isAtEnd() && !ctx.check(';')) {
    if (depth === 0 && ctx.hasImplicitStatementBoundary?.()) break;
    const token = ctx.advance();
    tokens.push(token);
    if (token.value === '(' || token.value === '[' || token.value === '{') {
      depth++;
    } else if (token.value === ')' || token.value === ']' || token.value === '}') {
      depth = Math.max(0, depth - 1);
    }
  }

  if (ctx.tokensToSql) {
    return ctx.tokensToSql(tokens);
  }
  return tokens.map(token => token.type === 'keyword' ? token.upper : token.value).join(' ');
}

export function parseInsertOnConflictClause(ctx: DmlParser): AST.InsertStatement['onConflict'] {
  let conflictColumns: string[] | undefined;
  let constraintName: string | undefined;

  if (ctx.peekUpper() === 'ON' && ctx.peekUpperAt(1) === 'CONSTRAINT') {
    ctx.advance();
    ctx.advance();
    constraintName = ctx.advance().value;
  } else if (ctx.check('(')) {
    conflictColumns = parseParenthesizedIdentifierList(ctx);
  }

  ctx.expect('DO');
  if (ctx.peekUpper() === 'NOTHING') {
    ctx.advance();
    return { columns: conflictColumns, constraintName, action: 'nothing' };
  }

  ctx.expect('UPDATE');
  ctx.expect('SET');
  const setItems: AST.SetItem[] = [];
  ctx.consumeComments?.();
  setItems.push(parseSetItem(ctx));
  ctx.consumeComments?.();
  while (ctx.check(',')) {
    ctx.advance();
    ctx.consumeComments?.();
    setItems.push(parseSetItem(ctx));
    ctx.consumeComments?.();
  }

  let where: AST.Expression | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = ctx.parseExpression();
  }

  return { columns: conflictColumns, constraintName, action: 'update', setItems, where };
}

export function parseValuesTuple(
  ctx: DmlParser,
  leadingComments: AST.CommentNode[] = [],
): AST.ValuesList {
  ctx.expect('(');
  const values = ctx.check(')') ? [] : ctx.parseExpressionList();
  ctx.expect(')');
  const trailingComments = ctx.consumeComments?.() ?? [];
  return {
    values,
    leadingComments: leadingComments.length > 0 ? leadingComments : undefined,
    trailingComments: trailingComments.length > 0 ? trailingComments : undefined,
  };
}

function parseInsertReturningClause(
  ctx: DmlParser
): { returning?: AST.Expression[]; returningInto?: string[] } {
  if (ctx.peekUpper() !== 'RETURNING') return {};
  ctx.advance();
  const returning = ctx.parseReturningList();

  let returningInto: string[] | undefined;
  if (ctx.peekUpper() === 'INTO') {
    ctx.advance();
    returningInto = [parseReturningIntoTarget(ctx)];
    while (ctx.check(',')) {
      ctx.advance();
      returningInto.push(parseReturningIntoTarget(ctx));
    }
  }

  return { returning, returningInto };
}

function parseReturningIntoTarget(ctx: DmlParser): string {
  let target = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance();
    target += '.' + ctx.advance().value;
  }
  return target;
}

export function parseUpdateStatement(
  ctx: DmlParser,
  comments: AST.CommentNode[]
): AST.UpdateStatement {
  ctx.expect('UPDATE');
  const table = parseDottedName(ctx);
  const alias = parseOptionalTableAlias(ctx, new Set(['SET', ',']));

  const additionalTables: Array<{ table: string; alias?: string }> = [];
  while (ctx.check(',')) {
    ctx.advance();
    const additionalTable = parseDottedName(ctx);
    const additionalAlias = parseOptionalTableAlias(ctx, new Set(['SET', ',']));
    additionalTables.push({ table: additionalTable, alias: additionalAlias });
  }

  let joinSources: AST.JoinClause[] | undefined;
  if (ctx.isJoinKeyword()) {
    const joins: AST.JoinClause[] = [];
    while (ctx.isJoinKeyword()) {
      joins.push(ctx.parseJoin());
    }
    if (joins.length > 0) joinSources = joins;
  }

  ctx.expect('SET');
  const setItems: AST.SetItem[] = [];
  ctx.consumeComments?.();
  setItems.push(parseSetItem(ctx));
  ctx.consumeComments?.();
  while (ctx.check(',')) {
    ctx.advance();
    ctx.consumeComments?.();
    setItems.push(parseSetItem(ctx));
    ctx.consumeComments?.();
  }

  let from: AST.FromClause[] | undefined;
  let fromJoins: AST.JoinClause[] | undefined;
  if (ctx.peekUpper() === 'FROM') {
    ctx.advance();
    from = [ctx.parseFromItem()];
    while (ctx.check(',')) {
      ctx.advance();
      from.push(ctx.parseFromItem());
    }
    const joins: AST.JoinClause[] = [];
    while (ctx.isJoinKeyword()) {
      joins.push(ctx.parseJoin());
    }
    if (joins.length > 0) fromJoins = joins;
  }

  let where: AST.WhereClause | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = { condition: ctx.parseExpression() };
  }

  const returning = parseOptionalReturning(ctx);

  return {
    type: 'update',
    table,
    alias,
    additionalTables: additionalTables.length > 0 ? additionalTables : undefined,
    joinSources,
    setItems,
    from,
    fromJoins,
    where,
    returning,
    leadingComments: comments,
  };
}

export function parseSetItem(ctx: DmlParser): AST.SetItem {
  ctx.consumeComments?.();
  let column = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance(); // consume dot
    column += '.' + ctx.advance().value;
  }

   // T-SQL XML method syntax in SET: column.method(...)
  if (ctx.check('(')) {
    ctx.advance();
    const args: AST.Expression[] = [];
    if (!ctx.check(')')) {
      args.push(ctx.parseExpression());
      while (ctx.check(',')) {
        ctx.advance();
        args.push(ctx.parseExpression());
      }
    }
    ctx.expect(')');
    return {
      column,
      methodCall: true,
      value: {
        type: 'function_call',
        name: column,
        args,
        distinct: false,
      },
    };
  }

  ctx.consumeComments?.();
  let assignmentOperator: AST.SetItem['assignmentOperator'] = '=';
  if (
    ctx.check('+=')
    || ctx.check('-=')
    || ctx.check('*=')
    || ctx.check('/=')
    || ctx.check('%=')
    || ctx.check('&=')
    || ctx.check('^=')
    || ctx.check('|=')
  ) {
    assignmentOperator = ctx.advance().value as AST.SetItem['assignmentOperator'];
  } else {
    ctx.expect('=');
  }
  ctx.consumeComments?.();
  const value = ctx.parseExpression();
  ctx.consumeComments?.();
  return {
    column,
    value,
    assignmentOperator: assignmentOperator === '=' ? undefined : assignmentOperator,
  };
}

export function parseDeleteStatement(
  ctx: DmlParser,
  comments: AST.CommentNode[]
): AST.DeleteStatement {
  ctx.expect('DELETE');
  let targets: string[] | undefined;
  let table: string;
  if (ctx.peekUpper() !== 'FROM') {
    const first = parseDottedName(ctx);
    const parsedTargets = [first];
    while (ctx.check(',')) {
      ctx.advance();
      parsedTargets.push(parseDottedName(ctx));
    }

    if (ctx.peekUpper() === 'FROM') {
      targets = parsedTargets;
      ctx.advance();
      table = parseDottedName(ctx);
    } else if (
      parsedTargets.length === 1
      && (ctx.check(';') || ctx.isAtEnd() || ctx.peekUpper() === 'WHERE' || ctx.peekUpper() === 'RETURNING')
    ) {
      // Oracle shorthand: DELETE table_name [WHERE ...]
      table = first;
    } else {
      ctx.expect('FROM');
      table = parseDottedName(ctx);
    }
  } else {
    ctx.advance();
    table = parseDottedName(ctx);
  }
  const alias = parseOptionalTableAlias(ctx, new Set(['USING', 'WHERE', 'RETURNING', 'GO', 'DBCC']));

  let fromJoins: AST.JoinClause[] | undefined;
  {
    const joins: AST.JoinClause[] = [];
    while (ctx.isJoinKeyword()) {
      joins.push(ctx.parseJoin());
    }
    if (joins.length > 0) fromJoins = joins;
  }

  let using: AST.FromClause[] | undefined;
  let usingJoins: AST.JoinClause[] | undefined;
  if (ctx.peekUpper() === 'USING') {
    ctx.advance();
    using = [ctx.parseFromItem()];
    while (ctx.check(',')) {
      ctx.advance();
      using.push(ctx.parseFromItem());
    }
    const joins: AST.JoinClause[] = [];
    while (ctx.isJoinKeyword()) {
      joins.push(ctx.parseJoin());
    }
    if (joins.length > 0) usingJoins = joins;
  }

  let where: AST.WhereClause | undefined;
  let currentOf: string | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    if (ctx.peekUpper() === 'CURRENT' && ctx.peekUpperAt(1) === 'OF') {
      ctx.advance();
      ctx.advance();
      currentOf = parseDottedName(ctx);
    } else {
      where = { condition: ctx.parseExpression() };
    }
  }

  const returning = parseOptionalReturning(ctx);

  return {
    type: 'delete',
    targets,
    from: table,
    alias,
    fromJoins,
    using,
    usingJoins,
    where,
    currentOf,
    returning,
    leadingComments: comments,
  };
}

function parseParenthesizedIdentifierList(ctx: DmlParser): string[] {
  ctx.expect('(');
  const items: string[] = [];
  while (!ctx.check(')') && !ctx.isAtEnd()) {
    items.push(ctx.advance().value);
    if (ctx.check(',')) {
      ctx.advance();
    }
  }
  ctx.expect(')');
  return items;
}

function parseOptionalInsertSourceAlias(ctx: DmlParser): AST.InsertStatement['valuesAlias'] {
  if (ctx.peekUpper() !== 'AS') return undefined;
  ctx.advance();
  const name = ctx.advance().value;
  let columns: string[] | undefined;
  if (ctx.check('(')) {
    columns = parseParenthesizedIdentifierList(ctx);
  }
  return {
    name,
    columns: columns && columns.length > 0 ? columns : undefined,
  };
}

function parseOptionalReturning(ctx: DmlParser): AST.Expression[] | undefined {
  if (ctx.peekUpper() !== 'RETURNING') return undefined;
  ctx.advance();
  return ctx.parseReturningList();
}

function matchesKeywords(ctx: DmlParser, ...keywords: string[]): boolean {
  for (let i = 0; i < keywords.length; i++) {
    if (ctx.peekUpperAt(i) !== keywords[i]) return false;
  }
  return true;
}

function parseOptionalTableAlias(ctx: DmlParser, stopKeywords: Set<string>): string | undefined {
  if (ctx.peekUpper() === 'AS') {
    ctx.advance();
    return ctx.advance().value;
  }
  if (ctx.isJoinKeyword()) return undefined;
  const upper = ctx.peekUpper();
  if (stopKeywords.has(upper)) return undefined;
  if (ctx.check(',') || ctx.check('(') || ctx.check(')') || ctx.check(';')) return undefined;
  // Accept identifiers and non-clause keywords as aliases.
  const token = ctx.advance();
  if (token.type === 'identifier') return token.value;
  if (token.type === 'keyword' && !stopKeywords.has(token.upper)) return token.value;
  return undefined;
}

function parseDottedName(ctx: DmlParser): string {
  let name = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance();
    name += '.' + ctx.advance().value;
  }
  return name;
}
