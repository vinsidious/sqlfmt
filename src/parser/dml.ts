import * as AST from '../ast';
import type { Token } from '../tokenizer';

export interface DmlParser {
  expect(value: string): Token;
  advance(): Token;
  check(value: string): boolean;
  peekUpper(): string;
  peekUpperAt(offset: number): string;
  isAtEnd(): boolean;
  isJoinKeyword(): boolean;
  parseJoin(): AST.JoinClause;
  parseExpression(): AST.Expression;
  parseExpressionList(): AST.Expression[];
  parseReturningList(): AST.Expression[];
  parseFromItem(): AST.FromClause;
  parseQueryExpression(): AST.QueryExpression;
}

export function parseInsertStatement(
  ctx: DmlParser,
  comments: AST.CommentNode[]
): AST.InsertStatement {
  ctx.expect('INSERT');
  ctx.expect('INTO');
  const table = ctx.advance().value;

  let columns: string[] = [];
  if (ctx.check('(')) {
    columns = parseParenthesizedIdentifierList(ctx);
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
  let values: AST.ValuesList[] | undefined;
  let selectQuery: AST.QueryExpression | undefined;

  if (matchesKeywords(ctx, 'DEFAULT', 'VALUES')) {
    ctx.advance();
    ctx.advance();
    defaultValues = true;
  } else if (ctx.peekUpper() === 'VALUES') {
    ctx.advance();
    values = [];
    values.push(parseValuesTuple(ctx));
    while (ctx.check(',')) {
      ctx.advance();
      values.push(parseValuesTuple(ctx));
    }
    // VALUES and SELECT are mutually exclusive — force a ParseError
    if (ctx.peekUpper() === 'SELECT' || ctx.peekUpper() === 'WITH') {
      ctx.expect(';');
    }
  } else if (ctx.peekUpper() === 'SELECT' || ctx.peekUpper() === 'WITH' || ctx.check('(')) {
    selectQuery = ctx.parseQueryExpression();
  }

  let onConflict: AST.InsertStatement['onConflict'];
  if (ctx.peekUpper() === 'ON' && ctx.peekUpperAt(1) === 'CONFLICT') {
    ctx.advance();
    ctx.advance();
    onConflict = parseInsertOnConflictClause(ctx);
  }

  const returning = parseOptionalReturning(ctx);

  return {
    type: 'insert',
    table,
    columns,
    overriding,
    defaultValues,
    values,
    selectQuery,
    onConflict,
    returning,
    leadingComments: comments,
  };
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
  const setItems: { column: string; value: AST.Expression }[] = [];
  setItems.push(parseSetItem(ctx));
  while (ctx.check(',')) {
    ctx.advance();
    setItems.push(parseSetItem(ctx));
  }

  let where: AST.Expression | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = ctx.parseExpression();
  }

  return { columns: conflictColumns, constraintName, action: 'update', setItems, where };
}

export function parseValuesTuple(ctx: DmlParser): AST.ValuesList {
  ctx.expect('(');
  const values = ctx.parseExpressionList();
  ctx.expect(')');
  return { values };
}

export function parseUpdateStatement(
  ctx: DmlParser,
  comments: AST.CommentNode[]
): AST.UpdateStatement {
  ctx.expect('UPDATE');
  const table = ctx.advance().value;
  const alias = parseOptionalTableAlias(ctx, new Set(['SET']));

  ctx.expect('SET');
  const setItems: AST.SetItem[] = [];
  setItems.push(parseSetItem(ctx));
  while (ctx.check(',')) {
    ctx.advance();
    setItems.push(parseSetItem(ctx));
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

  return { type: 'update', table, alias, setItems, from, fromJoins, where, returning, leadingComments: comments };
}

export function parseSetItem(ctx: DmlParser): AST.SetItem {
  const column = ctx.advance().value;
  ctx.expect('=');
  const value = ctx.parseExpression();
  return { column, value };
}

export function parseDeleteStatement(
  ctx: DmlParser,
  comments: AST.CommentNode[]
): AST.DeleteStatement {
  ctx.expect('DELETE');
  ctx.expect('FROM');
  const table = ctx.advance().value;
  const alias = parseOptionalTableAlias(ctx, new Set(['USING', 'WHERE', 'RETURNING']));

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
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = { condition: ctx.parseExpression() };
  }

  const returning = parseOptionalReturning(ctx);

  return { type: 'delete', from: table, alias, using, usingJoins, where, returning, leadingComments: comments };
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
  const upper = ctx.peekUpper();
  if (stopKeywords.has(upper)) return undefined;
  if (ctx.check(',') || ctx.check(')') || ctx.check(';')) return undefined;
  // Accept identifiers and non-clause keywords as aliases.
  const token = ctx.advance();
  if (token.type === 'identifier') return token.value;
  if (token.type === 'keyword' && !stopKeywords.has(token.upper)) return token.value;
  return undefined;
}
