import * as AST from '../ast';
import type { Token } from '../tokenizer';

export interface DmlContext {
  expect(value: string): Token;
  advance(): Token;
  check(value: string): boolean;
  peekUpper(): string;
  peekUpperAt(offset: number): string;
  isAtEnd(): boolean;
  parseExpression(): AST.Expression;
  parseExpressionList(): AST.Expression[];
  parseReturningList(): AST.Expression[];
  parseFromItem(): AST.FromClause;
  parseQueryExpression(): AST.QueryExpression;
}

export function parseInsertStatement(
  ctx: DmlContext,
  comments: AST.CommentNode[]
): AST.InsertStatement {
  ctx.expect('INSERT');
  ctx.expect('INTO');
  const table = ctx.advance().value;

  let columns: string[] = [];
  if (ctx.check('(')) {
    columns = parseParenthesizedIdentifierList(ctx);
  }

  let defaultValues = false;
  let values: AST.ValuesList[] | undefined;
  let selectQuery: AST.QueryExpression | undefined;

  if (ctx.peekUpper() === 'DEFAULT' && ctx.peekUpperAt(1) === 'VALUES') {
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
  } else if (ctx.peekUpper() === 'SELECT' || ctx.peekUpper() === 'WITH' || ctx.check('(')) {
    selectQuery = ctx.parseQueryExpression();
  }

  let onConflict: AST.InsertStatement['onConflict'];
  if (ctx.peekUpper() === 'ON' && ctx.peekUpperAt(1) === 'CONFLICT') {
    ctx.advance();
    ctx.advance();
    onConflict = parseInsertOnConflictClause(ctx);
  }

  let returning: AST.Expression[] | undefined;
  if (ctx.peekUpper() === 'RETURNING') {
    ctx.advance();
    returning = ctx.parseReturningList();
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

export function parseInsertOnConflictClause(ctx: DmlContext): AST.InsertStatement['onConflict'] {
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

export function parseValuesTuple(ctx: DmlContext): AST.ValuesList {
  ctx.expect('(');
  const values = ctx.parseExpressionList();
  ctx.expect(')');
  return { values };
}

export function parseUpdateStatement(
  ctx: DmlContext,
  comments: AST.CommentNode[]
): AST.UpdateStatement {
  ctx.expect('UPDATE');
  const table = ctx.advance().value;

  ctx.expect('SET');
  const setItems: AST.SetItem[] = [];
  setItems.push(parseSetItem(ctx));
  while (ctx.check(',')) {
    ctx.advance();
    setItems.push(parseSetItem(ctx));
  }

  let from: AST.FromClause | undefined;
  if (ctx.peekUpper() === 'FROM') {
    ctx.advance();
    from = ctx.parseFromItem();
  }

  let where: AST.WhereClause | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = { condition: ctx.parseExpression() };
  }

  let returning: AST.Expression[] | undefined;
  if (ctx.peekUpper() === 'RETURNING') {
    ctx.advance();
    returning = ctx.parseReturningList();
  }

  return { type: 'update', table, setItems, from, where, returning, leadingComments: comments };
}

export function parseSetItem(ctx: DmlContext): AST.SetItem {
  const column = ctx.advance().value;
  ctx.expect('=');
  const value = ctx.parseExpression();
  return { column, value };
}

export function parseDeleteStatement(
  ctx: DmlContext,
  comments: AST.CommentNode[]
): AST.DeleteStatement {
  ctx.expect('DELETE');
  ctx.expect('FROM');
  const table = ctx.advance().value;

  let using: AST.FromClause[] | undefined;
  if (ctx.peekUpper() === 'USING') {
    ctx.advance();
    using = [ctx.parseFromItem()];
    while (ctx.check(',')) {
      ctx.advance();
      using.push(ctx.parseFromItem());
    }
  }

  let where: AST.WhereClause | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = { condition: ctx.parseExpression() };
  }

  let returning: AST.Expression[] | undefined;
  if (ctx.peekUpper() === 'RETURNING') {
    ctx.advance();
    returning = ctx.parseReturningList();
  }

  return { type: 'delete', from: table, using, where, returning, leadingComments: comments };
}

function parseParenthesizedIdentifierList(ctx: DmlContext): string[] {
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
