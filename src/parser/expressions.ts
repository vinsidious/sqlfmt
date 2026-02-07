import * as AST from '../ast';
import type { Token } from '../tokenizer';

export interface ComparisonParser {
  parseAddSub(): AST.Expression;
  peekUpper(): string;
  peekUpperAt(offset: number): string;
  advance(): Token;
  expect(value: string): Token;
  tryParseQueryExpressionAtCurrent(): AST.QueryExpression | null;
  parseExpressionList(): AST.Expression[];
  isRegexOperator(): boolean;
  checkComparisonOperator(): boolean;
}

export function parseComparisonExpression(ctx: ComparisonParser): AST.Expression {
  const left = ctx.parseAddSub();

  const isExpr = tryParseIsComparison(ctx, left);
  if (isExpr) return isExpr;

  const betweenExpr = tryParseBetweenComparison(ctx, left);
  if (betweenExpr) return betweenExpr;

  const inExpr = tryParseInComparison(ctx, left);
  if (inExpr) return inExpr;

  const likeExpr = tryParseLikeFamilyComparison(ctx, left, 'LIKE');
  if (likeExpr) return likeExpr;

  const ilikeExpr = tryParseLikeFamilyComparison(ctx, left, 'ILIKE');
  if (ilikeExpr) return ilikeExpr;

  const similarExpr = tryParseSimilarToComparison(ctx, left);
  if (similarExpr) return similarExpr;

  const regexExpr = tryParseRegexComparison(ctx, left);
  if (regexExpr) return regexExpr;

  const binaryExpr = tryParseBinaryComparison(ctx, left);
  if (binaryExpr) return binaryExpr;

  return left;
}

function tryParseIsComparison(ctx: ComparisonParser, left: AST.Expression): AST.Expression | null {
  if (ctx.peekUpper() !== 'IS') return null;
  ctx.advance();

  if (ctx.peekUpper() === 'NOT') {
    ctx.advance();
    if (ctx.peekUpper() === 'NULL') {
      ctx.advance();
      return { type: 'is', expr: left, value: 'NOT NULL' };
    }
    if (ctx.peekUpper() === 'DISTINCT' && ctx.peekUpperAt(1) === 'FROM') {
      ctx.advance();
      ctx.advance();
      const right = ctx.parseAddSub();
      return { type: 'is_distinct_from', left, right, negated: true } as AST.IsDistinctFromExpr;
    }
    if (ctx.peekUpper() === 'TRUE') {
      ctx.advance();
      return { type: 'is', expr: left, value: 'NOT TRUE' };
    }
    if (ctx.peekUpper() === 'FALSE') {
      ctx.advance();
      return { type: 'is', expr: left, value: 'NOT FALSE' };
    }
    return null;
  }

  if (ctx.peekUpper() === 'NULL') {
    ctx.advance();
    return { type: 'is', expr: left, value: 'NULL' };
  }
  if (ctx.peekUpper() === 'TRUE') {
    ctx.advance();
    return { type: 'is', expr: left, value: 'TRUE' };
  }
  if (ctx.peekUpper() === 'FALSE') {
    ctx.advance();
    return { type: 'is', expr: left, value: 'FALSE' };
  }
  if (ctx.peekUpper() === 'DISTINCT' && ctx.peekUpperAt(1) === 'FROM') {
    ctx.advance();
    ctx.advance();
    const right = ctx.parseAddSub();
    return { type: 'is_distinct_from', left, right, negated: false } as AST.IsDistinctFromExpr;
  }

  return null;
}

function tryParseBetweenComparison(ctx: ComparisonParser, left: AST.Expression): AST.Expression | null {
  let negated = false;
  if (ctx.peekUpper() === 'NOT' && ctx.peekUpperAt(1) === 'BETWEEN') {
    ctx.advance();
    negated = true;
  }
  if (ctx.peekUpper() !== 'BETWEEN') return null;

  ctx.advance();
  const low = ctx.parseAddSub();
  ctx.expect('AND');
  const high = ctx.parseAddSub();
  return { type: 'between', expr: left, low, high, negated };
}

function tryParseInComparison(ctx: ComparisonParser, left: AST.Expression): AST.Expression | null {
  let negated = false;
  if (ctx.peekUpper() === 'NOT' && ctx.peekUpperAt(1) === 'IN') {
    ctx.advance();
    negated = true;
  }
  if (ctx.peekUpper() !== 'IN') return null;

  ctx.advance();
  ctx.expect('(');
  const query = ctx.tryParseQueryExpressionAtCurrent();
  if (query) {
    ctx.expect(')');
    return { type: 'in', expr: left, values: { type: 'subquery', query }, negated, subquery: true as const };
  }
  const values = ctx.parseExpressionList();
  ctx.expect(')');
  return { type: 'in', expr: left, values, negated, subquery: false as const };
}

function tryParseLikeFamilyComparison(
  ctx: ComparisonParser,
  left: AST.Expression,
  keyword: 'LIKE' | 'ILIKE'
): AST.Expression | null {
  let negated = false;
  if (ctx.peekUpper() === 'NOT' && ctx.peekUpperAt(1) === keyword) {
    ctx.advance();
    negated = true;
  }
  if (ctx.peekUpper() !== keyword) return null;

  ctx.advance();
  const pattern = ctx.parseAddSub();
  let escape: AST.Expression | undefined;
  if (ctx.peekUpper() === 'ESCAPE') {
    ctx.advance();
    escape = ctx.parseAddSub();
  }

  if (keyword === 'LIKE') {
    return { type: 'like', expr: left, pattern, negated, escape };
  }
  return { type: 'ilike', expr: left, pattern, negated, escape };
}

function tryParseSimilarToComparison(ctx: ComparisonParser, left: AST.Expression): AST.Expression | null {
  let negated = false;
  if (ctx.peekUpper() === 'NOT' && ctx.peekUpperAt(1) === 'SIMILAR') {
    ctx.advance();
    negated = true;
  }
  if (!(ctx.peekUpper() === 'SIMILAR' && ctx.peekUpperAt(1) === 'TO')) return null;

  ctx.advance();
  ctx.advance();
  const pattern = ctx.parseAddSub();
  return { type: 'similar_to', expr: left, pattern, negated };
}

function tryParseRegexComparison(ctx: ComparisonParser, left: AST.Expression): AST.Expression | null {
  if (!ctx.isRegexOperator()) return null;
  const op = ctx.advance().value;
  const right = ctx.parseAddSub();
  return { type: 'regex_match', left, operator: op, right } as AST.RegexExpr;
}

function tryParseBinaryComparison(ctx: ComparisonParser, left: AST.Expression): AST.Expression | null {
  if (!ctx.checkComparisonOperator()) return null;
  const op = ctx.advance().value;
  const right = ctx.parseAddSub();
  return { type: 'binary', left, operator: op, right };
}

export interface PrimaryExpressionParser {
  peek(): Token;
  peekUpperAt(offset: number): string;
  peekAt(offset: number): Token;
  peekTypeAt(offset: number): Token['type'];
  advance(): Token;
  check(value: string): boolean;
  expect(value: string): Token;
  parseSubquery(): AST.SubqueryExpr;
  tryParseSubqueryAtCurrent(): AST.SubqueryExpr | null;
  parseExpression(): AST.Expression;
  parseCaseExpr(): AST.CaseExpr;
  parseCast(): AST.CastExpr;
  parseExtract(): AST.ExtractExpr;
  parsePositionExpr(): AST.Expression;
  parseSubstringExpr(): AST.Expression;
  parseOverlayExpr(): AST.Expression;
  parseTrimExpr(): AST.Expression;
  parseIdentifierOrFunction(): AST.Expression;
}

export function parsePrimaryExpression(ctx: PrimaryExpressionParser): AST.Expression {
  const token = ctx.peek();

  return (
    tryParseExistsPrimary(ctx, token)
    ?? tryParseNotExistsPrimary(ctx, token)
    ?? tryParseSpecialKeywordPrimary(ctx, token)
    ?? tryParseIntervalPrimary(ctx, token)
    ?? tryParseArrayConstructorPrimary(ctx, token)
    ?? tryParseRowConstructorPrimary(ctx, token)
    ?? tryParseParenOrSubqueryPrimary(ctx, token)
    ?? tryParseLiteralPrimary(ctx, token)
    ?? tryParseTypedStringPrimary(ctx, token)
    ?? tryParseCurrentDatetimePrimary(ctx, token)
    ?? tryParseIdentifierPrimary(ctx, token)
    ?? parseFallbackPrimary(ctx, token)
  );
}

function tryParseExistsPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (token.upper !== 'EXISTS') return null;
  ctx.advance();
  const subq = ctx.parseSubquery();
  return { type: 'exists', subquery: subq };
}

function tryParseNotExistsPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (!(token.upper === 'NOT' && ctx.peekUpperAt(1) === 'EXISTS')) return null;
  ctx.advance();
  ctx.advance();
  const subq = ctx.parseSubquery();
  return {
    type: 'unary',
    operator: 'NOT',
    operand: { type: 'exists', subquery: subq }
  };
}

function tryParseSpecialKeywordPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (token.upper === 'CASE') return ctx.parseCaseExpr();
  if (token.upper === 'CAST') return ctx.parseCast();
  if (token.upper === 'EXTRACT' && ctx.peekAt(1)?.value === '(') return ctx.parseExtract();
  if (token.upper === 'POSITION' && ctx.peekAt(1)?.value === '(') return ctx.parsePositionExpr();
  if (token.upper === 'SUBSTRING' && ctx.peekAt(1)?.value === '(') return ctx.parseSubstringExpr();
  if (token.upper === 'OVERLAY' && ctx.peekAt(1)?.value === '(') return ctx.parseOverlayExpr();
  if (token.upper === 'TRIM' && ctx.peekAt(1)?.value === '(') return ctx.parseTrimExpr();
  return null;
}

const INTERVAL_UNITS = new Set([
  'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
]);

function tryParseIntervalPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (!(token.upper === 'INTERVAL' && ctx.peekTypeAt(1) === 'string')) return null;
  ctx.advance();
  const strToken = ctx.advance();
  let value = strToken.value;

  // INTERVAL '1' DAY, INTERVAL '1' DAY TO SECOND
  if (INTERVAL_UNITS.has(ctx.peek().upper)) {
    value += ' ' + ctx.advance().upper;
    if (ctx.peek().upper === 'TO' && INTERVAL_UNITS.has(ctx.peekUpperAt(1))) {
      value += ' ' + ctx.advance().upper; // TO
      value += ' ' + ctx.advance().upper; // target unit
    }
  }

  return { type: 'interval', value };
}

function tryParseArrayConstructorPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (!(token.upper === 'ARRAY' && ctx.peekAt(1)?.value === '[')) return null;
  ctx.advance(); // ARRAY
  ctx.advance(); // [
  const elements: AST.Expression[] = [];
  if (!ctx.check(']')) {
    elements.push(ctx.parseExpression());
    while (ctx.check(',')) {
      ctx.advance();
      elements.push(ctx.parseExpression());
    }
  }
  ctx.expect(']');
  return { type: 'array_constructor', elements } as AST.ArrayConstructorExpr;
}

function tryParseRowConstructorPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (!(token.upper === 'ROW' && ctx.peekAt(1)?.value === '(')) return null;
  ctx.advance(); // ROW
  ctx.advance(); // (
  const args: AST.Expression[] = [];
  if (!ctx.check(')')) {
    args.push(ctx.parseExpression());
    while (ctx.check(',')) {
      ctx.advance();
      args.push(ctx.parseExpression());
    }
  }
  ctx.expect(')');
  return { type: 'function_call', name: 'ROW', args, distinct: false } as AST.FunctionCallExpr;
}

function tryParseParenOrSubqueryPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (token.value !== '(') return null;
  const subquery = ctx.tryParseSubqueryAtCurrent();
  if (subquery) return subquery;
  ctx.advance();
  const expr = ctx.parseExpression();
  ctx.expect(')');
  return { type: 'paren', expr };
}

function tryParseLiteralPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (token.value === '*') {
    ctx.advance();
    return { type: 'star' };
  }
  if (token.upper === 'TRUE' || token.upper === 'FALSE') {
    ctx.advance();
    return { type: 'literal', value: token.upper, literalType: 'boolean' };
  }
  if (token.upper === 'NULL') {
    ctx.advance();
    return { type: 'null' };
  }
  if (token.type === 'number') {
    ctx.advance();
    return { type: 'literal', value: token.value, literalType: 'number' };
  }
  if (token.type === 'string') {
    ctx.advance();
    return { type: 'literal', value: token.value, literalType: 'string' };
  }
  if (token.type === 'parameter') {
    ctx.advance();
    return { type: 'raw', text: token.value };
  }
  return null;
}

function tryParseTypedStringPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (
    !(
      (token.upper === 'DATE' || token.upper === 'TIME' || token.upper === 'TIMESTAMP')
      && ctx.peekTypeAt(1) === 'string'
    )
  ) {
    return null;
  }

  ctx.advance();
  const strToken = ctx.advance();
  return {
    type: 'typed_string',
    dataType: token.upper as 'DATE' | 'TIME' | 'TIMESTAMP',
    value: strToken.value,
  };
}

function tryParseCurrentDatetimePrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (
    token.upper === 'CURRENT_DATE'
    || token.upper === 'CURRENT_TIME'
    || token.upper === 'CURRENT_TIMESTAMP'
  ) {
    ctx.advance();
    return { type: 'raw', text: token.upper };
  }
  return null;
}

function tryParseIdentifierPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (token.type === 'identifier' || token.type === 'keyword') {
    return ctx.parseIdentifierOrFunction();
  }
  return null;
}

function parseFallbackPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression {
  ctx.advance();
  return { type: 'raw', text: token.value };
}
