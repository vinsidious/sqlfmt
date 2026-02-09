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
  getPos(): number;
  setPos(pos: number): void;
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

  const rlikeExpr = tryParseLikeFamilyComparison(ctx, left, 'RLIKE');
  if (rlikeExpr) return rlikeExpr;

  const regexpExpr = tryParseLikeFamilyComparison(ctx, left, 'REGEXP');
  if (regexpExpr) return regexpExpr;

  const similarExpr = tryParseSimilarToComparison(ctx, left);
  if (similarExpr) return similarExpr;

  const regexExpr = tryParseRegexComparison(ctx, left);
  if (regexExpr) return regexExpr;

  const binaryExpr = tryParseBinaryComparison(ctx, left);
  if (binaryExpr) return binaryExpr;

  return left;
}

function tryParseIsComparison(ctx: ComparisonParser, left: AST.Expression): AST.Expression | null {
  if (ctx.peekUpper() === 'ISNULL') {
    ctx.advance();
    return { type: 'is', expr: left, value: 'NULL' };
  }
  if (ctx.peekUpper() === 'NOTNULL') {
    ctx.advance();
    return { type: 'is', expr: left, value: 'NOT NULL' };
  }

  if (ctx.peekUpper() !== 'IS') return null;

  const checkpoint = ctx.getPos();
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
    ctx.setPos(checkpoint);
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

  ctx.setPos(checkpoint);
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
    return {
      type: 'in',
      kind: 'subquery',
      expr: left,
      subquery: { type: 'subquery', query },
      negated,
    };
  }
  const values = ctx.parseExpressionList();
  ctx.expect(')');
  return { type: 'in', kind: 'list', expr: left, values, negated };
}

function tryParseLikeFamilyComparison(
  ctx: ComparisonParser,
  left: AST.Expression,
  keyword: 'LIKE' | 'ILIKE' | 'RLIKE' | 'REGEXP'
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
  if (keyword === 'ILIKE') {
    return { type: 'ilike', expr: left, pattern, negated, escape };
  }
  const op = negated ? `NOT ${keyword}` : keyword;
  return { type: 'binary', left, operator: op, right: pattern };
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

  if (
    (ctx.peekUpper() === 'ALL' || ctx.peekUpper() === 'ANY' || ctx.peekUpper() === 'SOME')
    && ctx.peekUpperAt(1) === '('
  ) {
    const quantifier = ctx.advance().upper as 'ALL' | 'ANY' | 'SOME';
    ctx.expect('(');
    const query = ctx.tryParseQueryExpressionAtCurrent();
    if (query) {
      ctx.expect(')');
      return {
        type: 'quantified_comparison',
        kind: 'subquery',
        left,
        operator: op,
        quantifier,
        subquery: { type: 'subquery', query },
      } as AST.QuantifiedComparisonSubqueryExpr;
    }

    const values = ctx.parseExpressionList();
    ctx.expect(')');
    return {
      type: 'quantified_comparison',
      kind: 'list',
      left,
      operator: op,
      quantifier,
      values,
    } as AST.QuantifiedComparisonListExpr;
  }

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
  getPos(): number;
  setPos(pos: number): void;
  tokensToSql(tokens: Token[]): string;
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
  consumeComments?: () => AST.CommentNode[];
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
  if (token.upper !== 'INTERVAL') return null;
  const nextType = ctx.peekTypeAt(1);
  if (nextType !== 'string' && nextType !== 'number') return null;

  ctx.advance();
  const valueToken = ctx.advance();
  let value = valueToken.value;

  // INTERVAL '1' DAY, INTERVAL '1' DAY TO SECOND, INTERVAL 30 DAY
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

  const innerStart = ctx.getPos();
  consumeCommentTokens(ctx);
  const firstExpr = ctx.parseExpression();
  const trailingComments = consumeCommentTokens(ctx);

  if (ctx.check(',')) {
    const items: AST.Expression[] = [firstExpr];
    while (ctx.check(',')) {
      ctx.advance();
      consumeCommentTokens(ctx);
      items.push(ctx.parseExpression());
      consumeCommentTokens(ctx);
    }
    ctx.expect(')');
    return { type: 'tuple', items };
  }

  if (trailingComments.length > 0 && isExpressionContinuationToken(ctx.peek())) {
    const rawInner = parseParenthesizedInnerAsRaw(ctx, innerStart);
    ctx.expect(')');
    return { type: 'paren', expr: rawInner };
  }

  ctx.expect(')');
  return { type: 'paren', expr: firstExpr };
}

function consumeCommentTokens(ctx: PrimaryExpressionParser): Token[] {
  const tokens: Token[] = [];
  while (ctx.peekTypeAt(0) === 'line_comment' || ctx.peekTypeAt(0) === 'block_comment') {
    tokens.push(ctx.advance());
  }
  return tokens;
}

function parseParenthesizedInnerAsRaw(ctx: PrimaryExpressionParser, startPos: number): AST.RawExpression {
  ctx.setPos(startPos);
  const tokens: Token[] = [];
  let depth = 0;

  while (ctx.peekTypeAt(0) !== 'eof') {
    const next = ctx.peek();
    if (next.value === ')' && depth === 0) break;

    const token = ctx.advance();
    tokens.push(token);
    if (token.value === '(') {
      depth++;
    } else if (token.value === ')') {
      depth = Math.max(0, depth - 1);
    }
  }

  return {
    type: 'raw',
    text: ctx.tokensToSql(tokens),
    reason: 'verbatim',
  };
}

const KEYWORD_CONTINUATIONS = new Set([
  'IS',
  'IN',
  'LIKE',
  'ILIKE',
  'SIMILAR',
  'BETWEEN',
  'NOT',
  'COLLATE',
]);

function isExpressionContinuationToken(token: Token): boolean {
  if (token.type === 'operator') return true;
  if (token.type === 'keyword') return KEYWORD_CONTINUATIONS.has(token.upper);
  return false;
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

    // KWDB-style compact duration literals: 10y, 12mon, 1000ms, etc.
    const maybeUnit = ctx.peekAt(0);
    if (
      isCompactDurationUnit(maybeUnit)
      && maybeUnit.position === token.position + token.value.length
    ) {
      ctx.advance();
      return {
        type: 'raw',
        text: token.value + maybeUnit.value,
        reason: 'verbatim',
      };
    }

    return { type: 'literal', value: token.value, literalType: 'number' };
  }
  if (token.type === 'string') {
    ctx.advance();
    if (/^U&/i.test(token.value) && ctx.peekUpperAt(0) === 'UESCAPE') {
      ctx.advance(); // UESCAPE
      const escapeToken = ctx.advance();
      return {
        type: 'raw',
        text: `${token.value} UESCAPE ${escapeToken.value}`,
        reason: 'verbatim',
      };
    }

    let expr: AST.Expression = { type: 'literal', value: token.value, literalType: 'string' };
    // SQL standard implicit concatenation: 'a' 'b' => 'ab'
    while (ctx.peekTypeAt(0) === 'string') {
      const nextToken = ctx.advance();
      expr = {
        type: 'binary',
        left: expr,
        operator: '||',
        right: { type: 'literal', value: nextToken.value, literalType: 'string' },
      };
    }
    return expr;
  }
  if (token.type === 'parameter') {
    ctx.advance();
    return { type: 'raw', text: token.value, reason: 'verbatim' };
  }
  return null;
}

function isCompactDurationUnit(token: Token | undefined): boolean {
  if (!token) return false;
  if (token.type !== 'identifier' && token.type !== 'keyword') return false;
  const unit = token.upper;
  return (
    unit === 'Y'
    || unit === 'MON'
    || unit === 'W'
    || unit === 'D'
    || unit === 'H'
    || unit === 'M'
    || unit === 'S'
    || unit === 'MS'
  );
}

function tryParseTypedStringPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (token.upper === 'TIME' || token.upper === 'TIMESTAMP') {
    const tzLiteral = tryParseTimeZoneTypedLiteral(ctx, token.upper as 'TIME' | 'TIMESTAMP');
    if (tzLiteral) return tzLiteral;
  }

  if (
    !(
      (
        token.upper === 'DATE'
        || token.upper === 'TIME'
        || token.upper === 'TIMESTAMP'
        || token.upper === 'TIMESTAMPTZ'
      )
      && ctx.peekTypeAt(1) === 'string'
    )
  ) {
    return null;
  }

  ctx.advance();
  const strToken = ctx.advance();

  // KWDB-style typed literals are written without a separating space.
  if (token.upper === 'TIMESTAMPTZ') {
    return {
      type: 'raw',
      text: `${token.upper}${strToken.value}`,
      reason: 'verbatim',
    };
  }

  return {
    type: 'typed_string',
    dataType: token.upper,
    value: strToken.value,
  };
}

const GENERIC_TYPED_LITERAL_TYPE_CONTINUATIONS: Record<string, Set<string>> = {
  DOUBLE: new Set(['PRECISION']),
  CHARACTER: new Set(['VARYING']),
  CHAR: new Set(['VARYING']),
  NATIONAL: new Set(['CHARACTER']),
  TIME: new Set(['WITH', 'WITHOUT']),
  TIMESTAMP: new Set(['WITH', 'WITHOUT']),
  WITH: new Set(['TIME']),
  WITHOUT: new Set(['TIME']),
  LOCAL: new Set(['TIME']),
};

const GENERIC_TYPED_LITERAL_DISALLOWED = new Set([
  'SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'GROUP', 'ORDER',
  'HAVING', 'LIMIT', 'OFFSET', 'FETCH', 'CASE', 'CAST', 'EXTRACT', 'POSITION', 'SUBSTRING',
  'OVERLAY', 'TRIM', 'ARRAY', 'ROW', 'INTERVAL', 'EXISTS', 'NOT', 'AND', 'OR', 'NULL',
  'TRUE', 'FALSE', 'IN', 'IS', 'LIKE', 'ILIKE', 'SIMILAR', 'BETWEEN',
]);

function tryParseGenericTypedStringPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (ctx.peekTypeAt(1) !== 'string') return null;
  if (token.value.startsWith('_')) return null;

  const firstTypeWord = token.upper || token.value.toUpperCase();
  if (GENERIC_TYPED_LITERAL_DISALLOWED.has(firstTypeWord)) return null;

  const typeParts: string[] = [];
  let offset = 0;

  while (true) {
    const current = ctx.peekAt(offset);
    if (current.type !== 'identifier' && current.type !== 'keyword') break;

    const currentPart = current.type === 'keyword' ? current.upper : current.value;
    typeParts.push(currentPart);

    const next = ctx.peekAt(offset + 1);
    if (next.type !== 'identifier' && next.type !== 'keyword') break;

    const allowNext = GENERIC_TYPED_LITERAL_TYPE_CONTINUATIONS[currentPart];
    if (!allowNext || !allowNext.has(next.upper)) break;
    offset++;
  }

  const literalToken = ctx.peekAt(offset + 1);
  if (literalToken.type !== 'string' || typeParts.length === 0) return null;
  const lastTypeToken = ctx.peekAt(offset);
  if (literalToken.position === lastTypeToken.position + lastTypeToken.value.length) {
    return null;
  }

  for (let i = 0; i <= offset; i++) {
    ctx.advance();
  }
  const valueToken = ctx.advance();
  return {
    type: 'typed_string',
    dataType: typeParts.join(' '),
    value: valueToken.value,
  };
}

function tryParseTimeZoneTypedLiteral(
  ctx: PrimaryExpressionParser,
  dataType: 'TIME' | 'TIMESTAMP',
): AST.Expression | null {
  const variants: Array<{ parts: string[]; stringOffset: number }> = [
    { parts: ['WITH', 'TIME', 'ZONE'], stringOffset: 4 },
    { parts: ['WITHOUT', 'TIME', 'ZONE'], stringOffset: 4 },
    { parts: ['WITH', 'LOCAL', 'TIME', 'ZONE'], stringOffset: 5 },
  ];

  for (const variant of variants) {
    let matches = true;
    for (let i = 0; i < variant.parts.length; i++) {
      if (ctx.peekUpperAt(i + 1) !== variant.parts[i]) {
        matches = false;
        break;
      }
    }
    if (!matches || ctx.peekTypeAt(variant.stringOffset) !== 'string') continue;

    ctx.advance(); // TIME or TIMESTAMP
    for (let i = 0; i < variant.parts.length; i++) {
      ctx.advance();
    }
    const literalToken = ctx.advance();
    return {
      type: 'raw',
      text: `${dataType} ${variant.parts.join(' ')} ${literalToken.value}`,
      reason: 'verbatim',
    };
  }

  return null;
}

function tryParseCurrentDatetimePrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  if (
    token.upper === 'CURRENT_DATE'
    || token.upper === 'CURRENT_TIME'
    || token.upper === 'CURRENT_TIMESTAMP'
  ) {
    ctx.advance();
    if (ctx.check('(')) {
      const suffixTokens: Token[] = [];
      let depth = 0;
      do {
        const t = ctx.advance();
        suffixTokens.push(t);
        if (t.value === '(') depth++;
        if (t.value === ')') depth--;
      } while (ctx.peekTypeAt(0) !== 'eof' && depth > 0);
      return { type: 'raw', text: token.upper + ctx.tokensToSql(suffixTokens), reason: 'verbatim' };
    }
    return { type: 'raw', text: token.upper, reason: 'verbatim' };
  }
  return null;
}

function tryParseIdentifierPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression | null {
  const genericTypedLiteral = tryParseGenericTypedStringPrimary(ctx, token);
  if (genericTypedLiteral) return genericTypedLiteral;
  if (token.type === 'identifier' || token.type === 'keyword') {
    return ctx.parseIdentifierOrFunction();
  }
  return null;
}

function parseFallbackPrimary(ctx: PrimaryExpressionParser, token: Token): AST.Expression {
  ctx.advance();
  return { type: 'raw', text: token.value, reason: 'unsupported' };
}
