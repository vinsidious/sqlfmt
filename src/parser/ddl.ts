import * as AST from '../ast';
import type { Token } from '../tokenizer';

export interface DdlParser {
  expect(value: string): Token;
  advance(): Token;
  check(value: string): boolean;
  peek(): Token;
  peekUpper(): string;
  peekUpperAt(offset: number): string;
  peekType(): Token['type'];
  isAtEnd(): boolean;
  getPos(): number;
  setPos(pos: number): void;
  parseError(expected: string, token: Token): Error;
  consumeIfNotExists(): boolean;
  consumeIfExists(): boolean;
  parseRawStatement(reason: AST.RawReason): AST.RawExpression | null;
  parseTableElements(): { elements: AST.TableElement[]; trailingComma: boolean };
  parseExpression(): AST.Expression;
  parseStatement(): AST.Node | null;
  collectTokensUntilTopLevelKeyword(stopKeywords: Set<string>): Token[];
  tokensToSql(tokens: Token[]): string;
  tokensToSqlPreserveCase(tokens: Token[]): string;
  consumeTokensUntilActionBoundary(): Token[];
  hasImplicitStatementBoundary?: () => boolean;
}

export function parseCreateStatement(ctx: DdlParser, comments: AST.CommentNode[]): AST.Node {
  const statementStart = ctx.getPos();
  ctx.advance(); // CREATE

  let orReplace = false;
  if (ctx.peekUpper() === 'OR' && ctx.peekUpperAt(1) === 'REPLACE') {
    ctx.advance();
    ctx.advance();
    orReplace = true;
  }

  let unique = false;
  if (ctx.peekUpper() === 'UNIQUE') {
    ctx.advance();
    unique = true;
  }

  let materialized = false;
  if (ctx.peekUpper() === 'MATERIALIZED') {
    ctx.advance();
    materialized = true;
  }

  let temporary = false;
  if (
    (ctx.peekUpper() === 'TEMPORARY' || ctx.peekUpper() === 'TEMP')
    && ctx.peekUpperAt(1) === 'VIEW'
  ) {
    ctx.advance();
    temporary = true;
  }

  let clustered: AST.CreateIndexStatement['clustered'];
  if (
    (ctx.peekUpper() === 'CLUSTERED' || ctx.peekUpper() === 'NONCLUSTERED')
    && ctx.peekUpperAt(1) === 'INDEX'
  ) {
    clustered = ctx.advance().upper as AST.CreateIndexStatement['clustered'];
  }

  const kw = ctx.peekUpper();
  if (kw === 'TABLE') return parseCreateTableStatement(ctx, comments, statementStart, orReplace);
  if (kw === 'INDEX') return parseCreateIndexStatement(ctx, comments, unique, clustered);
  if (kw === 'VIEW') return parseCreateViewStatement(ctx, comments, orReplace, materialized, temporary);
  if (kw === 'POLICY') return parseCreatePolicyStatement(ctx, comments);

  ctx.setPos(statementStart);
  const raw = ctx.parseRawStatement('unsupported');
  if (!raw) throw ctx.parseError('CREATE statement', ctx.peek());
  return raw;
}

const CREATE_TABLE_OPTION_STARTERS = new Set([
  'WITH',
  'ENGINE',
  'PARTITION',
  'ORDER',
  'SETTINGS',
  'COMMENT',
  'COLLATE',
  'ROW_FORMAT',
  'AUTO_INCREMENT',
  'CHARSET',
  'CHARACTER',
  'DEFAULT',
]);

function isCreateTableOptionStart(ctx: DdlParser): boolean {
  const kw = ctx.peekUpper();
  if (!CREATE_TABLE_OPTION_STARTERS.has(kw)) return false;
  if (kw === 'WITH') {
    return ctx.check('WITH') && ctx.peekUpperAt(1) === '(';
  }
  if (kw === 'CHARACTER') {
    return ctx.peekUpperAt(1) === 'SET';
  }
  if (kw === 'DEFAULT') {
    const next = ctx.peekUpperAt(1);
    return next === 'CHARSET' || next === 'CHARACTER' || next === 'COLLATE';
  }
  return true;
}

function parseCreateTableStatement(
  ctx: DdlParser,
  comments: AST.CommentNode[],
  statementStart: number,
  orReplace: boolean,
): AST.Node {
  ctx.expect('TABLE');
  const ifNotExists = ctx.consumeIfNotExists();
  const tableName = parseCreateObjectName(ctx);
  const fullName = ifNotExists ? 'IF NOT EXISTS ' + tableName : tableName;

  // CREATE TABLE ... AS SELECT ...
  if (ctx.peekUpper() === 'AS') {
    ctx.advance(); // AS
    const query = ctx.parseStatement();
    if (!query || (query.type !== 'select' && query.type !== 'union' && query.type !== 'cte')) {
      throw ctx.parseError('SELECT, UNION, or WITH query in CREATE TABLE AS', ctx.peek());
    }
    return {
      type: 'create_table',
      orReplace: orReplace || undefined,
      tableName: fullName,
      elements: [],
      asQuery: query,
      leadingComments: comments,
    };
  }

  // MySQL: CREATE TABLE new_table LIKE existing_table;
  if (ctx.peekUpper() === 'LIKE') {
    ctx.advance(); // LIKE
    let likeTable = ctx.advance().value;
    while (ctx.check('.')) {
      ctx.advance();
      likeTable += '.' + ctx.advance().value;
    }
    return {
      type: 'create_table',
      orReplace: orReplace || undefined,
      tableName: fullName,
      likeTable,
      elements: [],
      leadingComments: comments,
    };
  }

  const parenthesizedQuery = tryParseParenthesizedCreateTableQuery(ctx);

  let elements: AST.TableElement[] = [];
  let trailingComma = false;
  let asQuery: AST.QueryExpression | undefined = parenthesizedQuery || undefined;

  if (!parenthesizedQuery && !ctx.check('(')) {
    ctx.setPos(statementStart);
    const raw = ctx.parseRawStatement('unsupported');
    if (!raw) throw ctx.parseError('CREATE TABLE statement', ctx.peek());
    if (comments.length === 0) return raw;
    return { type: 'raw', text: `${comments.map(c => c.text).join('\n')}\n${raw.text}`.trim(), reason: 'unsupported' };
  }

  if (!parenthesizedQuery) {
    ctx.expect('(');
    const parsedElements = ctx.parseTableElements();
    elements = parsedElements.elements;
    trailingComma = parsedElements.trailingComma;
    ctx.expect(')');
  }

  let tableOptions: string | undefined;
  if (!ctx.isAtEnd() && !ctx.check(';')) {
    const optionTokens: Token[] = [];
    while (!ctx.isAtEnd() && !ctx.check(';')) {
      if (!asQuery) {
        const parsedAsQuery = tryParseCreateTableAsQuery(ctx);
        if (parsedAsQuery) {
          asQuery = parsedAsQuery;
          break;
        }
      }
      if (ctx.hasImplicitStatementBoundary?.() && !isCreateTableOptionStart(ctx)) break;
      optionTokens.push(ctx.advance());
    }
    tableOptions = ctx.tokensToSql(optionTokens) || undefined;
  }

  return {
    type: 'create_table',
    orReplace: orReplace || undefined,
    tableName: fullName,
    elements,
    trailingComma: trailingComma || undefined,
    tableOptions,
    asQuery,
    leadingComments: comments,
  };
}

function tryParseCreateTableAsQuery(ctx: DdlParser): AST.QueryExpression | null {
  if (ctx.peekUpper() !== 'AS') return null;
  const start = ctx.getPos();

  ctx.advance(); // AS
  let query: AST.Node | null;
  try {
    query = ctx.parseStatement();
  } catch {
    ctx.setPos(start);
    return null;
  }
  if (
    query
    && (query.type === 'select' || query.type === 'union' || query.type === 'cte')
  ) {
    return query;
  }

  ctx.setPos(start);
  return null;
}

function tryParseParenthesizedCreateTableQuery(ctx: DdlParser): AST.QueryExpression | null {
  if (!ctx.check('(')) return null;
  const start = ctx.getPos();

  ctx.advance(); // (
  let query: AST.Node | null;
  try {
    query = ctx.parseStatement();
  } catch {
    ctx.setPos(start);
    return null;
  }
  if (query && (query.type === 'select' || query.type === 'union' || query.type === 'cte') && ctx.check(')')) {
    ctx.advance(); // )
    return query;
  }

  ctx.setPos(start);
  return null;
}

function parseCreateIndexStatement(
  ctx: DdlParser,
  comments: AST.CommentNode[],
  unique: boolean,
  clustered?: AST.CreateIndexStatement['clustered'],
): AST.CreateIndexStatement {
  ctx.advance(); // INDEX
  skipInlineComments(ctx);

  let concurrently = false;
  if (ctx.peekUpper() === 'CONCURRENTLY') {
    ctx.advance();
    concurrently = true;
    skipInlineComments(ctx);
  }

  const ifNotExists = ctx.consumeIfNotExists();
  skipInlineComments(ctx);
  let name: string | undefined;
  if (ctx.peekUpper() !== 'ON') {
    name = parseDottedName(ctx);
    skipInlineComments(ctx);
  }

  ctx.expect('ON');
  skipInlineComments(ctx);
  let only = false;
  if (ctx.peekUpper() === 'ONLY') {
    ctx.advance();
    only = true;
    skipInlineComments(ctx);
  }
  const table = parseDottedName(ctx);
  skipInlineComments(ctx);

  let using: string | undefined;
  if (ctx.peekUpper() === 'USING') {
    ctx.advance();
    using = ctx.advance().upper;
    skipInlineComments(ctx);
  }

  ctx.expect('(');
  const columns: AST.Expression[] = [parseIndexColumn(ctx)];
  while (ctx.check(',')) {
    ctx.advance();
    skipInlineComments(ctx);
    columns.push(parseIndexColumn(ctx));
  }
  ctx.expect(')');
  skipInlineComments(ctx);

  let include: AST.Expression[] | undefined;
  if (ctx.peekUpper() === 'INCLUDE') {
    ctx.advance();
    skipInlineComments(ctx);
    ctx.expect('(');
    skipInlineComments(ctx);
    include = [ctx.parseExpression()];
    skipInlineComments(ctx);
    while (ctx.check(',')) {
      ctx.advance();
      skipInlineComments(ctx);
      include.push(ctx.parseExpression());
      skipInlineComments(ctx);
    }
    ctx.expect(')');
    skipInlineComments(ctx);
  }

  let where: AST.Expression | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = ctx.parseExpression();
  }

  let options: string | undefined;
  if (!ctx.isAtEnd() && !ctx.check(';')) {
    const optionTokens: Token[] = [];
    while (!ctx.isAtEnd() && !ctx.check(';')) {
      if (ctx.hasImplicitStatementBoundary?.()) break;
      optionTokens.push(ctx.advance());
    }
    options = ctx.tokensToSql(optionTokens) || undefined;
  }

  return {
    type: 'create_index',
    unique,
    clustered,
    concurrently,
    ifNotExists,
    name,
    table,
    only,
    using,
    columns,
    include,
    where,
    options,
    leadingComments: comments,
  };
}

function parseCreateObjectName(ctx: DdlParser): string {
  if (ctx.peekUpper() === 'IDENTIFIER' && ctx.peekUpperAt(1) === '(') {
    const tokens: Token[] = [];
    tokens.push(ctx.advance()); // IDENTIFIER
    let depth = 0;
    do {
      const token = ctx.advance();
      tokens.push(token);
      if (token.value === '(') depth++;
      else if (token.value === ')') depth--;
    } while (!ctx.isAtEnd() && depth > 0);

    if (depth !== 0) throw ctx.parseError(')', ctx.peek());
    return normalizeIdentifierCall(ctx.tokensToSqlPreserveCase(tokens));
  }
  return parseDottedName(ctx);
}

function normalizeIdentifierCall(text: string): string {
  const match = text.match(/^IDENTIFIER\s*\(([\s\S]*)\)$/i);
  if (!match) return text;
  return `IDENTIFIER(${match[1].trim()})`;
}

function parseIndexColumn(ctx: DdlParser): AST.Expression {
  const startPos = ctx.getPos();
  const expr = ctx.parseExpression();
  if (ctx.peekUpper() === 'ASC' || ctx.peekUpper() === 'DESC') {
    const dir = ctx.advance().upper as 'ASC' | 'DESC';
    if (!ctx.check(',') && !ctx.check(')')) {
      ctx.setPos(startPos);
      const tokens = consumeTokensUntilIndexColumnBoundary(ctx);
      return { type: 'raw', text: ctx.tokensToSql(tokens), reason: 'verbatim' } as AST.RawExpression;
    }
    return { type: 'ordered_expr', expr, direction: dir } as AST.OrderedExpr;
  }

  // PostgreSQL operator classes / index column modifiers (e.g. gin_trgm_ops)
  // are preserved verbatim when present.
  if (!ctx.check(',') && !ctx.check(')')) {
    ctx.setPos(startPos);
    const tokens = consumeTokensUntilIndexColumnBoundary(ctx);
    return { type: 'raw', text: ctx.tokensToSql(tokens), reason: 'verbatim' } as AST.RawExpression;
  }

  return expr;
}

function consumeTokensUntilIndexColumnBoundary(ctx: DdlParser): Token[] {
  const tokens: Token[] = [];
  let depth = 0;
  while (!ctx.isAtEnd()) {
    if (depth === 0 && (ctx.check(',') || ctx.check(')'))) break;
    const token = ctx.advance();
    tokens.push(token);
    if (token.value === '(' || token.value === '[' || token.value === '{') depth++;
    if (token.value === ')' || token.value === ']' || token.value === '}') depth = Math.max(0, depth - 1);
  }
  return tokens;
}

function parseCreateViewStatement(
  ctx: DdlParser,
  comments: AST.CommentNode[],
  orReplace: boolean,
  materialized: boolean,
  temporary: boolean,
): AST.CreateViewStatement {
  ctx.advance(); // VIEW

  const ifNotExists = ctx.consumeIfNotExists();
  const name = parseDottedName(ctx);
  skipInlineComments(ctx);

  let columnList: string[] | undefined;
  if (ctx.check('(')) {
    columnList = parseParenthesizedIdentifierList(ctx);
    skipInlineComments(ctx);
  }

  let toTable: string | undefined;
  let toColumns: string[] | undefined;
  if (ctx.peekUpper() === 'TO') {
    ctx.advance();
    toTable = parseDottedName(ctx);
    skipInlineComments(ctx);
    if (ctx.check('(')) {
      toColumns = parseParenthesizedIdentifierList(ctx);
      skipInlineComments(ctx);
    }
  }

  let comment: string | undefined;
  if (ctx.peekUpper() === 'COMMENT') {
    ctx.advance();
    skipInlineComments(ctx);
    ctx.expect('=');
    skipInlineComments(ctx);

    const commentTokens: Token[] = [];
    let depth = 0;
    while (!ctx.isAtEnd()) {
      if (depth === 0 && ctx.peekUpper() === 'AS') break;
      const token = ctx.advance();
      commentTokens.push(token);
      if (token.value === '(' || token.value === '[' || token.value === '{') depth++;
      else if (token.value === ')' || token.value === ']' || token.value === '}') depth = Math.max(0, depth - 1);
    }
    comment = ctx.tokensToSql(commentTokens) || undefined;
    skipInlineComments(ctx);
  }

  let withOptions: string | undefined;
  if (ctx.peekUpper() === 'WITH' && ctx.peekUpperAt(1) === '(') {
    const optionTokens: Token[] = [];
    optionTokens.push(ctx.advance()); // WITH
    if (ctx.check('(')) {
      let depth = 0;
      do {
        const token = ctx.advance();
        optionTokens.push(token);
        if (token.value === '(') depth++;
        if (token.value === ')') depth--;
      } while (!ctx.isAtEnd() && depth > 0);
    }
    withOptions = ctx.tokensToSql(optionTokens) || undefined;
    skipInlineComments(ctx);
  }

  ctx.expect('AS');
  const query = ctx.parseStatement();
  if (
    query
    && query.type !== 'select'
    && query.type !== 'union'
    && query.type !== 'cte'
    && query.type !== 'standalone_values'
  ) {
    throw ctx.parseError('SELECT, UNION, or WITH query in CREATE VIEW', ctx.peek());
  }

  let withData: boolean | undefined;
  let withClause: string | undefined;
  if (ctx.peekUpper() === 'WITH') {
    const startPos = ctx.getPos();
    ctx.advance();
    if (ctx.peekUpper() === 'DATA') {
      ctx.advance();
      withData = true;
    } else if (ctx.peekUpper() === 'NO' && ctx.peekUpperAt(1) === 'DATA') {
      ctx.advance();
      ctx.advance();
      withData = false;
    } else {
      const tokens: Token[] = [];
      ctx.setPos(startPos);
      while (!ctx.isAtEnd() && !ctx.check(';')) {
        tokens.push(ctx.advance());
      }
      withClause = ctx.tokensToSql(tokens) || undefined;
    }
  }

  return {
    type: 'create_view',
    orReplace,
    temporary: temporary || undefined,
    materialized,
    ifNotExists,
    name,
    columnList,
    toTable,
    toColumns,
    comment,
    withOptions,
    query: query as AST.Statement,
    withData,
    withClause,
    leadingComments: comments,
  };
}

function parseDottedName(ctx: DdlParser): string {
  let name = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance();
    name += '.' + ctx.advance().value;
  }
  return name;
}

function parseParenthesizedIdentifierList(ctx: DdlParser): string[] {
  ctx.expect('(');
  const columns: string[] = [];
  skipInlineComments(ctx);
  while (!ctx.isAtEnd() && !ctx.check(')')) {
    columns.push(ctx.advance().value);
    skipInlineComments(ctx);
    if (ctx.check(',')) {
      ctx.advance();
      skipInlineComments(ctx);
    }
  }
  ctx.expect(')');
  return columns;
}

function skipInlineComments(ctx: DdlParser): void {
  while (ctx.peekType() === 'line_comment' || ctx.peekType() === 'block_comment') {
    ctx.advance();
  }
}

function parseCreatePolicyStatement(ctx: DdlParser, comments: AST.CommentNode[]): AST.CreatePolicyStatement {
  ctx.expect('POLICY');
  const name = ctx.advance().value;

  ctx.expect('ON');
  let table = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance();
    table += '.' + ctx.advance().value;
  }

  let permissive: 'PERMISSIVE' | 'RESTRICTIVE' | undefined;
  if (ctx.peekUpper() === 'AS') {
    ctx.advance();
    const val = ctx.advance().upper;
    if (val === 'PERMISSIVE' || val === 'RESTRICTIVE') {
      permissive = val;
    }
  }

  let command: 'ALL' | 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | undefined;
  if (ctx.peekUpper() === 'FOR') {
    ctx.advance();
    const val = ctx.advance().upper;
    if (val === 'ALL' || val === 'SELECT' || val === 'INSERT' || val === 'UPDATE' || val === 'DELETE') {
      command = val;
    }
  }

  let roles: string[] | undefined;
  if (ctx.peekUpper() === 'TO') {
    ctx.advance();
    roles = [ctx.advance().value];
    while (ctx.check(',')) {
      ctx.advance();
      roles.push(ctx.advance().value);
    }
  }

  let using: AST.Expression | undefined;
  if (ctx.peekUpper() === 'USING') {
    ctx.advance();
    ctx.expect('(');
    skipInlineComments(ctx);
    using = ctx.parseExpression();
    skipInlineComments(ctx);
    ctx.expect(')');
  }

  let withCheck: AST.Expression | undefined;
  if (ctx.peekUpper() === 'WITH') {
    ctx.advance();
    ctx.expect('CHECK');
    ctx.expect('(');
    skipInlineComments(ctx);
    withCheck = ctx.parseExpression();
    skipInlineComments(ctx);
    ctx.expect(')');
  }

  return {
    type: 'create_policy',
    name,
    table,
    permissive,
    command,
    roles,
    using,
    withCheck,
    leadingComments: comments,
  };
}

export function parseAlterStatement(ctx: DdlParser, comments: AST.CommentNode[]): AST.AlterTableStatement {
  ctx.expect('ALTER');

  const objectType = ctx.advance().upper;
  const objectNameTokens: Token[] = [];
  const actionStarters = new Set([
    'ADD',
    'DROP',
    'RENAME',
    'SET',
    'ALTER',
    'OWNER',
    'CHANGE',
    'MODIFY',
    'CHECK',
    'WITH',
  ]);
  while (!ctx.isAtEnd() && !ctx.check(';')) {
    const upper = ctx.peekUpper();
    const previousUpper = objectNameTokens.length > 0 ? objectNameTokens[objectNameTokens.length - 1].upper : '';
    const startsAction = actionStarters.has(upper) || (objectType === 'DATABASE' && upper === 'CHARACTER');
    if (
      objectNameTokens.length > 0
      && startsAction
      && !(objectType === 'DATABASE' && upper === 'SET' && previousUpper === 'CHARACTER')
    ) {
      break;
    }
    objectNameTokens.push(ctx.advance());
  }
  const objectName = ctx.tokensToSqlPreserveCase(objectNameTokens);
  if (!objectName) {
    throw ctx.parseError('object name', ctx.peek());
  }

  const actions: AST.AlterAction[] = [];
  let actionSeparatorConsumed = false;
  while (!ctx.isAtEnd() && !ctx.check(';')) {
    if (ctx.check(',')) {
      ctx.advance();
      actionSeparatorConsumed = true;
      continue;
    }
    if (actions.length > 0 && !actionSeparatorConsumed && ctx.hasImplicitStatementBoundary?.()) {
      break;
    }
    actionSeparatorConsumed = false;
    actions.push(parseAlterAction(ctx));
  }

  return {
    type: 'alter_table',
    objectType,
    objectName,
    actions,
    leadingComments: comments,
  };
}

function parseAlterAction(ctx: DdlParser): AST.AlterAction {
  return (
    tryParseAlterAddNonColumnAction(ctx)
    ?? tryParseAlterAddColumnAction(ctx)
    ?? tryParseAlterDropColumnAction(ctx)
    ?? tryParseAlterDropConstraintAction(ctx)
    ?? tryParseAlterAlterColumnAction(ctx)
    ?? tryParseAlterOwnerToAction(ctx)
    ?? tryParseAlterRenameAction(ctx)
    ?? tryParseAlterSetSchemaAction(ctx)
    ?? tryParseAlterSetTablespaceAction(ctx)
    ?? parseRawAlterAction(ctx)
  );
}

function tryParseAlterAddNonColumnAction(ctx: DdlParser): AST.AlterAction | null {
  const start = ctx.getPos();
  if (ctx.peekUpper() !== 'ADD') return null;
  ctx.advance(); // ADD
  if (ctx.peekUpper() === 'COLUMN') {
    ctx.setPos(start);
    return null;
  }
  if (
    ctx.peekUpper() !== 'CONSTRAINT'
    && !(ctx.peekUpper() === 'PRIMARY' && ctx.peekUpperAt(1) === 'KEY')
    && !(ctx.peekUpper() === 'FOREIGN' && ctx.peekUpperAt(1) === 'KEY')
    && ctx.peekUpper() !== 'UNIQUE'
    && ctx.peekUpper() !== 'CHECK'
    && ctx.peekUpper() !== 'DEFAULT'
    && ctx.peekUpper() !== 'KEY'
    && ctx.peekUpper() !== 'INDEX'
    && ctx.peekUpper() !== 'FULLTEXT'
    && ctx.peekUpper() !== 'SPATIAL'
    && ctx.peekUpper() !== 'VALUE'
    && ctx.peekUpper() !== 'TABLE'
  ) {
    ctx.setPos(start);
    return null;
  }

  const tokens = ctx.consumeTokensUntilActionBoundary();
  const text = `ADD ${ctx.tokensToSqlPreserveCase(tokens)}`.trim();
  return { type: 'raw', text };
}

function tryParseAlterAddColumnAction(ctx: DdlParser): AST.AlterAction | null {
  const start = ctx.getPos();
  if (ctx.peekUpper() !== 'ADD') return null;
  ctx.advance(); // ADD
  const explicitColumn = ctx.peekUpper() === 'COLUMN';
  if (explicitColumn) ctx.advance();

  const ifNotExists = ctx.consumeIfNotExists();

  if (
    !explicitColumn
    && (
      ctx.peekUpper() === 'CONSTRAINT'
      || (ctx.peekUpper() === 'PRIMARY' && ctx.peekUpperAt(1) === 'KEY')
      || (ctx.peekUpper() === 'FOREIGN' && ctx.peekUpperAt(1) === 'KEY')
      || ctx.peekUpper() === 'UNIQUE'
      || ctx.peekUpper() === 'CHECK'
      || ctx.peekUpper() === 'DEFAULT'
    )
  ) {
    ctx.setPos(start);
    return null;
  }

  if (ctx.peekType() !== 'identifier' && ctx.peekType() !== 'keyword') {
    ctx.setPos(start);
    return null;
  }
  const columnName = ctx.advance().value;
  const definitionTokens = ctx.consumeTokensUntilActionBoundary();
  const definition = ctx.tokensToSql(definitionTokens);

  return {
    type: 'add_column',
    explicitColumnKeyword: explicitColumn || undefined,
    ifNotExists: ifNotExists || undefined,
    columnName,
    definition: definition || undefined,
  };
}

function tryParseAlterDropColumnAction(ctx: DdlParser): AST.AlterAction | null {
  if (ctx.peekUpper() !== 'DROP' || ctx.peekUpperAt(1) !== 'COLUMN') return null;
  ctx.advance(); // DROP
  ctx.advance(); // COLUMN

  const ifExists = ctx.consumeIfExists();

  if (ctx.peekType() !== 'identifier' && ctx.peekType() !== 'keyword') return null;
  const columnName = ctx.advance().value;

  let behavior: 'CASCADE' | 'RESTRICT' | undefined;
  if (ctx.peekUpper() === 'CASCADE') {
    ctx.advance();
    behavior = 'CASCADE';
  } else if (ctx.peekUpper() === 'RESTRICT') {
    ctx.advance();
    behavior = 'RESTRICT';
  }

  return {
    type: 'drop_column',
    ifExists: ifExists || undefined,
    columnName,
    behavior,
  };
}

function tryParseAlterRenameAction(ctx: DdlParser): AST.AlterAction | null {
  if (ctx.peekUpper() !== 'RENAME') return null;
  ctx.advance(); // RENAME

  if (ctx.peekUpper() === 'TO') {
    ctx.advance();
    const newName = ctx.advance().value;
    return { type: 'rename_to', newName };
  }

  if (ctx.peekUpper() === 'COLUMN') {
    ctx.advance();
    const columnName = ctx.advance().value;
    ctx.expect('TO');
    const newName = ctx.advance().value;
    return { type: 'rename_column', columnName, newName };
  }

  return null;
}

function tryParseAlterDropConstraintAction(ctx: DdlParser): AST.AlterAction | null {
  if (ctx.peekUpper() !== 'DROP' || ctx.peekUpperAt(1) !== 'CONSTRAINT') return null;
  ctx.advance(); // DROP
  ctx.advance(); // CONSTRAINT

  const ifExists = ctx.consumeIfExists();

  if (ctx.peekType() !== 'identifier' && ctx.peekType() !== 'keyword') return null;
  const constraintName = ctx.advance().value;

  let behavior: 'CASCADE' | 'RESTRICT' | undefined;
  if (ctx.peekUpper() === 'CASCADE') {
    ctx.advance();
    behavior = 'CASCADE';
  } else if (ctx.peekUpper() === 'RESTRICT') {
    ctx.advance();
    behavior = 'RESTRICT';
  }

  return {
    type: 'drop_constraint',
    ifExists: ifExists || undefined,
    constraintName,
    behavior,
  };
}

function tryParseAlterAlterColumnAction(ctx: DdlParser): AST.AlterAction | null {
  if (ctx.peekUpper() !== 'ALTER') return null;
  ctx.advance();
  if (ctx.peekUpper() === 'COLUMN') ctx.advance();

  if (ctx.peekType() !== 'identifier' && ctx.peekType() !== 'keyword') return null;
  const columnName = ctx.advance().value;
  const operationTokens = ctx.consumeTokensUntilActionBoundary();
  const operation = ctx.tokensToSql(operationTokens);
  if (!operation) return null;

  return {
    type: 'alter_column',
    columnName,
    operation,
  };
}

function tryParseAlterOwnerToAction(ctx: DdlParser): AST.AlterAction | null {
  if (ctx.peekUpper() !== 'OWNER') return null;
  ctx.advance();

  if (ctx.peekUpper() === 'TO') {
    ctx.advance();
  }

  if (ctx.peekType() !== 'identifier' && ctx.peekType() !== 'keyword' && ctx.peekType() !== 'string') {
    return null;
  }

  let owner = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance();
    owner += '.' + ctx.advance().value;
  }

  return { type: 'owner_to', owner };
}

function tryParseAlterSetSchemaAction(ctx: DdlParser): AST.AlterAction | null {
  if (ctx.peekUpper() !== 'SET' || ctx.peekUpperAt(1) !== 'SCHEMA') return null;
  ctx.advance();
  ctx.advance();
  const schema = ctx.advance().value;
  return { type: 'set_schema', schema };
}

function tryParseAlterSetTablespaceAction(ctx: DdlParser): AST.AlterAction | null {
  if (ctx.peekUpper() !== 'SET' || ctx.peekUpperAt(1) !== 'TABLESPACE') return null;
  ctx.advance();
  ctx.advance();
  const tablespace = ctx.advance().value;
  return { type: 'set_tablespace', tablespace };
}

function parseRawAlterAction(ctx: DdlParser): AST.AlterRawAction {
  const tokens = ctx.consumeTokensUntilActionBoundary();
  return {
    type: 'raw',
    text: ctx.tokensToSql(tokens),
  };
}

const DROP_NAME_BOUNDARY_STARTERS = new Set([
  'SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'MERGE',
  'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
  'GRANT', 'REVOKE', 'COMMENT', 'CALL',
  'SET', 'RESET', 'ANALYZE', 'VACUUM',
  'REINDEX',
  'DECLARE', 'PREPARE', 'EXECUTE', 'EXEC', 'DEALLOCATE',
  'USE', 'DO', 'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE',
  'START', 'VALUES', 'COPY', 'DELIMITER',
  'GO', 'ACCEPT', 'DESCRIBE', 'REM', 'DEFINE',
]);

function isDropNameBoundaryToken(token: Token): boolean {
  if (token.value === '/' || token.value.startsWith('@')) return true;
  if (token.type === 'keyword' || token.type === 'identifier') {
    return DROP_NAME_BOUNDARY_STARTERS.has(token.upper);
  }
  return false;
}

export function parseDropStatement(ctx: DdlParser, comments: AST.CommentNode[]): AST.DropTableStatement {
  ctx.expect('DROP');
  const objectTypeToken = ctx.advance();
  if (objectTypeToken.type !== 'keyword' && objectTypeToken.type !== 'identifier') {
    throw ctx.parseError('object type', objectTypeToken);
  }
  const objectType = objectTypeToken.upper;

  let concurrently = false;
  if (ctx.peekUpper() === 'CONCURRENTLY') {
    ctx.advance();
    concurrently = true;
  }

  const ifExists = ctx.consumeIfExists();

  const objectNameTokens: Token[] = [];
  let depth = 0;
  while (!ctx.isAtEnd() && !ctx.check(';')) {
    const token = ctx.peek();
    if (depth === 0) {
      if (token.upper === 'CASCADE' || token.upper === 'RESTRICT') break;
      if ((token.type === 'line_comment' || token.type === 'block_comment') && objectNameTokens.length > 0) break;
      if (objectNameTokens.length > 0 && isDropNameBoundaryToken(token)) break;
    }

    const consumed = ctx.advance();
    objectNameTokens.push(consumed);
    if (consumed.value === '(' || consumed.value === '[' || consumed.value === '{') depth++;
    if (consumed.value === ')' || consumed.value === ']' || consumed.value === '}') depth = Math.max(0, depth - 1);
  }
  const objectName = ctx.tokensToSqlPreserveCase(objectNameTokens);
  if (!objectName) {
    throw ctx.parseError('object name', ctx.peek());
  }

  let behavior: 'CASCADE' | 'RESTRICT' | 'CASCADE CONSTRAINT' | 'CASCADE CONSTRAINTS' | undefined;
  if (ctx.peekUpper() === 'CASCADE') {
    ctx.advance();
    if (ctx.peekUpper() === 'CONSTRAINTS') {
      ctx.advance();
      behavior = 'CASCADE CONSTRAINTS';
    } else if (ctx.peekUpper() === 'CONSTRAINT') {
      ctx.advance();
      behavior = 'CASCADE CONSTRAINT';
    } else {
      behavior = 'CASCADE';
    }
  } else if (ctx.peekUpper() === 'RESTRICT') {
    ctx.advance();
    behavior = 'RESTRICT';
  }

  return {
    type: 'drop_table',
    objectType,
    concurrently: concurrently || undefined,
    ifExists,
    objectName,
    behavior,
    leadingComments: comments,
  };
}
