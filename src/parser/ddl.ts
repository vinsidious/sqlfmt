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
  consumeTokensUntilActionBoundary(): Token[];
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

  const kw = ctx.peekUpper();
  if (kw === 'TABLE') return parseCreateTableStatement(ctx, comments, statementStart);
  if (kw === 'INDEX') return parseCreateIndexStatement(ctx, comments, unique);
  if (kw === 'VIEW') return parseCreateViewStatement(ctx, comments, orReplace, materialized);
  if (kw === 'POLICY') return parseCreatePolicyStatement(ctx, comments);

  ctx.setPos(statementStart);
  const raw = ctx.parseRawStatement('unsupported');
  if (!raw) throw ctx.parseError('CREATE statement', ctx.peek());
  return raw;
}

function parseCreateTableStatement(
  ctx: DdlParser,
  comments: AST.CommentNode[],
  statementStart: number,
): AST.Node {
  ctx.expect('TABLE');
  const ifNotExists = ctx.consumeIfNotExists();
  let tableName = ctx.advance().value;
  while (ctx.check('.')) {
    ctx.advance(); // consume dot
    tableName += '.' + ctx.advance().value;
  }
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
      tableName: fullName,
      elements: [],
      asQuery: query,
      leadingComments: comments,
    };
  }

  if (!ctx.check('(')) {
    ctx.setPos(statementStart);
    const raw = ctx.parseRawStatement('unsupported');
    if (!raw) throw ctx.parseError('CREATE TABLE statement', ctx.peek());
    if (comments.length === 0) return raw;
    return { type: 'raw', text: `${comments.map(c => c.text).join('\n')}\n${raw.text}`.trim(), reason: 'unsupported' };
  }

  ctx.expect('(');
  if (hasCommentsInCreateTableElements(ctx)) {
    ctx.setPos(statementStart);
    const raw = ctx.parseRawStatement('unsupported');
    if (!raw) throw ctx.parseError('CREATE TABLE statement', ctx.peek());
    if (comments.length === 0) return raw;
    return { type: 'raw', text: `${comments.map(c => c.text).join('\n')}\n${raw.text}`.trim(), reason: 'unsupported' };
  }
  const { elements, trailingComma } = ctx.parseTableElements();
  ctx.expect(')');

  // PostgreSQL storage parameters and related trailing clauses are currently
  // preserved verbatim to avoid strict-mode failures.
  if (!ctx.isAtEnd() && !ctx.check(';')) {
    ctx.setPos(statementStart);
    const raw = ctx.parseRawStatement('unsupported');
    if (!raw) throw ctx.parseError('CREATE TABLE statement', ctx.peek());
    if (comments.length === 0) return raw;
    return { type: 'raw', text: `${comments.map(c => c.text).join('\n')}\n${raw.text}`.trim(), reason: 'unsupported' };
  }

  return {
    type: 'create_table',
    tableName: fullName,
    elements,
    trailingComma: trailingComma || undefined,
    leadingComments: comments,
  };
}

function hasCommentsInCreateTableElements(ctx: DdlParser): boolean {
  const start = ctx.getPos();
  let depth = 1;
  let hasComment = false;

  while (!ctx.isAtEnd() && depth > 0) {
    const token = ctx.advance();
    if (token.type === 'line_comment' || token.type === 'block_comment') {
      hasComment = true;
    } else if (token.value === '(') {
      depth++;
    } else if (token.value === ')') {
      depth--;
    }
  }

  ctx.setPos(start);
  return hasComment;
}

function parseCreateIndexStatement(
  ctx: DdlParser,
  comments: AST.CommentNode[],
  unique: boolean
): AST.CreateIndexStatement {
  ctx.advance(); // INDEX

  let concurrently = false;
  if (ctx.peekUpper() === 'CONCURRENTLY') {
    ctx.advance();
    concurrently = true;
  }

  const ifNotExists = ctx.consumeIfNotExists();
  const name = ctx.advance().value;

  ctx.expect('ON');
  const table = ctx.advance().value;

  let using: string | undefined;
  if (ctx.peekUpper() === 'USING') {
    ctx.advance();
    using = ctx.advance().upper;
  }

  ctx.expect('(');
  const columns: AST.Expression[] = [parseIndexColumn(ctx)];
  while (ctx.check(',')) {
    ctx.advance();
    columns.push(parseIndexColumn(ctx));
  }
  ctx.expect(')');

  let where: AST.Expression | undefined;
  if (ctx.peekUpper() === 'WHERE') {
    ctx.advance();
    where = ctx.parseExpression();
  }

  return {
    type: 'create_index',
    unique,
    concurrently,
    ifNotExists,
    name,
    table,
    using,
    columns,
    where,
    leadingComments: comments,
  };
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
  materialized: boolean
): AST.CreateViewStatement {
  ctx.advance(); // VIEW

  const ifNotExists = ctx.consumeIfNotExists();
  const name = ctx.advance().value;

  ctx.expect('AS');
  const query = ctx.parseStatement();
  if (query && query.type !== 'select' && query.type !== 'union' && query.type !== 'cte') {
    throw ctx.parseError('SELECT, UNION, or WITH query in CREATE VIEW', ctx.peek());
  }

  let withData: boolean | undefined;
  if (ctx.peekUpper() === 'WITH') {
    ctx.advance();
    if (ctx.peekUpper() === 'DATA') {
      ctx.advance();
      withData = true;
    } else if (ctx.peekUpper() === 'NO' && ctx.peekUpperAt(1) === 'DATA') {
      ctx.advance();
      ctx.advance();
      withData = false;
    }
  }

  return {
    type: 'create_view',
    orReplace,
    materialized,
    ifNotExists,
    name,
    query: query as AST.Statement,
    withData,
    leadingComments: comments,
  };
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
    using = ctx.parseExpression();
    ctx.expect(')');
  }

  let withCheck: AST.Expression | undefined;
  if (ctx.peekUpper() === 'WITH') {
    ctx.advance();
    ctx.expect('CHECK');
    ctx.expect('(');
    withCheck = ctx.parseExpression();
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
  while (!ctx.isAtEnd() && !ctx.check(';')) {
    const upper = ctx.peekUpper();
    if (
      objectNameTokens.length > 0
      && (upper === 'ADD' || upper === 'DROP' || upper === 'RENAME' || upper === 'SET' || upper === 'ALTER')
    ) {
      break;
    }
    objectNameTokens.push(ctx.advance());
  }
  const objectName = ctx.tokensToSql(objectNameTokens);
  if (!objectName) {
    throw ctx.parseError('object name', ctx.peek());
  }

  const actions: AST.AlterAction[] = [];
  while (!ctx.isAtEnd() && !ctx.check(';')) {
    actions.push(parseAlterAction(ctx));
    if (ctx.check(',')) {
      ctx.advance();
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

function parseAlterAction(ctx: DdlParser): AST.AlterAction {
  return (
    tryParseAlterAddNonColumnAction(ctx)
    ?? tryParseAlterAddColumnAction(ctx)
    ?? tryParseAlterDropColumnAction(ctx)
    ?? tryParseAlterDropConstraintAction(ctx)
    ?? tryParseAlterAlterColumnAction(ctx)
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
  ) {
    ctx.setPos(start);
    return null;
  }

  const tokens = ctx.consumeTokensUntilActionBoundary();
  const text = `ADD ${ctx.tokensToSql(tokens)}`.trim();
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
  const objectName = ctx.tokensToSql(objectNameTokens);
  if (!objectName) {
    throw ctx.parseError('object name', ctx.peek());
  }

  let behavior: 'CASCADE' | 'RESTRICT' | 'CASCADE CONSTRAINTS' | undefined;
  if (ctx.peekUpper() === 'CASCADE') {
    ctx.advance();
    if (ctx.peekUpper() === 'CONSTRAINTS') {
      ctx.advance();
      behavior = 'CASCADE CONSTRAINTS';
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
