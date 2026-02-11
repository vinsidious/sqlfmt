import { tokenize, Token } from './tokenizer';
import * as AST from './ast';
import { parseComparisonExpression, parsePrimaryExpression } from './parser/expressions';
import { DEFAULT_MAX_DEPTH } from './constants';
import type { SQLDialect } from './dialect';
import {
  type DmlParser,
  parseDeleteStatement,
  parseInsertStatement,
  parseSetItem as parseDmlSetItem,
  parseUpdateStatement,
} from './parser/dml';
import {
  type DdlParser,
  parseAlterStatement,
  parseCreateStatement,
  parseDropStatement,
} from './parser/ddl';
const BASE_CLAUSE_KEYWORDS = new Set([
  'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
  'UNION', 'INTERSECT', 'EXCEPT', 'ON', 'SET', 'VALUES',
  'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'JOIN',
  'INTO', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER',
  'DROP', 'WITH', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR',
  'RETURNING', 'FETCH', 'WINDOW', 'LATERAL', 'FOR', 'USING', 'ESCAPE',
  'PIVOT', 'UNPIVOT', 'GO',
  'START', 'CONNECT', 'BY', 'PRIOR', 'NOCYCLE',
]);
const FUNCTION_KEYWORD_OVERRIDES = new Set([
  // Can appear as join keywords and as scalar functions.
  'LEFT',
  'RIGHT',
]);
const CAST_STYLE_FUNCTIONS = new Set(['SAFE_CAST', 'TRY_CAST']);
const STRING_ALIAS_TYPED_LITERAL_DISALLOWED = new Set([
  'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMPTZ', 'INTERVAL',
  'CHAR', 'CHARACTER', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'TEXT',
  'BIT', 'BINARY', 'VARBINARY',
  'BOOL', 'BOOLEAN',
  'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
  'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL', 'MONEY',
  'JSON', 'JSONB', 'UUID',
]);
const IMPLICIT_STATEMENT_STARTERS = new Set([
  'SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'MERGE',
  'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
  'GRANT', 'REVOKE', 'COMMENT', 'CALL',
  'EXPLAIN',
  'PRAGMA', 'SHOW', 'FLUSH',
  'LOCK', 'UNLOCK',
  'BACKUP', 'BULK', 'CLUSTER',
  'PRINT',
  'SET', 'RESET', 'ANALYZE', 'VACUUM',
  'REINDEX',
  'DECLARE', 'PREPARE', 'EXECUTE', 'EXEC', 'DEALLOCATE',
  'USE', 'DO', 'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE',
  'START', 'VALUES', 'COPY', 'DELIMITER',
  'GO', 'DBCC', 'ACCEPT', 'DESC', 'DESCRIBE', 'REM', 'DEFINE', 'PROMPT', 'SP_RENAME',
]);

const CREATE_KEYWORD_NORMALIZED_TYPES = new Set([
  'DATABASE',
  'ROLE',
  'USER',
  'PUBLICATION',
  'EXTENSION',
]);

const SELECT_PROJECTION_BOUNDARY_KEYWORDS = new Set([
  'FROM', 'INTO', 'WHERE', 'GROUP', 'HAVING', 'WINDOW', 'ORDER', 'LIMIT',
  'OFFSET', 'FETCH', 'FOR', 'UNION', 'INTERSECT', 'EXCEPT',
]);

// Lookup table for multi-word SQL type names.
// Key = last consumed word, value = set of valid next words.
const TYPE_CONTINUATIONS: Record<string, Set<string>> = {
  DOUBLE: new Set(['PRECISION']),
  CHARACTER: new Set(['VARYING']),
  CHAR: new Set(['VARYING']),
  NATIONAL: new Set(['CHARACTER']),
  TIMESTAMP: new Set(['WITH', 'WITHOUT']),
  TIME: new Set(['WITH', 'WITHOUT', 'ZONE']),
  WITH: new Set(['TIME']),
  WITHOUT: new Set(['TIME']),
  TINYINT: new Set(['UNSIGNED', 'SIGNED']),
  SMALLINT: new Set(['UNSIGNED', 'SIGNED']),
  MEDIUMINT: new Set(['UNSIGNED', 'SIGNED']),
  INT: new Set(['UNSIGNED', 'SIGNED']),
  INTEGER: new Set(['UNSIGNED', 'SIGNED']),
  BIGINT: new Set(['UNSIGNED', 'SIGNED']),
  DECIMAL: new Set(['UNSIGNED', 'SIGNED']),
  NUMERIC: new Set(['UNSIGNED', 'SIGNED']),
  FLOAT: new Set(['UNSIGNED', 'SIGNED']),
  REAL: new Set(['UNSIGNED', 'SIGNED']),
  UNSIGNED: new Set(['ZEROFILL']),
  SIGNED: new Set(['ZEROFILL']),
};

function isQuotedIdentifierToken(value: string): boolean {
  return value.startsWith('"') || value.startsWith('`') || value.startsWith('[');
}

/**
 * Options for {@link parse} and the {@link Parser} constructor.
 */
export interface ParseOptions {
  /**
   * When `true` (default), unparseable statements are preserved as
   * `RawStatement` nodes instead of throwing.
   *
   * @default true
   */
  recover?: boolean;

  /**
   * Maximum allowed nesting depth. Exceeding this limit throws an error
   * to prevent stack overflow on deeply nested or adversarial input.
   *
   * @default 200
   */
  maxDepth?: number;

  /**
   * Optional callback invoked when recovery mode captures an unparseable
   * statement as a `RawExpression`.
   *
   * Not called when `recover` is `false` or when parsing fails with
   * `MaxDepthError` (which always throws).
   */
  onRecover?: (error: ParseError, raw: AST.RawExpression | null, context: ParseRecoveryContext) => void;

  /**
   * Optional callback invoked when recovery cannot produce raw text for a
   * failed statement (for example, the error occurs at end-of-input).
   *
   * This makes statement drops explicit to callers in recovery mode.
   */
  onDropStatement?: (error: ParseError, context: ParseRecoveryContext) => void;

  /**
   * Optional SQL dialect extensions (keywords + clause boundaries).
   *
   * When omitted, holywell uses its default PostgreSQL-first behavior.
   */
  dialect?: SQLDialect;

  /**
   * Maximum token count allowed during tokenization.
   *
   * Defaults to the global tokenizer safety limit. Increase this for very
   * large SQL dumps when running in trusted environments.
   */
  maxTokenCount?: number;
}

export interface ParseRecoveryContext {
  statementIndex: number;
  totalStatements: number;
}

/**
 * Thrown when the parser encounters unexpected tokens and recovery is disabled.
 *
 * Carries the offending {@link Token} plus the human-readable description of
 * what was expected, making it straightforward to build rich diagnostics.
 *
 * @example
 * ```typescript
 * import { parse, ParseError } from 'holywell';
 *
 * try {
 *   parse('SELECT FROM;', { recover: false });
 * } catch (err) {
 *   if (err instanceof ParseError) {
 *     console.error(`${err.line}:${err.column} - expected ${err.expected}`);
 *   }
 * }
 * ```
 */
export class ParseError extends Error {
  /** The token that triggered the error. */
  readonly token: Token;
  /** Human-readable description of what the parser expected. */
  readonly expected: string;
  /** One-based line number of the offending token. */
  readonly line: number;
  /** One-based column number of the offending token. */
  readonly column: number;

  constructor(expected: string, token: Token) {
    const got = token.type === 'eof' ? 'end of input' : `"${token.value}" (${token.type})`;
    super(`Expected ${expected}, got ${got}`);
    this.name = 'ParseError';
    this.expected = expected;
    this.token = token;
    this.line = token.line;
    this.column = token.column;
  }
}

/**
 * Thrown when the parser exceeds the configured maximum nesting depth.
 *
 * This is a subclass of {@link ParseError} so callers catching `ParseError`
 * still receive it, but it can be distinguished via `instanceof` when depth
 * violations need special handling (e.g. never recovered to RawStatement).
 */
export class MaxDepthError extends ParseError {
  /** The configured nesting depth limit that was exceeded. */
  readonly maxDepth: number;

  constructor(maxDepth: number, token: Token) {
    super(`maximum nesting depth ${maxDepth}`, token);
    this.name = 'MaxDepthError';
    this.maxDepth = maxDepth;
  }
}

/**
 * Recursive-descent SQL parser.
 *
 * Consumes an array of {@link Token} objects (produced by {@link tokenize}) and
 * builds an AST of {@link AST.Node} trees. Supports SELECT, INSERT, UPDATE,
 * DELETE, MERGE, CTEs, DDL (CREATE/ALTER/DROP TABLE, CREATE INDEX, CREATE VIEW),
 * GRANT/REVOKE, and PostgreSQL-specific syntax.
 *
 * The parser can operate in two modes controlled by {@link ParseOptions.recover}:
 * - **Recover mode** (default): unparseable statements are preserved as
 *   `RawStatement` nodes so the formatter can pass them through unchanged.
 * - **Strict mode**: parsing failures throw a {@link ParseError}.
 *
 * @example
 * import { tokenize, Parser } from 'holywell';
 *
 * const tokens = tokenize('SELECT 1; SELECT 2;');
 * const parser = new Parser(tokens);
 * const ast = parser.parseStatements();
 * // ast is an array of AST.Node (two SelectStatement nodes)
 */
export class Parser {
  private tokens: Token[];
  private readonly source?: string;
  private pos: number = 0;
  private activeDelimiter: string = ';';
  private blankLinesBeforeToken = new Map<number, number>();
  private readonly recover: boolean;
  private readonly maxDepth: number;
  private readonly onRecover?: (error: ParseError, raw: AST.RawExpression | null, context: ParseRecoveryContext) => void;
  private readonly onDropStatement?: (error: ParseError, context: ParseRecoveryContext) => void;
  private readonly clauseKeywords: Set<string>;
  private readonly totalStatements: number;
  private currentStatementIndex: number = 0;
  private depth: number = 0;
  private parsingArraySubscriptBounds: boolean = false;

  constructor(tokens: Token[], options: ParseOptions = {}, source?: string) {
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
    this.source = source;
    this.recover = options.recover ?? true;
    this.maxDepth = options.maxDepth ?? DEFAULT_MAX_DEPTH;
    this.onRecover = options.onRecover;
    this.onDropStatement = options.onDropStatement;
    this.clauseKeywords = new Set(BASE_CLAUSE_KEYWORDS);
    for (const kw of options.dialect?.clauseKeywords ?? []) {
      this.clauseKeywords.add(kw.toUpperCase());
    }
    this.totalStatements = this.estimateTotalStatements();
  }

  private estimateTotalStatements(): number {
    let total = 0;
    let hasContent = false;
    for (const token of this.tokens) {
      if (token.type === 'eof') break;
      if (token.value === ';') {
        if (hasContent) {
          total++;
          hasContent = false;
        }
        continue;
      }
      hasContent = true;
    }
    if (hasContent) total++;
    return total;
  }

  private isKeywordSequence(...keywords: string[]): boolean {
    for (let i = 0; i < keywords.length; i++) {
      if (this.peekUpperAt(i) !== keywords[i]) return false;
    }
    return true;
  }

  private consumeKeywordSequence(...keywords: string[]): boolean {
    if (!this.isKeywordSequence(...keywords)) return false;
    for (let i = 0; i < keywords.length; i++) {
      this.advance();
    }
    return true;
  }

  private consumeIfNotExists(): boolean {
    return this.consumeKeywordSequence('IF', 'NOT', 'EXISTS');
  }

  private consumeIfExists(): boolean {
    return this.consumeKeywordSequence('IF', 'EXISTS');
  }

  /**
   * Parse all statements from the token stream.
   *
   * **Recovery mode** (`this.recover === true`, the default):
   * When a statement fails to parse, the error is caught and the parser
   * rewinds to the start of that statement. The tokens are then consumed
   * as a {@link AST.RawExpression} (raw text) up to the next semicolon.
   * This allows the formatter to pass through unrecognized SQL verbatim
   * rather than aborting the entire input.
   *
   * Recovery is limited to ordinary {@link ParseError}s. The following
   * errors always propagate regardless of recovery mode:
   *   - {@link MaxDepthError} — nesting depth exceeded (security guard)
   *   - Non-ParseError exceptions (e.g. runtime TypeError)
   *
   * **Strict mode** (`this.recover === false`):
   * All ParseErrors propagate immediately, letting the caller handle them.
   *
   * API consumers should be aware that in recovery mode the returned array
   * may contain `RawExpression` nodes alongside structured statement nodes.
   * These raw nodes preserve the original text and can be detected via
   * `node.type === 'raw'`.
   */
  parseStatements(): AST.Node[] {
    const stmts: AST.Node[] = [];
    while (!this.isAtEnd()) {
      this.skipSemicolons();
      if (this.isAtEnd()) break;
      this.currentStatementIndex++;
      const stmtStart = this.pos;
      const recoveryContext: ParseRecoveryContext = {
        statementIndex: this.currentStatementIndex,
        totalStatements: this.totalStatements,
      };
      try {
        const stmt = this.parseStatement();
        if (
          stmt
          && stmt.type !== 'raw'
          && !this.isAtEnd()
          && !this.check(';')
          && !this.hasImplicitStatementBoundary()
        ) {
          throw new ParseError(';', this.peek());
        }
        if (stmt) {
          stmts.push(stmt);
          if (stmt.type !== 'raw' && this.check(';')) {
            const semi = this.advance();
            if (this.isSyntheticSlashSemicolon(semi)) {
              stmts.push({
                type: 'raw',
                text: '/',
                reason: 'slash_terminator',
              });
              continue;
            }
            if (this.peekType() === 'line_comment' && this.peek().line === semi.line) {
              const trailing = this.advance();
              stmts.push({
                type: 'raw',
                text: trailing.value,
                reason: 'trailing_semicolon_comment',
              });
            } else if (this.peek().value === '\\' && this.peek().line === semi.line) {
              const inlineMeta = this.parseSingleLineStatement([], 'trailing_semicolon_comment');
              if (inlineMeta) stmts.push(inlineMeta);
            } else if (
              this.source !== undefined
              && this.peek().line === semi.line
              && (
                this.source[this.peek().position] === '\\'
                || (this.peek().position > 0 && this.source[this.peek().position - 1] === '\\')
              )
            ) {
              const startPos = this.source[this.peek().position] === '\\'
                ? this.peek().position
                : this.peek().position - 1;
              const metaLine = this.peek().line;
              let endPos = startPos + 1;
              while (!this.isAtEnd() && this.peek().line === metaLine) {
                const token = this.advance();
                const startsWithBackslash = this.source[token.position] === '\\' ? 1 : 0;
                endPos = token.position + token.value.length + startsWithBackslash;
              }
              const metaText = this.source.slice(startPos, endPos).trim();
              if (metaText) {
                stmts.push({
                  type: 'raw',
                  text: metaText,
                  reason: 'trailing_semicolon_comment',
                });
              }
            }
          }
        }
      } catch (err) {
        if (!this.recover) throw err;
        if (!(err instanceof ParseError)) throw err;
        if (err instanceof MaxDepthError) throw err;
        // Recovery: rewind and consume as raw text until next semicolon
        this.pos = stmtStart;
        const recovered = this.parseRawStatement('parse_error');
        const dialectUnsupported = recovered ? this.looksLikeSqlServerStatement(recovered.text) : false;
        const raw = dialectUnsupported
          ? ({ ...recovered, reason: 'unsupported' } as AST.RawExpression)
          : recovered;
        if (!dialectUnsupported) {
          this.onRecover?.(err, raw, recoveryContext);
        }
        if (raw) {
          stmts.push(raw);
        } else {
          if (!dialectUnsupported) {
            this.onRecover?.(err, null, recoveryContext);
          }
          if (this.onDropStatement) {
            this.onDropStatement(err, recoveryContext);
          } else {
            console.error(
              `Warning: dropped statement ${recoveryContext.statementIndex}/${recoveryContext.totalStatements} after parse failure at line ${err.line}, column ${err.column}`
            );
          }
        }
      }
      this.skipSemicolons();
    }
    return stmts;
  }

  private looksLikeSqlServerStatement(text: string): boolean {
    if (/\{\s*fn\b/i.test(text)) return true;
    if (/(^|\n)\s*GO\s*(\n|$)/i.test(text)) return true;
    if (/@[A-Za-z_][A-Za-z0-9_]*/.test(text)) return true;
    const hasBracketIdentifier = /\[[A-Za-z_][^\]\r\n]*\]/.test(text);
    const hasSqlKeyword = /\b(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|USE|EXEC|MERGE)\b/i.test(text);
    return hasBracketIdentifier && hasSqlKeyword;
  }

  private isStatementStarterToken(token: Token | undefined): boolean {
    if (!token || token.type === 'eof') return false;
    if (token.value === '/' || token.value.startsWith('@')) return true;
    if (IMPLICIT_STATEMENT_STARTERS.has(token.upper)) return true;
    return false;
  }

  private hasImplicitStatementBoundary(): boolean {
    if (this.pos <= 0 || this.pos >= this.tokens.length) return false;
    const previous = this.tokens[this.pos - 1];
    if (!previous || previous.type === 'eof') return false;
    if (previous.value === '\\') return false;

    let lookahead = this.pos;
    while (
      lookahead < this.tokens.length
      && (this.tokens[lookahead].type === 'line_comment' || this.tokens[lookahead].type === 'block_comment')
    ) {
      lookahead++;
    }
    const next = this.tokens[lookahead];
    if (!this.isStatementStarterToken(next)) return false;
    return (next?.line ?? 0) > previous.line;
  }

  private skipSemicolons(): void {
    while (this.check(';')) {
      if (this.isSyntheticSlashSemicolon(this.peek())) break;
      this.advance();
    }
  }

  private parseStatement(): AST.Node | null {
    const statementStartPos = this.peek().position;
    const comments = this.consumeComments();

    if (this.isAtEnd()) {
      if (comments.length === 0) return null;
      return this.commentsToRaw(comments);
    }

    // SQL*Plus run terminator: slash on its own line (tokenized as synthetic ";").
    if (this.check(';') && this.isSyntheticSlashSemicolon(this.peek())) {
      this.advance();
      return this.combineCommentsWithRaw(
        comments,
        { type: 'raw', text: '/', reason: 'trailing_semicolon_comment' },
        'trailing_semicolon_comment',
      );
    }

    // T-SQL leading terminator before CTE: ;WITH ...
    if (this.check(';') && this.peekUpperAt(1) === 'WITH') {
      this.advance();
    }

    // Comment-only statement terminated by a semicolon.
    if (this.check(';')) {
      const semi = this.advance();
      if (comments.length === 0) return null;
      if (this.source !== undefined) {
        const text = this.source.slice(statementStartPos, semi.position + semi.value.length).trim();
        if (!text) return null;
        return { type: 'raw', text, reason: 'comment_only' };
      }
      return {
        type: 'raw',
        text: `${comments.map(c => c.text).join('\n')};`,
        reason: 'comment_only',
      };
    }

    if (this.isStandaloneSlashTerminator()) {
      this.advance();
      return null;
    }

    // Parenthesized top-level query expression
    if (this.check('(')) {
      const query = this.tryParseQueryExpressionAtCurrent(comments);
      if (query) return query;
    }

    const kw = this.peekUpper();
    const currentToken = this.peek();

    // SQL*Plus commands prefixed with @ and slash-execute lines.
    if (currentToken.value === '/' || currentToken.value.startsWith('@')) {
      return this.parseSingleLineStatement(comments, 'unsupported');
    }

    if (this.activeDelimiter !== ';' && kw !== 'DELIMITER') {
      return this.parseCustomDelimitedStatement(comments);
    }

    if (kw === 'CREATE' && this.looksLikeCreateRoutineStatement()) {
      const routineKind = this.getCreateRoutineKind();
      const allowPreBeginSemicolons = this.hasKeywordAhead('BEGIN');
      const normalizeKeywords = routineKind === 'PROCEDURE' || routineKind === 'TRIGGER' || routineKind === 'FUNCTION';
      return this.parseStatementUntilEndBlock(comments, 'unsupported', {
        normalizeKeywords,
        allowPreBeginSemicolons,
      });
    }
    if (kw === 'ALTER' && this.looksLikeAlterRoutineStatement()) {
      const allowPreBeginSemicolons = this.hasKeywordAhead('BEGIN');
      return this.parseStatementUntilEndBlock(comments, 'unsupported', {
        normalizeKeywords: true,
        allowPreBeginSemicolons,
      });
    }
    if (kw === 'BEGIN' && this.hasKeywordAhead('END')) {
      return this.parseStatementUntilEndBlock(comments, 'unsupported');
    }
    if (kw === 'IF' && this.hasKeywordAhead('BEGIN') && this.hasKeywordAhead('END')) {
      return this.parseStatementUntilEndBlock(comments, 'unsupported', {
        normalizeKeywords: true,
        allowElseChain: true,
      });
    }

    if (kw === 'WITH') return this.parseCTE(comments);
    if (kw === 'SELECT') return this.parseUnionOrSelect(comments);
    if (kw === 'EXPLAIN') return this.parseExplain(comments);
    if (kw === 'INSERT') {
      if (this.peekUpperAt(1) === 'ALL') {
        return this.parseVerbatimStatement(comments, 'unsupported', true);
      }
      return this.parseInsert(comments);
    }
    if (kw === 'UPDATE') return this.parseUpdate(comments);
    if (kw === 'DELETE') return this.parseDelete(comments);
    if (kw === 'CREATE' && this.looksLikeCreateTypeBodyStatement()) {
      return this.parseCreateTypeBody(comments);
    }
    if (kw === 'CREATE') {
      const createType = this.getCreateKeywordNormalizedType();
      if (createType) {
        let allowImplicitBoundary = createType !== 'ROLE' && createType !== 'USER';
        if (createType === 'SCHEMA' && this.hasCreateSchemaEmbeddedDdl()) {
          allowImplicitBoundary = false;
        }
        return this.parseKeywordNormalizedStatement(comments, 'unsupported', allowImplicitBoundary);
      }
      return this.parseCreate(comments);
    }
    if (kw === 'ALTER') {
      if (this.looksLikeAlterViewWithQueryStatement()) {
        return this.parseAlterViewWithQuery(comments);
      }
      if (this.peekUpperAt(1) === 'DEFAULT' && this.peekUpperAt(2) === 'PRIVILEGES') {
        return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
      }
      if (
        (this.peekUpperAt(1) === 'SESSION' || this.peekUpperAt(1) === 'SYSTEM' || this.peekUpperAt(1) === 'DATABASE')
        && this.peekUpperAt(2) === 'SET'
      ) {
        return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
      }
      return this.parseAlter(comments);
    }
    if (kw === 'DROP') return this.parseDrop(comments);
    if (kw === 'MERGE') {
      if (this.looksLikeMergeIntoValuesShorthand()) {
        return this.parseVerbatimStatement(comments, 'unsupported', true);
      }
      return this.parseMerge(comments);
    }
    if (kw === 'GRANT' || kw === 'REVOKE') return this.parseGrant(comments);
    if (kw === 'TRUNCATE') return this.parseTruncate(comments);
    if (kw === 'VALUES') return this.parseStandaloneValues(comments);
    if (kw === 'COPY') return this.parseCopyStatement(comments);
    if (kw === 'DELIMITER') return this.parseDelimiterScript(comments);
    if (kw === 'GO') return this.parseSingleLineStatement(comments, 'unsupported');
    if (kw === 'BACKUP' || kw === 'BULK' || kw === 'CLUSTER') {
      return this.parseVerbatimStatement(comments, 'unsupported');
    }
    if (kw === 'DBCC') {
      return this.parseVerbatimStatement(comments, 'unsupported', true);
    }
    if (kw === 'DESC' || kw === 'DESCRIBE') {
      return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
    }
    if (kw === 'ACCEPT' || kw === 'DESCRIBE' || kw === 'REM' || kw === 'DEFINE' || kw === 'PROMPT') {
      return this.parseSingleLineStatement(comments, 'unsupported');
    }
    if (kw === 'SHOW') {
      return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
    }
    if (kw === 'PRAGMA' || kw === 'FLUSH' || kw === 'LOCK' || kw === 'UNLOCK') {
      return this.parseVerbatimStatement(comments, 'unsupported', true);
    }
    if (kw === 'COMMENT') {
      return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
    }
    if (kw === 'CALL') {
      return this.parseVerbatimStatement(comments, 'unsupported', true);
    }
    if (kw === 'PRINT') {
      return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
    }

    if (this.peekType() === 'identifier' && this.peekUpper() === 'SP_RENAME') {
      return this.parseVerbatimStatement(comments, 'unsupported', true);
    }

    if (kw === 'DECLARE' && this.hasKeywordAhead('BEGIN') && this.hasKeywordAhead('END')) {
      return this.parseStatementUntilEndBlock(comments, 'unsupported', { allowPreBeginSemicolons: true });
    }

    if (kw === 'USE' || kw === 'SET') {
      return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
    }

    if (
      kw === 'RESET'
      || kw === 'ANALYZE'
      || kw === 'VACUUM'
      || kw === 'REINDEX'
      || kw === 'DECLARE'
      || kw === 'PREPARE'
      || kw === 'EXECUTE'
      || kw === 'EXEC'
      || kw === 'DEALLOCATE'
      || kw === 'DO'
      || kw === 'END'
    ) {
      return this.parseVerbatimStatement(comments, 'unsupported', true);
    }

    // Transaction control — consume tokens without the semicolon (parseStatements handles it)
    if (kw === 'BEGIN' || kw === 'COMMIT' || kw === 'ROLLBACK' || kw === 'SAVEPOINT' || kw === 'RELEASE'
        || (kw === 'START' && this.peekUpperAt(1) === 'TRANSACTION')) {
      return this.parseKeywordNormalizedStatement(comments, 'transaction_control', true);
    }

    // Unknown statement — in strict mode, fail instead of falling back to raw
    if (!this.recover) {
      throw new ParseError('SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, ...)', this.peek());
    }
    const raw = this.parseRawStatement('unsupported', { allowImplicitBoundary: true });
    if (!raw) {
      if (comments.length === 0) return null;
      return this.commentsToRaw(comments);
    }
    if (comments.length === 0) return raw;
    return {
      type: 'raw',
      text: `${this.commentsToRaw(comments).text}\n${raw.text}`.trim(),
      reason: 'unsupported',
    };
  }

  private getCreateRoutineKind(): 'PROCEDURE' | 'FUNCTION' | 'TRIGGER' | 'EVENT' | null {
    if (this.peekUpper() !== 'CREATE') return null;
    for (let i = 1; i < 40; i++) {
      const token = this.peekAt(i);
      if (token.type === 'eof' || token.value === ';') break;
      if (token.upper === 'TABLE' || token.upper === 'INDEX' || token.upper === 'VIEW' || token.upper === 'POLICY') {
        return null;
      }
      if (
        token.upper === 'PROCEDURE'
        || token.upper === 'PROC'
        || token.upper === 'FUNCTION'
        || token.upper === 'TRIGGER'
        || token.upper === 'EVENT'
      ) {
        if (token.upper === 'PROC') return 'PROCEDURE';
        return token.upper as 'PROCEDURE' | 'FUNCTION' | 'TRIGGER' | 'EVENT';
      }
    }
    return null;
  }

  private looksLikeCreateRoutineStatement(): boolean {
    if (this.looksLikeCreateTypeBodyStatement()) return false;
    return this.getCreateRoutineKind() !== null;
  }

  private looksLikeCreateTypeBodyStatement(): boolean {
    if (this.peekUpper() !== 'CREATE') return false;
    let offset = 1;
    if (this.peekUpperAt(offset) === 'OR' && this.peekUpperAt(offset + 1) === 'REPLACE') {
      offset += 2;
    }
    return this.peekUpperAt(offset) === 'TYPE' && this.peekUpperAt(offset + 1) === 'BODY';
  }

  private parseCreateTypeBody(
    comments: AST.CommentNode[],
    reason: AST.RawReason = 'unsupported',
  ): AST.RawExpression | null {
    const startPos = this.peek().position;
    let endPos = startPos;
    let beginDepth = 0;

    while (!this.isAtEnd()) {
      const token = this.advance();
      endPos = token.position + token.value.length;

      if (token.upper === 'BEGIN') {
        beginDepth++;
        continue;
      }

      if (token.upper === 'END') {
        if (!this.isControlFlowEndQualifier(this.peekUpper())) {
          if (beginDepth > 0) {
            beginDepth--;
            continue;
          }

          if (this.check(';')) {
            const semi = this.advance();
            endPos = semi.position + semi.value.length;
          }
          if (this.check('/')) {
            const slash = this.advance();
            endPos = slash.position + slash.value.length;
          }
          break;
        }
      }
    }

    return this.buildRawFromSourceSlice(comments, startPos, endPos, reason);
  }

  private getCreateKeywordNormalizedType(): string | null {
    if (this.peekUpper() !== 'CREATE') return null;
    let offset = 1;
    if (this.peekUpperAt(offset) === 'OR' && this.peekUpperAt(offset + 1) === 'REPLACE') {
      offset += 2;
    }

    const first = this.peekUpperAt(offset);
    const second = this.peekUpperAt(offset + 1);

    if (CREATE_KEYWORD_NORMALIZED_TYPES.has(first)) return first;
    if (first === 'SCHEMA') return 'SCHEMA';
    if (first === 'STAGE') return 'STAGE';
    if (first === 'FILE' && second === 'FORMAT') return 'FILE FORMAT';
    return null;
  }

  private hasCreateSchemaEmbeddedDdl(maxLookahead: number = 1000): boolean {
    if (this.peekUpper() !== 'CREATE' || this.peekUpperAt(1) !== 'SCHEMA') return false;

    let depth = 0;
    for (let i = 2; i <= maxLookahead; i++) {
      const token = this.peekAt(i);
      if (token.type === 'eof' || token.value === ';') break;
      if (this.isOpenGroupToken(token)) {
        depth++;
        continue;
      }
      if (this.isCloseGroupToken(token)) {
        depth = Math.max(0, depth - 1);
        continue;
      }
      if (depth === 0 && token.upper === 'CREATE') {
        return true;
      }
    }
    return false;
  }

  private looksLikeAlterRoutineStatement(): boolean {
    return this.getAlterRoutineKind() !== null;
  }

  private looksLikeAlterViewWithQueryStatement(maxLookahead: number = 1000): boolean {
    if (this.peekUpper() !== 'ALTER' || this.peekUpperAt(1) !== 'VIEW') return false;

    let depth = 0;
    for (let i = 2; i <= maxLookahead; i++) {
      const token = this.peekAt(i);
      if (token.type === 'eof' || token.value === ';') break;
      if (this.isOpenGroupToken(token)) {
        depth++;
        continue;
      }
      if (this.isCloseGroupToken(token)) {
        depth = Math.max(0, depth - 1);
        continue;
      }
      if (depth === 0 && token.upper === 'AS') {
        return true;
      }
    }
    return false;
  }

  private getAlterRoutineKind(): 'PROCEDURE' | 'FUNCTION' | 'TRIGGER' | 'EVENT' | null {
    if (this.peekUpper() !== 'ALTER') return null;
    for (let i = 1; i < 40; i++) {
      const token = this.peekAt(i);
      if (token.type === 'eof' || token.value === ';') break;
      if (token.upper === 'TABLE' || token.upper === 'INDEX' || token.upper === 'VIEW' || token.upper === 'DATABASE') {
        return null;
      }
      if (
        token.upper === 'PROCEDURE'
        || token.upper === 'PROC'
        || token.upper === 'FUNCTION'
        || token.upper === 'TRIGGER'
        || token.upper === 'EVENT'
      ) {
        if (token.upper === 'PROC') return 'PROCEDURE';
        return token.upper as 'PROCEDURE' | 'FUNCTION' | 'TRIGGER' | 'EVENT';
      }
    }
    return null;
  }

  private hasKeywordAhead(keyword: string, maxLookahead: number = 500): boolean {
    for (let i = 1; i <= maxLookahead; i++) {
      const token = this.peekAt(i);
      if (token.type === 'eof') break;
      if (token.upper === keyword) return true;
    }
    return false;
  }

  private peekUpperSkippingComments(offset: number = 0): string {
    let lookahead = this.pos + offset;
    while (lookahead < this.tokens.length) {
      const token = this.tokens[lookahead];
      if (token.type === 'line_comment' || token.type === 'block_comment') {
        lookahead++;
        continue;
      }
      return token.upper;
    }
    return '';
  }

  private isControlFlowEndQualifier(kw: string): boolean {
    if (kw === 'IF') {
      // Avoid swallowing a new top-level IF statement after END.
      // Treat END IF as a control-flow qualifier only when IF is followed
      // by a terminator (or EOF), as in "END IF;".
      const next = this.peekUpperSkippingComments(1);
      return next === ';' || next === '' || next === this.activeDelimiter;
    }
    return kw === 'CASE' || kw === 'LOOP' || kw === 'WHILE' || kw === 'REPEAT';
  }

  private parseStatementUntilEndBlock(
    comments: AST.CommentNode[],
    reason: AST.RawReason,
    options: { normalizeKeywords?: boolean; allowPreBeginSemicolons?: boolean; allowElseChain?: boolean } = {},
  ): AST.RawExpression | null {
    const startPos = this.peek().position;
    const startIndex = this.pos;
    let endPos = startPos;
    let beginDepth = 0;
    let sawBegin = false;

    while (!this.isAtEnd()) {
      const token = this.advance();
      endPos = token.position + token.value.length;

      if (token.upper === 'BEGIN') {
        sawBegin = true;
        beginDepth++;
        continue;
      }

      if (token.upper === 'END') {
        if (!this.isControlFlowEndQualifier(this.peekUpper()) && beginDepth > 0) {
          beginDepth--;
          if (beginDepth === 0) {
            if (options.allowElseChain && this.hasElseContinuationAhead()) {
              sawBegin = false;
              continue;
            }
            if (this.check(';')) {
              const semi = this.advance();
              endPos = semi.position + semi.value.length;
            }
            break;
          }
        }
        continue;
      }

      if (token.value === ';' && !sawBegin) {
        if (!options.allowPreBeginSemicolons) {
          break;
        }
        continue;
      }
    }

    if (options.normalizeKeywords) {
      const statementTokens = this.tokens.slice(startIndex, this.pos);
      let text = this.tokensToSqlKeywordNormalized(statementTokens);
      text = text
        .replace(/\bCREATE\s+OR\s+REPLACE\b/g, 'CREATE OR REPLACE')
        .replace(/\bCREATE\s+OR\s+ALTER\b/g, 'CREATE OR ALTER')
        .replace(/\bALTER\s+PROCEDURE\b/g, 'ALTER PROCEDURE')
        .replace(/\bALTER\s+FUNCTION\b/g, 'ALTER FUNCTION')
        .replace(/\bALTER\s+TRIGGER\b/g, 'ALTER TRIGGER');
      const raw = text
        ? ({ type: 'raw', text, reason } as AST.RawExpression)
        : null;
      return this.combineCommentsWithRaw(comments, raw, reason);
    }

    return this.buildRawFromSourceSlice(comments, startPos, endPos, reason);
  }

  private hasElseContinuationAhead(): boolean {
    let lookahead = this.pos;
    while (lookahead < this.tokens.length) {
      const token = this.tokens[lookahead];
      if (token.type === 'line_comment' || token.type === 'block_comment') {
        lookahead++;
        continue;
      }
      if (token.value === ';') {
        lookahead++;
        while (lookahead < this.tokens.length) {
          const next = this.tokens[lookahead];
          if (next.type === 'line_comment' || next.type === 'block_comment') {
            lookahead++;
            continue;
          }
          return next.upper === 'ELSE';
        }
        return false;
      }
      return token.upper === 'ELSE';
    }
    return false;
  }

  private combineCommentsWithRaw(
    comments: AST.CommentNode[],
    raw: AST.RawExpression | null,
    fallbackReason: AST.RawReason,
  ): AST.RawExpression | null {
    if (!raw) {
      if (comments.length === 0) return null;
      return this.commentsToRaw(comments);
    }
    if (comments.length === 0) return raw;
    return {
      type: 'raw',
      text: `${this.commentsToRaw(comments).text}\n${raw.text}`.trim(),
      reason: raw.reason ?? fallbackReason,
    };
  }

  private buildRawFromSourceSlice(
    comments: AST.CommentNode[],
    startPos: number,
    endPos: number,
    reason: AST.RawReason,
  ): AST.RawExpression | null {
    if (this.source === undefined) {
      return this.combineCommentsWithRaw(comments, this.parseRawStatement(reason), reason);
    }
    const text = this.source.slice(startPos, endPos).trim();
    const raw = text ? ({ type: 'raw', text, reason } as AST.RawExpression) : null;
    return this.combineCommentsWithRaw(comments, raw, reason);
  }

  private parseVerbatimStatement(
    comments: AST.CommentNode[],
    reason: AST.RawReason = 'unsupported',
    allowImplicitBoundary: boolean = false,
  ): AST.RawExpression | null {
    const raw = this.parseRawStatement(reason, { allowImplicitBoundary });
    return this.combineCommentsWithRaw(comments, raw, reason);
  }

  private parseKeywordNormalizedStatement(
    comments: AST.CommentNode[],
    reason: AST.RawReason = 'unsupported',
    allowImplicitBoundary: boolean = false,
  ): AST.RawExpression | null {
    const start = this.pos;
    let depth = 0;
    while (!this.isAtEnd() && !this.check(';')) {
      if (
        allowImplicitBoundary
        && depth === 0
        && this.pos > start
        && this.hasImplicitStatementBoundary()
      ) {
        break;
      }
      const token = this.advance();
      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }
    if (this.check(';')) {
      this.advance();
    }

    const tokens = this.tokens.slice(start, this.pos);
    if (tokens.length === 0) return this.combineCommentsWithRaw(comments, null, reason);
    const text = this.tokensToSqlKeywordNormalized(tokens).trim();
    const raw = text ? ({ type: 'raw', text, reason } as AST.RawExpression) : null;
    return this.combineCommentsWithRaw(comments, raw, reason);
  }

  private parseAlterViewWithQuery(
    comments: AST.CommentNode[],
    reason: AST.RawReason = 'unsupported',
  ): AST.RawExpression | null {
    const start = this.pos;
    let depth = 0;
    let sawAsKeyword = false;
    let expectingViewQueryStart = false;

    while (!this.isAtEnd() && !this.check(';')) {
      if (depth === 0 && this.pos > start && this.hasImplicitStatementBoundary()) {
        if (sawAsKeyword) {
          const boundaryKeyword = this.peekUpper();
          const startsViewQuery =
            expectingViewQueryStart
            && (boundaryKeyword === 'SELECT' || boundaryKeyword === 'WITH' || boundaryKeyword === 'VALUES');
          if (!startsViewQuery) break;
        }
      }

      const token = this.advance();
      if (token.upper === 'AS' && depth === 0) {
        sawAsKeyword = true;
        expectingViewQueryStart = true;
      } else if (
        expectingViewQueryStart
        && token.type !== 'whitespace'
        && token.type !== 'line_comment'
        && token.type !== 'block_comment'
      ) {
        expectingViewQueryStart = false;
      }

      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }
    if (this.check(';')) {
      this.advance();
    }

    const tokens = this.tokens.slice(start, this.pos);
    if (tokens.length === 0) return this.combineCommentsWithRaw(comments, null, reason);
    const text = this.tokensToSqlKeywordNormalized(tokens).trim();
    const raw = text ? ({ type: 'raw', text, reason } as AST.RawExpression) : null;
    return this.combineCommentsWithRaw(comments, raw, reason);
  }

  private parseSingleLineStatement(
    comments: AST.CommentNode[],
    reason: AST.RawReason = 'unsupported',
  ): AST.RawExpression | null {
    const startToken = this.peek();
    const startPos = startToken.position;
    const startLine = startToken.line;
    let endPos = startPos;

    while (!this.isAtEnd()) {
      const token = this.peek();
      if (token.type === 'eof' || token.line !== startLine) break;
      this.advance();
      endPos = token.position + token.value.length;
    }

    return this.buildRawFromSourceSlice(comments, startPos, endPos, reason);
  }

  private parseCopyStatement(comments: AST.CommentNode[]): AST.RawExpression | null {
    let lookahead = this.pos;
    let sawFrom = false;
    let fromStdin = false;
    while (lookahead < this.tokens.length) {
      const token = this.tokens[lookahead];
      if (token.type === 'eof' || token.value === ';') break;
      if (token.upper === 'FROM') {
        sawFrom = true;
      } else if (sawFrom) {
        if (token.upper === 'STDIN') {
          fromStdin = true;
          break;
        }
        sawFrom = false;
      }
      lookahead++;
    }

    if (!fromStdin) {
      return this.parseKeywordNormalizedStatement(comments, 'unsupported', true);
    }

    const startToken = this.peek();
    const startPos = startToken.position;

    sawFrom = false;
    fromStdin = false;
    let endPos = startPos;

    while (!this.isAtEnd()) {
      const token = this.advance();
      endPos = token.position + token.value.length;

      if (token.upper === 'FROM') {
        sawFrom = true;
      } else if (sawFrom) {
        fromStdin = token.upper === 'STDIN';
        sawFrom = false;
      } else {
        sawFrom = false;
      }

      if (token.value === ';') break;
    }

    if (fromStdin) {
      while (!this.isAtEnd()) {
        const token = this.peek();
        this.advance();
        endPos = token.position + token.value.length;
        if (token.type === 'line_comment' && token.value.trim() === '\\.') {
          break;
        }
      }
    }

    return this.buildRawFromSourceSlice(comments, startPos, endPos, 'unsupported');
  }

  private parseDelimiterScript(comments: AST.CommentNode[]): AST.RawExpression | null {
    const raw = this.parseSingleLineStatement(comments, 'unsupported');
    if (raw) {
      const firstLine = raw.text.split(/\r?\n/, 1)[0] ?? '';
      const match = firstLine.match(/^DELIMITER\s+(\S+)/i);
      if (match?.[1]) {
        this.activeDelimiter = match[1];
      }
    }
    return raw;
  }

  private parseCustomDelimitedStatement(comments: AST.CommentNode[]): AST.RawExpression | null {
    if (this.activeDelimiter === ';') {
      return this.parseRawStatement('unsupported', { allowImplicitBoundary: true });
    }
    if (this.source === undefined) {
      return this.parseRawStatement('unsupported', { allowImplicitBoundary: true });
    }

    const startPos = this.peek().position;
    const startSearch = Math.max(0, startPos);
    const delimPos = this.source.indexOf(this.activeDelimiter, startSearch);

    if (delimPos < 0) {
      while (!this.isAtEnd()) this.advance();
      return this.buildRawFromSourceSlice(comments, startPos, this.source.length, 'unsupported');
    }

    const endPos = delimPos + this.activeDelimiter.length;
    while (!this.isAtEnd()) {
      const token = this.peek();
      const tokenEnd = token.position + token.value.length;
      this.advance();
      if (tokenEnd >= endPos) break;
    }

    return this.buildRawFromSourceSlice(comments, startPos, endPos, 'unsupported');
  }

  private commentsToRaw(comments: AST.CommentNode[]): AST.RawExpression {
    return {
      type: 'raw',
      text: comments.map(c => c.text).join('\n'),
      reason: 'comment_only',
    };
  }

  private parseRawStatement(
    reason: AST.RawReason = 'unsupported',
    options: { allowImplicitBoundary?: boolean } = {},
  ): AST.RawExpression | null {
    const start = this.pos;
    const startPos = this.peek().position;
    let depth = 0;
    while (!this.isAtEnd() && !this.check(';')) {
      if (
        options.allowImplicitBoundary
        && depth === 0
        && this.pos > start
        && this.hasImplicitStatementBoundary()
      ) {
        break;
      }

      const token = this.advance();
      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }
    const end = this.pos;

    let sliceEndPos = startPos;
    if (end > start) {
      const last = this.tokens[end - 1];
      sliceEndPos = last.position + last.value.length;
    }
    let hasSemicolon = false;
    let appendSyntheticSemicolon = false;
    let trailingSemicolonComment: Token | undefined;
    if (this.check(';')) {
      const semi = this.advance();
      sliceEndPos = semi.position + semi.value.length;
      if (this.source !== undefined && this.source[semi.position] !== ';') {
        const synthetic = this.source[semi.position];
        // Synthetic semicolons are injected around inline meta-commands
        // (e.g. trailing "\gset"). Do not consume the source character at
        // this position, which would otherwise leak a stray backslash.
        if (synthetic === '\\') {
          sliceEndPos = semi.position;
        } else if (synthetic === '/') {
          // SQL*Plus/DB2 slash-terminated statements should round-trip with
          // an explicit SQL semicolon.
          sliceEndPos = semi.position;
          appendSyntheticSemicolon = true;
        }
      }
      hasSemicolon = true;

      if (this.peekType() === 'line_comment' && this.peek().line === semi.line) {
        trailingSemicolonComment = this.advance();
        sliceEndPos = trailingSemicolonComment.position + trailingSemicolonComment.value.length;
      }
    }

    if (this.source) {
      let text = this.source.slice(startPos, sliceEndPos).trim();
      if (appendSyntheticSemicolon && text) {
        text += ';';
      }
      if (!text) return null;
      return { type: 'raw', text, reason };
    }

    let text = '';
    for (let i = start; i < end; i++) {
      const token = this.tokens[i];
      text += token.value;
      if (i >= end - 1) continue;

      const next = this.tokens[i + 1];
      const gap = next.position - (token.position + token.value.length);
      if (gap > 0) text += ' ';
    }
    text = text.trim();
    if (hasSemicolon) {
      if (text) text += ';';
      else text = ';';
    }
    if (trailingSemicolonComment) {
      text += (text ? ' ' : '') + trailingSemicolonComment.value;
    }

    if (!text) return null;
    return { type: 'raw', text, reason };
  }

  private parseUnionOrSelect(comments: AST.CommentNode[]): AST.Node {
    const first = this.parseQueryMember();

    if (this.checkUnionKeyword()) {
      const members: { statement: AST.QueryExpression; parenthesized: boolean }[] = [
        first,
      ];
      const operators: string[] = [];

      while (this.checkUnionKeyword()) {
        const op = this.consumeUnionKeyword();
        operators.push(op);
        const memberComments = this.consumeComments();
        members.push(this.parseQueryMember(memberComments));
      }

      let orderBy: AST.OrderByClause | undefined;
      let limit: AST.LimitClause | undefined;
      let offset: AST.OffsetClause | undefined;
      let fetch: { count: AST.Expression; withTies?: boolean } | undefined;
      let lockingClause: string | undefined;

      if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
        this.advance(); this.advance();
        orderBy = { items: this.parseOrderByItems() };
      }

      const parsedLimit = this.parseLimitClause();
      if (parsedLimit.limit) limit = parsedLimit.limit;
      if (parsedLimit.offsetFromLimit) offset = parsedLimit.offsetFromLimit;

      if (this.peekUpper() === 'OFFSET') {
        this.advance();
        const offsetCount = this.parsePrimary();
        let rowsKeyword = false;
        if (this.peekUpper() === 'ROWS') {
          this.advance();
          rowsKeyword = true;
        }
        offset = { count: offsetCount, rowsKeyword };
      }

      if (!limit && this.peekUpper() === 'LIMIT') {
        const parsedLimitAfterOffset = this.parseLimitClause();
        if (parsedLimitAfterOffset.limit) limit = parsedLimitAfterOffset.limit;
        if (parsedLimitAfterOffset.offsetFromLimit && !offset) {
          offset = parsedLimitAfterOffset.offsetFromLimit;
        }
      }

      if (this.peekUpper() === 'FETCH') {
        fetch = this.parseFetchClause();
      }

      if (this.peekUpper() === 'FOR') {
        lockingClause = this.parseForClause();
      }

      return {
        type: 'union',
        members,
        operators,
        orderBy,
        limit,
        offset,
        fetch,
        lockingClause,
        leadingComments: comments,
      } as AST.UnionStatement;
    }

    let orderBy: AST.OrderByClause | undefined;
    let limit: AST.LimitClause | undefined;
    let offset: AST.OffsetClause | undefined;
    let fetch: { count: AST.Expression; withTies?: boolean } | undefined;
    let lockingClause: string | undefined;

    if (first.parenthesized) {
      if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
        this.advance(); this.advance();
        orderBy = { items: this.parseOrderByItems() };
      }

      const parsedLimit = this.parseLimitClause();
      if (parsedLimit.limit) limit = parsedLimit.limit;
      if (parsedLimit.offsetFromLimit) offset = parsedLimit.offsetFromLimit;

      if (this.peekUpper() === 'OFFSET') {
        this.advance();
        const offsetCount = this.parsePrimary();
        let rowsKeyword = false;
        if (this.peekUpper() === 'ROWS') {
          this.advance();
          rowsKeyword = true;
        }
        offset = { count: offsetCount, rowsKeyword };
      }

      if (!limit && this.peekUpper() === 'LIMIT') {
        const parsedLimitAfterOffset = this.parseLimitClause();
        if (parsedLimitAfterOffset.limit) limit = parsedLimitAfterOffset.limit;
        if (parsedLimitAfterOffset.offsetFromLimit && !offset) {
          offset = parsedLimitAfterOffset.offsetFromLimit;
        }
      }

      if (this.peekUpper() === 'FETCH') {
        fetch = this.parseFetchClause();
      }

      if (this.peekUpper() === 'FOR') {
        lockingClause = this.parseForClause();
      }
    }

    if (first.parenthesized && (orderBy || limit || offset || fetch || lockingClause)) {
      return {
        type: 'union',
        members: [first],
        operators: [],
        orderBy,
        limit,
        offset,
        fetch,
        lockingClause,
        leadingComments: comments,
      } as AST.UnionStatement;
    }

    if (first.statement.type === 'union') {
      return {
        ...first.statement,
        leadingComments: comments,
      };
    }
    if (first.statement.type !== 'select') {
      throw new ParseError('query expression', this.peek());
    }
    return {
      ...first.statement,
      parenthesized: first.parenthesized || undefined,
      leadingComments: comments,
    };
  }

  private parseLimitClause(): { limit?: AST.LimitClause; offsetFromLimit?: AST.OffsetClause } {
    if (this.peekUpper() !== 'LIMIT') return {};
    this.advance();
    const first = this.parsePrimary();
    if (this.check(',')) {
      this.advance();
      const second = this.parsePrimary();
      return {
        limit: { count: second },
        offsetFromLimit: { count: first, rowsKeyword: false },
      };
    }
    return { limit: { count: first } };
  }

  private parseQueryExpression(comments: AST.CommentNode[] = []): AST.QueryExpression {
    return this.withDepth(() => {
      if (this.peekUpper() === 'WITH') {
        return this.parseCTE(comments, { queryOnly: true });
      }
      if (this.peekUpper() === 'VALUES') {
        const values = this.parseValuesClause();
        if (comments.length === 0) return values;
        return { ...values, leadingComments: comments };
      }
      if (this.peekUpper() === 'SELECT' || this.check('(')) {
        const query = this.parseUnionOrSelect(comments);
        if (query.type === 'select' || query.type === 'union') return query;
      }
      throw new ParseError('query expression', this.peek());
    });
  }

  private tryParse<T>(fn: () => T): T | null {
    const checkpoint = this.pos;
    try {
      return fn();
    } catch (err) {
      if (err instanceof ParseError) {
        this.pos = checkpoint;
        return null;
      }
      throw err;
    }
  }

  private tryParseQueryExpressionAtCurrent(comments: AST.CommentNode[] = []): AST.QueryExpression | null {
    return this.tryParse(() => this.parseQueryExpression(comments));
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

  private parseQueryMember(leadingComments: AST.CommentNode[] = []): { statement: AST.QueryExpression; parenthesized: boolean } {
    if (this.check('(')) {
      this.advance(); // consume (
      const query = this.parseQueryExpression(leadingComments);
      this.expect(')');
      return { statement: query, parenthesized: true };
    }
    if (this.peekUpper() === 'VALUES') {
      const values = this.parseValuesClause();
      if (leadingComments.length === 0) {
        return { statement: values, parenthesized: false };
      }
      return {
        statement: {
          ...values,
          leadingComments,
        },
        parenthesized: false,
      };
    }
    const select = this.parseSelect();
    if (leadingComments.length === 0) {
      return { statement: select, parenthesized: false };
    }
    return {
      statement: {
        ...select,
        leadingComments,
      },
      parenthesized: false,
    };
  }

  private parseSelect(): AST.SelectStatement {
    this.expect('SELECT');

    let distinct = false;
    let distinctOn: AST.Expression[] | undefined;
    if (this.peekUpper() === 'DISTINCT') {
      this.advance();
      distinct = true;
      if (this.peekUpper() === 'ON') {
        this.advance();
        this.expect('(');
        distinctOn = this.parseExpressionList();
        this.expect(')');
      }
    } else if (this.peekUpper() === 'UNIQUE') {
      // Oracle synonym: SELECT UNIQUE ...
      this.advance();
      distinct = true;
    } else if (this.peekUpper() === 'ALL') {
      // Explicit ALL is semantically equivalent to the default.
      this.advance();
    }

    let top: string | undefined;
    if (this.peekUpper() === 'TOP') {
      const topTokens: Token[] = [];
      topTokens.push(this.advance()); // TOP

      if (this.check('(')) {
        let depth = 0;
        do {
          const token = this.advance();
          topTokens.push(token);
          if (token.value === '(') depth++;
          if (token.value === ')') depth--;
        } while (!this.isAtEnd() && depth > 0);
      } else {
        topTokens.push(this.advance());
      }

      if (this.peekUpper() === 'PERCENT') {
        topTokens.push(this.advance());
      }
      if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'TIES') {
        topTokens.push(this.advance());
        topTokens.push(this.advance());
      }

      top = this.tokensToSql(topTokens);
    }

    const columns = this.isSelectListBoundary() ? [] : this.parseColumnList();
    let into: string | undefined;
    if (this.peekUpper() === 'INTO') {
      this.advance();
      const intoTokens = this.collectTokensUntilTopLevelKeyword(new Set([
        'FROM', 'WHERE', 'GROUP', 'HAVING', 'WINDOW', 'ORDER', 'LIMIT',
        'OFFSET', 'FETCH', 'FOR', 'UNION', 'INTERSECT', 'EXCEPT',
      ]));
      into = this.tokensToSql(intoTokens);
      if (!into) {
        throw new ParseError('INTO target', this.peek());
      }
    }
    let from: AST.FromClause | undefined;
    let additionalFromItems: AST.FromClause[] | undefined;
    const joins: AST.JoinClause[] = [];
    let where: AST.WhereClause | undefined;
    let startWith: AST.Expression | undefined;
    let connectBy: { condition: AST.Expression; noCycle?: boolean } | undefined;
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
      const extraFromItems: AST.FromClause[] = [];
      while (this.check(',')) {
        this.advance();
        extraFromItems.push(this.parseFromItem());
      }
      if (extraFromItems.length > 0) additionalFromItems = extraFromItems;

      // Parse JOINs
      while (true) {
        this.consumeCommentsIfFollowedByJoinKeyword();
        if (!this.isJoinKeyword()) break;
        joins.push(this.parseJoin());
      }

      if (!this.hasImplicitStatementBoundary()) {
        const trailingFromComments = this.consumeComments();
        if (trailingFromComments.length > 0) {
          if (joins.length > 0) {
            const last = joins[joins.length - 1];
            if (!last.trailingComment) {
              joins[joins.length - 1] = { ...last, trailingComment: trailingFromComments[0] };
            }
          } else if (from) {
            from = {
              ...from,
              trailingComments: [
                ...(from.trailingComments ?? []),
                ...trailingFromComments,
              ],
            };
          }
        }
      }
    }

    const parseStartWithClause = (): void => {
      this.expect('START');
      this.expect('WITH');
      this.consumeComments();
      startWith = this.parseExpression();
    };

    const parseConnectByClause = (): void => {
      this.expect('CONNECT');
      this.expect('BY');
      let noCycle = false;
      if (this.peekUpper() === 'NOCYCLE') {
        this.advance();
        noCycle = true;
      }
      this.consumeComments();
      const condition = this.parseExpression();
      connectBy = { condition, noCycle: noCycle || undefined };
    };

    const parseHierarchicalClauses = (): void => {
      let advanced = true;
      while (advanced) {
        advanced = false;
        if (!startWith && this.peekUpper() === 'START' && this.peekUpperAt(1) === 'WITH') {
          parseStartWithClause();
          advanced = true;
          continue;
        }
        if (!connectBy && this.peekUpper() === 'CONNECT' && this.peekUpperAt(1) === 'BY') {
          parseConnectByClause();
          advanced = true;
        }
      }
    };

    parseHierarchicalClauses();

    if (this.peekUpper() === 'WHERE') {
      this.advance();
      this.consumeComments();
      const condition = this.parseExpression();
      let whereComment: AST.CommentNode | undefined;
      if (
        this.peekType() === 'line_comment'
        && this.peek().line === this.peekAt(-1).line
      ) {
        const t = this.advance();
        whereComment = { type: 'comment', style: 'line', text: t.value };
      }
      where = { condition, trailingComment: whereComment };
    }

    parseHierarchicalClauses();

    if (this.peekUpper() === 'GROUP' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      groupBy = this.parseGroupByClause();
    }

    if (this.peekUpper() === 'HAVING') {
      this.advance();
      this.consumeComments();
      having = { condition: this.parseExpression() };
    }

    if (this.peekUpper() === 'WINDOW') {
      windowClause = this.parseWindowClause();
    }

    if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      orderBy = { items: this.parseOrderByItems() };
    }

    const parsedLimit = this.parseLimitClause();
    if (parsedLimit.limit) limit = parsedLimit.limit;
    if (parsedLimit.offsetFromLimit) offset = parsedLimit.offsetFromLimit;

    if (this.peekUpper() === 'OFFSET') {
      this.advance();
      const offsetCount = this.parsePrimary();
      let rowsKeyword = false;
      if (this.peekUpper() === 'ROWS') {
        this.advance();
        rowsKeyword = true;
      }
      offset = { count: offsetCount, rowsKeyword };
    }

    if (!limit && this.peekUpper() === 'LIMIT') {
      const parsedLimitAfterOffset = this.parseLimitClause();
      if (parsedLimitAfterOffset.limit) limit = parsedLimitAfterOffset.limit;
      if (parsedLimitAfterOffset.offsetFromLimit && !offset) {
        offset = parsedLimitAfterOffset.offsetFromLimit;
      }
    }

    if (this.peekUpper() === 'FETCH') {
      fetch = this.parseFetchClause();
    }

    if (this.peekUpper() === 'FOR') {
      lockingClause = this.parseForClause();
    }

    return {
      type: 'select',
      distinct,
      distinctOn,
      top,
      into,
      columns,
      from,
      additionalFromItems,
      joins,
      where,
      startWith,
      connectBy,
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
  }

  private parseFromItem(): AST.FromClause {
    let lateral = false;
    if (this.peekUpper() === 'LATERAL') {
      this.advance();
      lateral = true;
    }

    const table = this.parseTableExprWithDescendants();
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

    let ordinality = false;
    if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'ORDINALITY') {
      this.advance();
      this.advance();
      ordinality = true;
    }

    const { alias, aliasColumns } = this.parseOptionalAlias({
      allowColumnList: true,
      stopKeywords: ['TABLESAMPLE', 'PIVOT', 'UNPIVOT'],
    });
    const pivotClause = this.parseOptionalPivotClause();

    return { table, alias, aliasColumns, pivotClause, lateral, ordinality, tablesample };
  }

  // Shared alias parsing for FROM items, JOINs, SELECT columns, and RETURNING.
  // opts.allowColumnList: whether to parse (col1, col2) after alias (FROM/JOIN only)
  // opts.stopKeywords: additional keywords that prevent implicit alias detection
  private parseOptionalAlias(
    opts: { allowColumnList?: boolean; stopKeywords?: string[]; allowStringLiteral?: boolean } = {}
  ): { alias?: string; aliasColumns?: string[] } {
    let alias: string | undefined;
    let aliasColumns: string[] | undefined;

    if (this.peekUpper() === 'AS') {
      this.advance();
      alias = this.advance().value;
      if (opts.allowColumnList) aliasColumns = this.tryParseAliasColumnList();
    } else if (
      (
        this.peekType() === 'identifier'
        || (this.peekType() === 'keyword' && !this.clauseKeywords.has(this.peekUpper()))
      )
      && !this.clauseKeywords.has(this.peekUpper())
      && !this.isJoinKeyword()
      && !this.check(',')
      && !this.check(')')
      && !this.check(';')
      && !(opts.stopKeywords && opts.stopKeywords.includes(this.peekUpper()))
    ) {
      alias = this.advance().value;
      if (opts.allowColumnList) aliasColumns = this.tryParseAliasColumnList();
    } else if (opts.allowStringLiteral && this.peekType() === 'string') {
      const next = this.peekAt(1);
      if (
        next.type === 'eof'
        || next.value === ','
        || next.value === ')'
        || next.value === ';'
        || (next.type === 'keyword' && this.clauseKeywords.has(next.upper))
      ) {
        alias = this.advance().value;
      }
    }

    return { alias, aliasColumns };
  }

  private parseOptionalPivotClause(): string | undefined {
    if (this.peekUpper() !== 'PIVOT' && this.peekUpper() !== 'UNPIVOT') return undefined;

    const tokens: Token[] = [];
    let depth = 0;
    const stopKeywords = new Set([
      'WHERE', 'GROUP', 'HAVING', 'WINDOW', 'ORDER', 'LIMIT', 'OFFSET', 'FETCH', 'FOR',
      'UNION', 'INTERSECT', 'EXCEPT', 'ON', 'USING',
    ]);

    while (!this.isAtEnd() && !this.check(';')) {
      const kw = this.peekUpper();
      if (depth === 0) {
        if (this.check(',')) break;
        if (stopKeywords.has(kw)) break;
        if (this.isJoinKeyword()) break;
      }

      const token = this.advance();
      tokens.push(token);
      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }

    return this.tokensToSql(tokens) || undefined;
  }

  // Parse an optional parenthesized column alias list: (col1, col2, ...)
  private tryParseAliasColumnList(): string[] | undefined {
    if (!this.check('(')) return undefined;
    this.advance();
    const cols: string[] = [];
    while (!this.check(')') && !this.isAtEnd()) {
      const columnTokens: Token[] = [];
      let depth = 0;

      while (!this.isAtEnd()) {
        if (depth === 0 && (this.check(',') || this.check(')'))) break;
        const token = this.advance();
        columnTokens.push(token);
        if (this.isOpenGroupToken(token)) depth++;
        else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
      }

      const columnDef = this.tokensToSqlPreserveCase(columnTokens);
      if (columnDef) cols.push(columnDef);

      if (this.check(',')) this.advance();
    }
    this.expect(')');
    return cols;
  }

  private parseTableExpr(): AST.Expression {
    if (this.check('(')) {
      const subquery = this.tryParseSubqueryAtCurrent();
      if (subquery) {
        return subquery;
      }

      const parenthesizedJoin = this.tryParseParenthesizedJoinExpression();
      if (parenthesizedJoin) {
        return parenthesizedJoin;
      }

      this.advance();
      const expr = this.parseExpression();
      this.expect(')');
      return { type: 'paren', expr } as AST.ParenExpr;
    }
    return this.parsePrimary();
  }

  private parseTableExprWithDescendants(): AST.Expression {
    const table = this.parseTableExpr();
    if (table.type === 'identifier' && this.check('*')) {
      this.advance();
      return { ...table, withDescendants: true };
    }
    return table;
  }

  private tryParseParenthesizedJoinExpression(): AST.Expression | null {
    const checkpoint = this.pos;
    if (!this.check('(')) return null;

    const tokens: Token[] = [];
    let depth = 0;

    do {
      const token = this.advance();
      tokens.push(token);
      if (token.value === '(') depth++;
      if (token.value === ')') depth--;
    } while (!this.isAtEnd() && depth > 0);

    if (depth !== 0) {
      this.pos = checkpoint;
      return null;
    }

    let nesting = 0;
    let sawJoinKeyword = false;
    for (let i = 1; i < tokens.length - 1; i++) {
      const token = tokens[i];
      if (this.isOpenGroupToken(token)) {
        nesting++;
        continue;
      }
      if (this.isCloseGroupToken(token)) {
        nesting = Math.max(0, nesting - 1);
        continue;
      }
      if (nesting > 0) continue;
      if (
        token.upper === 'JOIN'
        || token.upper === 'NATURAL'
        || token.upper === 'INNER'
        || token.upper === 'LEFT'
        || token.upper === 'RIGHT'
        || token.upper === 'FULL'
        || token.upper === 'CROSS'
      ) {
        sawJoinKeyword = true;
        break;
      }
    }

    if (!sawJoinKeyword) {
      this.pos = checkpoint;
      return null;
    }

    return {
      type: 'raw',
      text: this.tokensToSql(tokens),
      reason: 'verbatim',
    };
  }

  private tryParseSubqueryAtCurrent(): AST.SubqueryExpr | null {
    if (!this.check('(')) return null;
    return this.tryParse(() => {
      this.advance();
      const query = this.parseQueryExpression();
      this.expect(')');
      return { type: 'subquery', query } as AST.SubqueryExpr;
    });
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
    if (kw === 'STRAIGHT_JOIN') return true;
    if ((kw === 'CROSS' || kw === 'OUTER') && this.peekUpperAt(1) === 'APPLY') return true;
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
    if (this.peekUpper() === 'STRAIGHT_JOIN') {
      const joinType = this.advance().upper;
      const table = this.parseTableExprWithDescendants();
      let ordinality = false;
      if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'ORDINALITY') {
        this.advance();
        this.advance();
        ordinality = true;
      }
      const { alias, aliasColumns } = this.parseOptionalAlias({
        allowColumnList: true,
        stopKeywords: ['PIVOT', 'UNPIVOT'],
      });
      const pivotClause = this.parseOptionalPivotClause();

      let on: AST.Expression | undefined;
      let usingClause: string[] | undefined;
      let usingAlias: string | undefined;
      let usingAliasColumns: string[] | undefined;
      let trailingComment: AST.CommentNode | undefined;

      if (this.peekUpper() === 'ON') {
        this.advance();
        this.consumeComments();
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

      if (usingClause) {
        const usingAliasResult = this.parseOptionalAlias({ allowColumnList: true });
        usingAlias = usingAliasResult.alias;
        usingAliasColumns = usingAliasResult.aliasColumns;
      }

      if (this.peekType() === 'line_comment') {
        const t = this.advance();
        trailingComment = {
          type: 'comment',
          style: 'line',
          text: t.value,
        };
      }

      return {
        joinType,
        table,
        alias,
        aliasColumns,
        pivotClause,
        ordinality,
        on,
        usingClause,
        usingAlias,
        usingAliasColumns,
        trailingComment,
      };
    }

    if ((this.peekUpper() === 'CROSS' || this.peekUpper() === 'OUTER') && this.peekUpperAt(1) === 'APPLY') {
      const joinType = this.advance().upper + ' ' + this.advance().upper;
      const table = this.parseTableExprWithDescendants();
      const { alias, aliasColumns } = this.parseOptionalAlias({
        allowColumnList: true,
        stopKeywords: ['PIVOT', 'UNPIVOT'],
      });
      const pivotClause = this.parseOptionalPivotClause();
      return { joinType, table, alias, aliasColumns, pivotClause };
    }

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

    const table = this.parseTableExprWithDescendants();
    let ordinality = false;
    if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'ORDINALITY') {
      this.advance();
      this.advance();
      ordinality = true;
    }
    // ON and USING are already clause keywords, so no extra stop keywords needed
    const { alias, aliasColumns } = this.parseOptionalAlias({
      allowColumnList: true,
      stopKeywords: ['PIVOT', 'UNPIVOT'],
    });
    const pivotClause = this.parseOptionalPivotClause();

    let on: AST.Expression | undefined;
    let usingClause: string[] | undefined;
    let usingAlias: string | undefined;
    let usingAliasColumns: string[] | undefined;
    let trailingComment: AST.CommentNode | undefined;

    if (this.peekUpper() === 'ON') {
      this.advance();
      this.consumeComments();
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

    if (usingClause) {
      const usingAliasResult = this.parseOptionalAlias({ allowColumnList: true });
      usingAlias = usingAliasResult.alias;
      usingAliasColumns = usingAliasResult.aliasColumns;
    }

    if (this.peekType() === 'line_comment') {
      const t = this.advance();
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: t.value,
      };
    }

    return {
      joinType: joinType.trim(),
      table,
      alias,
      aliasColumns,
      pivotClause,
      lateral,
      ordinality,
      on,
      usingClause,
      usingAlias,
      usingAliasColumns,
      trailingComment,
    };
  }

  private parseGroupByClause(): AST.GroupByClause {
    let setQuantifier: 'ALL' | 'DISTINCT' | undefined;
    if (this.peekUpper() === 'ALL' || this.peekUpper() === 'DISTINCT') {
      setQuantifier = this.advance().upper as 'ALL' | 'DISTINCT';
      this.consumeComments();
    }

    const items: AST.Expression[] = [];
    const groupingSetsArr: { type: 'grouping_sets' | 'rollup' | 'cube'; sets: AST.Expression[][] }[] = [];
    let withRollup = false;

    // Check for GROUPING SETS, ROLLUP, CUBE
    const kw = this.peekUpper();
    if (kw === 'GROUPING' && this.peekUpperAt(1) === 'SETS') {
      groupingSetsArr.push(this.parseGroupingSetsSpec('grouping_sets'));
    } else if (kw === 'ROLLUP') {
      groupingSetsArr.push(this.parseGroupingSetsSpec('rollup'));
    } else if (kw === 'CUBE') {
      groupingSetsArr.push(this.parseGroupingSetsSpec('cube'));
    } else if (!this.isGroupByBoundary()) {
      // Normal GROUP BY items
      items.push(this.parseExpression());
      while (this.check(',')) {
        this.advance();
        // Check if next is GROUPING SETS, ROLLUP, or CUBE
        const nextKw = this.peekUpper();
        if ((nextKw === 'GROUPING' && this.peekUpperAt(1) === 'SETS') || nextKw === 'ROLLUP' || nextKw === 'CUBE') {
          const specType = nextKw === 'GROUPING' ? 'grouping_sets' : nextKw.toLowerCase() as 'rollup' | 'cube';
          groupingSetsArr.push(this.parseGroupingSetsSpec(specType));
        } else {
          items.push(this.parseExpression());
        }
      }
    }

    if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'ROLLUP') {
      this.advance();
      this.advance();
      withRollup = true;
    }

    return {
      setQuantifier,
      items,
      groupingSets: groupingSetsArr.length > 0 ? groupingSetsArr : undefined,
      withRollup: withRollup || undefined,
    };
  }

  private isGroupByBoundary(): boolean {
    if (this.isAtEnd() || this.check(';') || this.check(')')) return true;
    const kw = this.peekUpper();
    return (
      kw === 'HAVING'
      || kw === 'WINDOW'
      || kw === 'ORDER'
      || kw === 'LIMIT'
      || kw === 'OFFSET'
      || kw === 'FETCH'
      || kw === 'FOR'
      || kw === 'UNION'
      || kw === 'INTERSECT'
      || kw === 'EXCEPT'
      || (kw === 'WITH' && this.peekUpperAt(1) === 'ROLLUP')
    );
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
    this.expect('AS');
    this.expect('(');
    const spec = this.parseWindowSpec();
    this.expect(')');
    return { name, spec };
  }

  private parseWindowSpec(): AST.WindowSpec {
    let baseWindowName: string | undefined;
    let partitionBy: AST.Expression[] | undefined;
    let orderBy: AST.OrderByItem[] | undefined;
    let frame: AST.FrameSpec | undefined;
    let exclude: string | undefined;

    const first = this.peekUpper();
    if (
      first
      && first !== ')'
      && first !== 'PARTITION'
      && first !== 'ORDER'
      && first !== 'ROWS'
      && first !== 'RANGE'
      && first !== 'GROUPS'
    ) {
      baseWindowName = this.advance().value;
    }

    if (this.peekUpper() === 'PARTITION' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      partitionBy = this.parseExpressionList();
    }

    if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
      this.advance(); this.advance();
      orderBy = this.parseOrderByItems();
    }

    // Frame clause
    if (this.peekUpper() === 'ROWS' || this.peekUpper() === 'RANGE' || this.peekUpper() === 'GROUPS') {
      const unit = this.advance().upper as AST.FrameSpec['unit'];
      if (this.peekUpper() === 'BETWEEN') {
        this.advance();
        const start = this.parseFrameBound();
        this.expect('AND');
        const end = this.parseFrameBound();
        frame = { unit, start, end };
      } else {
        frame = { unit, start: this.parseFrameBound() };
      }

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

    return { baseWindowName, partitionBy, orderBy, frame, exclude };
  }

  private parseFrameBound(): AST.FrameBound {
    if (this.peekUpper() === 'UNBOUNDED') {
      this.advance();
      if (this.peekUpper() === 'PRECEDING') {
        this.advance();
        return { kind: 'UNBOUNDED PRECEDING' };
      }
      if (this.peekUpper() === 'FOLLOWING') {
        this.advance();
        return { kind: 'UNBOUNDED FOLLOWING' };
      }
      throw new ParseError('PRECEDING or FOLLOWING', this.peek());
    }
    if (this.peekUpper() === 'CURRENT' && this.peekUpperAt(1) === 'ROW') {
      this.advance();
      this.advance();
      return { kind: 'CURRENT ROW' };
    }

    const value = this.parseExpression();
    if (this.peekUpper() === 'PRECEDING') {
      this.advance();
      return { kind: 'PRECEDING', value };
    }
    if (this.peekUpper() === 'FOLLOWING') {
      this.advance();
      return { kind: 'FOLLOWING', value };
    }
    throw new ParseError('PRECEDING or FOLLOWING', this.peek());
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
    this.expect('FOR');

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
    this.consumeComments();
    items.push(this.parseOrderByItem());
    while (this.check(',')) {
      this.advance();
      if (
        this.peekType() === 'line_comment'
        && !items[items.length - 1].trailingComment
        && this.pos > 0
        && this.peek().line === this.tokens[this.pos - 1].line
      ) {
        const t = this.advance();
        const last = items[items.length - 1];
        items[items.length - 1] = { ...last, trailingComment: { type: 'comment', style: 'line', text: t.value } };
      }
      this.consumeComments();
      items.push(this.parseOrderByItem());
    }
    return items;
  }

  private parseOrderByItem(): AST.OrderByItem {
    const expr = this.parseExpression();
    let usingOperator: string | undefined;
    if (this.peekUpper() === 'USING') {
      this.advance();
      usingOperator = this.parseOrderByUsingOperator();
    }
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
    let trailingComment: AST.CommentNode | undefined;
    if (
      this.peekType() === 'line_comment'
      && this.pos > 0
      && this.peek().line === this.tokens[this.pos - 1].line
    ) {
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: this.advance().value,
      };
    }
    return { expr, usingOperator, direction, nulls, trailingComment };
  }

  private parseOrderByUsingOperator(): string {
    if (this.peekUpper() === 'OPERATOR' && this.peekAt(1).value === '(') {
      const tokens: Token[] = [];
      tokens.push(this.advance()); // OPERATOR
      const openParen = this.expect('(');
      tokens.push(openParen);
      let depth = 1;
      while (!this.isAtEnd() && depth > 0) {
        const token = this.advance();
        tokens.push(token);
        if (token.value === '(') depth++;
        else if (token.value === ')') depth--;
      }
      if (depth !== 0) {
        throw new ParseError(')', this.peek());
      }
      return this.tokensToSqlPreserveCase(tokens);
    }

    let operator = '';
    while (this.peek().type === 'operator') {
      operator += this.advance().value;
    }
    if (!operator) {
      throw new ParseError('operator', this.peek());
    }
    return operator;
  }

  // Expression parser using precedence climbing
  private parseExpression(): AST.Expression {
    return this.withDepth(() => this.parseOr());
  }

  private parseOr(): AST.Expression {
    let left = this.parseAnd();
    while (true) {
      const preCommentStart = this.pos;
      const preCommentCount = this.consumeCommentsIfFollowedByKeyword('OR');
      if (this.peekUpper() !== 'OR') break;
      this.advance();
      const postCommentStart = this.pos;
      const comments = this.consumeComments();
      const parsedRight = this.parseAnd();
      const rightStart = preCommentCount > 0 ? preCommentStart : postCommentStart;
      const right = (preCommentCount > 0 || comments.length > 0)
        ? this.rawExpressionFromTokenRange(rightStart, this.pos)
        : parsedRight;
      left = { type: 'binary', left, operator: 'OR', right };
    }
    return left;
  }

  private parseAnd(): AST.Expression {
    let left = this.parseNot();
    // BETWEEN consumes its own AND token at comparison precedence, so plain AND
    // here always means boolean conjunction. MySQL also accepts && as AND.
    while (true) {
      const preCommentStart = this.pos;
      const preCommentCount = this.consumeCommentsIfFollowedByKeyword('AND');
      let operator: 'AND' | '&&' | null = null;
      if (this.peekUpper() === 'AND') {
        this.advance();
        operator = 'AND';
      } else if (this.peek().type === 'operator' && this.peek().value === '&&') {
        this.advance();
        operator = '&&';
      }
      if (!operator) break;
      const postCommentStart = this.pos;
      const comments = this.consumeComments();
      const parsedRight = this.parseNot();
      const rightStart = preCommentCount > 0 ? preCommentStart : postCommentStart;
      const right = (preCommentCount > 0 || comments.length > 0)
        ? this.rawExpressionFromTokenRange(rightStart, this.pos)
        : parsedRight;
      left = { type: 'binary', left, operator, right };
    }
    return left;
  }

  private parseNot(): AST.Expression {
    if (this.peekUpper() === 'NOT') {
      this.advance();

      if (
        this.peekUpper() === 'IN'
        || this.peekUpper() === 'LIKE'
        || this.peekUpper() === 'ILIKE'
        || this.peekUpper() === 'RLIKE'
        || this.peekUpper() === 'REGEXP'
        || this.peekUpper() === 'BETWEEN'
        || this.peekUpper() === 'EXISTS'
        || this.peekUpper() === 'SIMILAR'
      ) {
        this.pos--;
        return this.parseComparison();
      }

      const operand = this.parseNot();
      return { type: 'unary', operator: 'NOT', operand };
    }
    return this.parseComparison();
  }

  private parseComparison(): AST.Expression {
    return parseComparisonExpression({
      parseAddSub: () => this.parseAddSub(),
      peekUpper: () => this.peekUpper(),
      peekUpperAt: (offset: number) => this.peekUpperAt(offset),
      advance: () => this.advance(),
      expect: (value: string) => this.expect(value),
      tryParseQueryExpressionAtCurrent: () => this.tryParseQueryExpressionAtCurrent(),
      parseExpressionList: () => this.parseExpressionList(),
      isRegexOperator: () => this.isRegexOperator(),
      checkComparisonOperator: () => this.checkComparisonOperator(),
      getPos: () => this.pos,
      setPos: (pos: number) => { this.pos = pos; },
    });
  }

  private isRegexOperator(): boolean {
    const v = this.peek().value;
    return v === '~' || v === '~*' || v === '!~' || v === '!~*';
  }

  private checkComparisonOperator(): boolean {
    const t = this.peek();
    if (t.type === 'operator') {
      return ['=', '<>', '!=', '<', '>', '<=', '>=', '&&'].includes(t.value);
    }
    return false;
  }

  private parseAddSub(): AST.Expression {
    let left = this.parseMulDiv();
    while (this.peek().type === 'operator' && (this.peek().value === '+' || this.peek().value === '-')) {
      const op = this.advance().value;
      const rightStart = this.pos;
      const comments = this.consumeComments();
      const parsedRight = this.parseMulDiv();
      const right = comments.length > 0
        ? this.rawExpressionFromTokenRange(rightStart, this.pos)
        : parsedRight;
      left = { type: 'binary', left, operator: op, right };
    }
    return left;
  }

  private parseMulDiv(): AST.Expression {
    let left = this.parseJsonOps();
    while (this.peek().type === 'operator' && (this.peek().value === '*' || this.peek().value === '/' || this.peek().value === '%' || this.peek().value === '||')) {
      const op = this.advance().value;
      const rightStart = this.pos;
      const comments = this.consumeComments();
      const parsedRight = this.parseJsonOps();
      const right = comments.length > 0
        ? this.rawExpressionFromTokenRange(rightStart, this.pos)
        : parsedRight;
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
        const rightStart = this.pos;
        const comments = this.consumeComments();
        const parsedRight = this.parseUnaryExpr();
        const right = comments.length > 0
          ? this.rawExpressionFromTokenRange(rightStart, this.pos)
          : parsedRight;
        left = { type: 'binary', left, operator: v, right };
        continue;
      }

      // Array / JSONB containment
      if (v === '@>' || v === '<@') {
        this.advance();
        const rightStart = this.pos;
        const comments = this.consumeComments();
        const parsedRight = this.parseUnaryExpr();
        const right = comments.length > 0
          ? this.rawExpressionFromTokenRange(rightStart, this.pos)
          : parsedRight;
        left = { type: 'binary', left, operator: v, right };
        continue;
      }

      // JSONB existence/path operators
      if (v === '?' || v === '?|' || v === '?&' || v === '@?' || v === '@@') {
        this.advance();
        const rightStart = this.pos;
        const comments = this.consumeComments();
        const parsedRight = this.parseUnaryExpr();
        const right = comments.length > 0
          ? this.rawExpressionFromTokenRange(rightStart, this.pos)
          : parsedRight;
        left = { type: 'binary', left, operator: v, right };
        continue;
      }

      // Bitwise operators
      if (v === '&' || v === '|' || v === '#' || v === '<<' || v === '>>') {
        this.advance();
        const rightStart = this.pos;
        const comments = this.consumeComments();
        const parsedRight = this.parseUnaryExpr();
        const right = comments.length > 0
          ? this.rawExpressionFromTokenRange(rightStart, this.pos)
          : parsedRight;
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
    if (this.peekUpper() === 'PRIOR') {
      this.advance();
      const operand = this.parsePrimary();
      return { type: 'unary', operator: 'PRIOR', operand };
    }
    return this.parsePrimaryWithPostfix();
  }

  // Parse primary then handle postfix :: cast
  private parsePrimaryWithPostfix(): AST.Expression {
    let expr = this.parsePrimary();

    while (true) {
      // Handle :: casts (can be chained)
      if (this.peek().type === 'operator' && this.peek().value === '::') {
        this.advance();
        const targetType = this.consumeTypeSpecifier();
        expr = { type: 'pg_cast', expr, targetType } as AST.PgCastExpr;
        continue;
      }

      // Handle array subscript: expr[idx] or expr[lo:hi]
      if (this.check('[')) {
        this.advance(); // consume [
        if (this.check(':')) {
          this.advance();
          const upper = this.check(']') ? undefined : this.parseArraySubscriptBoundExpression();
          this.expect(']');
          expr = {
            type: 'array_subscript',
            array: expr,
            isSlice: true,
            upper,
          } as AST.ArraySubscriptExpr;
          continue;
        }

        const lower = this.parseArraySubscriptBoundExpression();
        if (this.check(':')) {
          this.advance();
          const upper = this.check(']') ? undefined : this.parseArraySubscriptBoundExpression();
          this.expect(']');
          expr = {
            type: 'array_subscript',
            array: expr,
            isSlice: true,
            lower,
            upper,
          } as AST.ArraySubscriptExpr;
          continue;
        }
        this.expect(']');
        expr = {
          type: 'array_subscript',
          array: expr,
          isSlice: false,
          lower,
        } as AST.ArraySubscriptExpr;
        continue;
      }

      // PostgreSQL collation operator: <expr> COLLATE <collation_name>
      if (this.peekUpper() === 'COLLATE') {
        this.advance();
        const collation = this.parseQualifiedName();
        expr = { type: 'collate', expr, collation } as AST.CollateExpr;
        continue;
      }

      // SQL-standard / PostgreSQL / T-SQL postfix: <expr> AT TIME ZONE <zone>
      if (this.peekUpper() === 'AT' && this.peekUpperAt(1) === 'TIME' && this.peekUpperAt(2) === 'ZONE') {
        this.advance();
        this.advance();
        this.advance();
        const zone = this.parsePrimaryWithPostfix();
        expr = { type: 'binary', left: expr, operator: 'AT TIME ZONE', right: zone };
        continue;
      }

      // Snowflake VARIANT path access: value:key[:nested]
      if (!this.parsingArraySubscriptBounds && this.check(':')) {
        this.advance();
        const segment = this.parseVariantPathSegment();
        expr = { type: 'binary', left: expr, operator: ':', right: segment };
        continue;
      }

      break;
    }

    return expr;
  }

  private parseVariantPathSegment(): AST.Expression {
    const token = this.peek();
    if (
      token.type === 'identifier'
      || token.type === 'keyword'
      || token.type === 'string'
      || token.type === 'number'
      || token.type === 'parameter'
    ) {
      let text = this.advance().value;
      while (this.check('.')) {
        this.advance();
        text += '.' + this.advance().value;
      }
      return { type: 'raw', text, reason: 'verbatim' };
    }

    // Keep parser moving for less-common path fragments while preserving text.
    return { type: 'raw', text: this.advance().value, reason: 'unsupported' };
  }

  private parseArraySubscriptBoundExpression(): AST.Expression {
    const previous = this.parsingArraySubscriptBounds;
    this.parsingArraySubscriptBounds = true;
    try {
      return this.parseExpression();
    } finally {
      this.parsingArraySubscriptBounds = previous;
    }
  }

  private parseQualifiedName(): string {
    const first = this.peek();
    if (
      first.type !== 'identifier'
      && first.type !== 'keyword'
      && first.type !== 'string'
    ) {
      throw new ParseError('identifier', first);
    }
    let name = this.advance().value;
    while (this.check('.')) {
      this.advance();
      const next = this.peek();
      if (next.type !== 'identifier' && next.type !== 'keyword') {
        throw new ParseError('identifier', next);
      }
      name += '.' + this.advance().value;
    }
    return name;
  }

  private parsePrimary(): AST.Expression {
    return parsePrimaryExpression({
      peek: () => this.peek(),
      peekUpperAt: (offset: number) => this.peekUpperAt(offset),
      peekAt: (offset: number) => this.peekAt(offset),
      peekTypeAt: (offset: number) => this.peekTypeAt(offset),
      advance: () => this.advance(),
      check: (value: string) => this.check(value),
      expect: (value: string) => this.expect(value),
      getPos: () => this.pos,
      setPos: (pos: number) => { this.pos = pos; },
      tokensToSql: (tokens: Token[]) => this.tokensToSqlPreserveCase(tokens),
      parseSubquery: () => this.parseSubquery(),
      tryParseSubqueryAtCurrent: () => this.tryParseSubqueryAtCurrent(),
      parseExpression: () => this.parseExpression(),
      parseAddSub: () => this.parseAddSub(),
      parseCaseExpr: () => this.parseCaseExpr(),
      parseCast: () => this.parseCast(),
      parseExtract: () => this.parseExtract(),
      parsePositionExpr: () => this.parsePositionExpr(),
      parseSubstringExpr: () => this.parseSubstringExpr(),
      parseOverlayExpr: () => this.parseOverlayExpr(),
      parseTrimExpr: () => this.parseTrimExpr(),
      parseIdentifierOrFunction: () => this.parseIdentifierOrFunction(),
      consumeComments: () => this.consumeComments(),
    });
  }

  private parseIdentifierOrFunction(): AST.Expression {
    const name = this.advance();
    let fullName = name.value;
    let lastNameToken = name;
    const quoted = isQuotedIdentifierToken(name.value);

    // Qualified name: a.b or a.*
    while (this.check('.')) {
      this.advance(); // consume .
      if (this.check('*')) {
        this.advance();
        return { type: 'star', qualifier: fullName };
      }
      const next = this.advance();
      fullName += '.' + next.value;
      lastNameToken = next;
    }

    // MySQL charset introducers: _utf8'text', _latin1'text', ...
    if (
      /^_[A-Za-z][A-Za-z0-9_]*$/.test(fullName)
      && this.peekType() === 'string'
      && this.peek().position === lastNameToken.position + lastNameToken.value.length
    ) {
      const textLiteral = this.advance().value;
      return {
        type: 'raw',
        text: `${fullName}${textLiteral}`,
        reason: 'verbatim',
      };
    }

    // Function call
    const canParseAsFunction =
      !this.clauseKeywords.has(name.upper) || FUNCTION_KEYWORD_OVERRIDES.has(name.upper);
    if (this.check('(') && canParseAsFunction) {
      this.advance(); // consume (
      const upperName = fullName.toUpperCase();

      let distinct = false;
      if (this.peekUpper() === 'DISTINCT') {
        this.advance();
        distinct = true;
      }

      const args: AST.Expression[] = [];
      if (CAST_STYLE_FUNCTIONS.has(upperName)) {
        const bodyTokens = this.consumeTokensUntilTopLevelFunctionClose();
        if (bodyTokens.length > 0) {
          args.push({
            type: 'raw',
            text: this.tokensToSqlPreserveCase(bodyTokens),
            reason: 'verbatim',
          });
        }
      } else {
      const parseFunctionBodyAsRaw =
        upperName === 'JSON_TABLE'
        || upperName === 'XMLELEMENT'
        || (upperName === 'ARRAY' && (this.peekUpper() === 'SELECT' || this.peekUpper() === 'WITH'))
        || (upperName === 'TRANSLATE' && this.hasTopLevelKeywordBeforeFunctionClose('USING'));
      if (parseFunctionBodyAsRaw) {
        const bodyTokens = this.consumeTokensUntilTopLevelFunctionClose();
        if (bodyTokens.length > 0) {
          args.push({
            type: 'raw',
            text: this.tokensToSqlPreserveCase(bodyTokens),
            reason: 'verbatim',
          });
        }
      } else if (!this.check(')')) {
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
      }

      let separator: AST.Expression | undefined;
      if (upperName === 'GROUP_CONCAT' && this.peekUpper() === 'SEPARATOR') {
        this.advance();
        separator = this.parseExpression();
      }

      // Handle ORDER BY inside aggregate: e.g., ARRAY_AGG(x ORDER BY y), STRING_AGG(x, ',' ORDER BY y)
      let innerOrderBy: AST.OrderByItem[] | undefined;
      if (this.peekUpper() === 'ORDER' && this.peekUpperAt(1) === 'BY') {
        this.advance(); this.advance();
        innerOrderBy = this.parseOrderByItems();
      }

      if (upperName === 'GROUP_CONCAT' && this.peekUpper() === 'SEPARATOR') {
        this.advance();
        separator = this.parseExpression();
      }

      this.expect(')');

      // FILTER (WHERE ...)
      let filter: AST.Expression | undefined;
      if (this.peekUpper() === 'FILTER') {
        this.advance();
        this.expect('(');
        this.expect('WHERE');
        filter = this.parseExpression();
        this.expect(')');
      }

      // WITHIN GROUP (ORDER BY ...)
      let withinGroup: { orderBy: AST.OrderByItem[] } | undefined;
      if (this.peekUpper() === 'WITHIN') {
        this.advance();
        this.expect('GROUP');
        this.expect('(');
        this.expect('ORDER');
        this.expect('BY');
        const withinOrderBy = this.parseOrderByItems();
        this.expect(')');
        withinGroup = { orderBy: withinOrderBy };
      }

      const funcExpr: AST.FunctionCallExpr = {
        type: 'function_call',
        name: fullName,
        args,
        separator,
        distinct,
        orderBy: innerOrderBy,
        filter,
        withinGroup,
      };

      let nullTreatment: AST.WindowFunctionExpr['nullTreatment'];
      if (
        (this.peekUpper() === 'IGNORE' || this.peekUpper() === 'RESPECT')
        && this.peekUpperAt(1) === 'NULLS'
      ) {
        const modifier = this.advance().upper as 'IGNORE' | 'RESPECT';
        this.expect('NULLS');
        nullTreatment = `${modifier} NULLS`;
      }

      // Window function: OVER (...)
      if (this.peekUpper() === 'OVER') {
        return this.parseWindowFunction(funcExpr, nullTreatment);
      }
      if (nullTreatment) {
        throw new ParseError('OVER', this.peek());
      }

      return funcExpr;
    }

    return { type: 'identifier', value: fullName, quoted };
  }

  private parseWindowFunction(
    func: AST.FunctionCallExpr,
    nullTreatment?: AST.WindowFunctionExpr['nullTreatment'],
  ): AST.WindowFunctionExpr {
    this.expect('OVER');

    // Check for named window reference: OVER window_name
    if (!this.check('(')) {
      const windowName = this.advance().value;
      return { type: 'window_function', func, windowName, nullTreatment };
    }

    this.expect('(');
    const spec = this.parseWindowSpec();
    this.expect(')');

    return {
      type: 'window_function',
      func,
      nullTreatment,
      windowName: spec.baseWindowName,
      partitionBy: spec.partitionBy,
      orderBy: spec.orderBy,
      frame: spec.frame,
      exclude: spec.exclude,
    };
  }

  private parseCaseExpr(): AST.CaseExpr {
    this.expect('CASE');

    this.consumeComments();
    let operand: AST.Expression | undefined;
    if (this.peekUpper() !== 'WHEN') {
      operand = this.parseExpression();
      this.consumeComments();
    }

    const whenClauses: { condition: AST.Expression; result: AST.Expression; trailingComment?: string }[] = [];
    while (true) {
      this.consumeComments();
      if (this.peekUpper() !== 'WHEN') break;
      this.advance();
      const condition = this.parseExpression();
      this.expect('THEN');
      this.consumeComments();
      const result = this.parseExpression();
      let trailingComment: string | undefined;
      if (
        this.peekType() === 'line_comment'
        && this.pos > 0
        && this.peek().line === this.tokens[this.pos - 1].line
      ) {
        trailingComment = this.advance().value;
      }
      this.consumeComments();
      whenClauses.push({ condition, result, trailingComment });
    }

    let elseResult: AST.Expression | undefined;
    if (this.peekUpper() === 'ELSE') {
      this.advance();
      this.consumeComments();
      elseResult = this.parseExpression();
      this.consumeComments();
    }

    this.expect('END');
    return { type: 'case', operand, whenClauses, elseResult };
  }

  private parseCast(): AST.CastExpr {
    this.expect('CAST');
    this.expect('(');
    const expr = this.parseExpression();
    this.expect('AS');
    const targetType = this.consumeTypeSpecifier();
    this.expect(')');
    return { type: 'cast', expr, targetType };
  }

  private parseExtract(): AST.ExtractExpr {
    this.expect('EXTRACT');
    this.expect('(');
    const field = this.advance().upper;
    this.expect('FROM');
    const source = this.parseExpression();
    this.expect(')');
    return { type: 'extract', field, source };
  }

  private parsePositionExpr(): AST.Expression {
    this.advance(); // POSITION
    this.expect('(');
    const substr = this.parseAddSub();
    this.expect('IN');
    const str = this.parseAddSub();
    this.expect(')');
    return { type: 'position', substring: substr, source: str } as AST.PositionExpr;
  }

  private parseSubstringExpr(): AST.Expression {
    this.advance(); // SUBSTRING
    this.expect('(');
    const str = this.parseExpression();
    let start: AST.Expression;
    let len: AST.Expression | undefined;
    let style: 'from_for' | 'comma' = 'from_for';

    if (this.peekUpper() === 'FROM') {
      this.advance();
      start = this.parseExpression();
      if (this.peekUpper() === 'FOR') {
        this.advance();
        len = this.parseExpression();
      }
    } else if (this.peekUpper() === 'FOR') {
      // SQL-standard shorthand: SUBSTRING(expr FOR len) implies start at 1.
      start = { type: 'literal', value: '1', literalType: 'number' };
      this.advance();
      len = this.parseExpression();
    } else if (this.check(',')) {
      this.advance();
      style = 'comma';
      start = this.parseExpression();
      if (this.check(',')) {
        this.advance();
        len = this.parseExpression();
      }
    } else {
      throw new ParseError('FROM or ,', this.peek());
    }

    this.expect(')');
    return { type: 'substring', source: str, start, length: len, style } as AST.SubstringExpr;
  }

  private parseOverlayExpr(): AST.Expression {
    this.advance(); // OVERLAY
    this.expect('(');
    const str = this.parseExpression();
    this.expect('PLACING');
    const replacement = this.parseExpression();
    this.expect('FROM');
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
    while (true) {
      const exprStart = this.pos;
      const leadingComments = this.consumeComments();
      const startForRaw = leadingComments.length > 0 ? exprStart : this.pos;
      const expr = this.parseExpression();
      const trailingComments = this.consumeComments();
      const hasComments = leadingComments.length > 0 || trailingComments.length > 0;
      list.push(hasComments ? this.rawExpressionFromTokenRange(startForRaw, this.pos) : expr);
      if (!this.check(',')) break;
      this.advance();
    }
    this.consumeComments();
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
    const { alias } = this.parseOptionalAlias();
    if (alias) {
      return { type: 'aliased', expr, alias } as AST.AliasedExpr;
    }
    return expr;
  }

  private parseColumnList(): AST.ColumnExpr[] {
    const columns: AST.ColumnExpr[] = [];
    let leadingComments = this.consumeComments();
    if (this.isSelectListBoundary()) {
      throw new ParseError('select expression', this.peek());
    }
    columns.push(this.parseColumnExpr(leadingComments));

    while (this.check(',')) {
      const commaToken = this.advance();
      if (
        this.peekType() === 'line_comment'
        && this.peek().line === commaToken.line
        && !columns[columns.length - 1].trailingComment
      ) {
        const t = this.advance();
        const last = columns[columns.length - 1];
        columns[columns.length - 1] = {
          ...last,
          trailingComment: { type: 'comment', style: 'line', text: t.value },
        };
      }
      leadingComments = this.consumeComments();
      if (this.isSelectListBoundary()) {
        if (leadingComments.length > 0 && !columns[columns.length - 1].trailingComment) {
          const last = columns[columns.length - 1];
          columns[columns.length - 1] = { ...last, trailingComment: leadingComments[0] };
        }
        break;
      }
      columns.push(this.parseColumnExpr(leadingComments));
    }

    return columns;
  }

  private parseColumnExpr(leadingComments: AST.CommentNode[] = []): AST.ColumnExpr {
    let expr = this.parseExpression();
    let trailingComment: AST.CommentNode | undefined;

    let { alias } = this.parseOptionalAlias({ allowStringLiteral: true });

    // MySQL allows string-literal aliases without AS: SELECT col 'alias' ...
    if (
      !alias
      && expr.type === 'typed_string'
      && expr.value.startsWith("'")
      && !/\s/.test(expr.dataType)
      && !STRING_ALIAS_TYPED_LITERAL_DISALLOWED.has(expr.dataType.toUpperCase())
    ) {
      alias = expr.value;
      expr = {
        type: 'identifier',
        value: expr.dataType,
        quoted: isQuotedIdentifierToken(expr.dataType),
      };
    }

    if (this.peekType() === 'line_comment') {
      trailingComment = {
        type: 'comment',
        style: 'line',
        text: this.advance().value,
      };
    }

    return {
      expr,
      alias,
      leadingComments: leadingComments.length > 0 ? leadingComments : undefined,
      trailingComment,
    };
  }

  private isSelectListBoundary(): boolean {
    if (this.isAtEnd() || this.check(';') || this.check(')')) return true;
    const kw = this.peekUpper();
    return (
      kw === 'FROM'
      || kw === 'INTO'
      || kw === 'WHERE'
      || kw === 'GROUP'
      || kw === 'HAVING'
      || kw === 'WINDOW'
      || kw === 'ORDER'
      || kw === 'LIMIT'
      || kw === 'OFFSET'
      || kw === 'FETCH'
      || kw === 'FOR'
      || kw === 'UNION'
      || kw === 'INTERSECT'
      || kw === 'EXCEPT'
    );
  }

  private parseExplain(comments: AST.CommentNode[]): AST.ExplainStatement {
    this.expect('EXPLAIN');

    let planFor = false;
    let analyze = false;
    let verbose = false;
    let costs: boolean | undefined;
    let buffers: boolean | undefined;
    let timing: boolean | undefined;
    let summary: boolean | undefined;
    let settings: boolean | undefined;
    let wal: boolean | undefined;
    let format: AST.ExplainStatement['format'];

    const parseExplainBoolean = (): boolean => {
      const value = this.peekUpper();
      if (value === 'TRUE' || value === 'ON') {
        this.advance();
        return true;
      }
      if (value === 'FALSE' || value === 'OFF') {
        this.advance();
        return false;
      }
      throw new ParseError('TRUE/FALSE or ON/OFF', this.peek());
    };

    // Parse optional boolean value for EXPLAIN options
    // If no value is provided (next token is comma or closing paren), defaults to TRUE
    const parseOptionalExplainBoolean = (): boolean => {
      const next = this.peekUpper();
      // If next token is comma, closing paren, or end of input, default to TRUE
      if (next === ',' || next === ')' || this.isAtEnd()) {
        return true;
      }
      // If next token is TRUE/FALSE or ON/OFF, parse it
      if (next === 'TRUE' || next === 'ON' || next === 'FALSE' || next === 'OFF') {
        return parseExplainBoolean();
      }
      // Otherwise (e.g., next token is SELECT, WITH, etc.), default to TRUE
      return true;
    };

    const parseOption = (): boolean => {
      const kw = this.peekUpper();
      if (kw === 'ANALYZE') {
        this.advance();
        const value = parseOptionalExplainBoolean();
        if (value) analyze = true;
        return true;
      }
      if (kw === 'VERBOSE') {
        this.advance();
        const value = parseOptionalExplainBoolean();
        if (value) verbose = true;
        return true;
      }
      if (kw === 'COSTS') {
        this.advance();
        costs = parseOptionalExplainBoolean();
        return true;
      }
      if (kw === 'BUFFERS') {
        this.advance();
        buffers = parseOptionalExplainBoolean();
        return true;
      }
      if (kw === 'TIMING') {
        this.advance();
        timing = parseOptionalExplainBoolean();
        return true;
      }
      if (kw === 'SUMMARY') {
        this.advance();
        summary = parseOptionalExplainBoolean();
        return true;
      }
      if (kw === 'SETTINGS') {
        this.advance();
        settings = parseOptionalExplainBoolean();
        return true;
      }
      if (kw === 'WAL') {
        this.advance();
        wal = parseOptionalExplainBoolean();
        return true;
      }
      if (kw === 'FORMAT') {
        this.advance();
        const fmt = this.advance().upper;
        if (fmt !== 'TEXT' && fmt !== 'XML' && fmt !== 'JSON' && fmt !== 'YAML') {
          throw new ParseError('TEXT, XML, JSON, or YAML', this.peekAt(-1));
        }
        format = fmt;
        return true;
      }
      return false;
    };

    if (this.check('(')) {
      this.advance();
      while (!this.check(')') && !this.isAtEnd()) {
        if (!parseOption()) {
          throw new ParseError('EXPLAIN option', this.peek());
        }
        if (this.check(',')) this.advance();
      }
      this.expect(')');
    }

    while (parseOption()) {
      if (this.check(',')) this.advance();
    }

    if (this.peekUpper() === 'PLAN' && this.peekUpperAt(1) === 'FOR') {
      this.advance();
      this.advance();
      planFor = true;
    }

    const statementComments = this.consumeComments();
    let statement: AST.ExplainStatement['statement'];
    const queryStatement = this.tryParseQueryExpressionAtCurrent(statementComments);
    if (queryStatement) {
      statement = queryStatement;
    } else if (this.peekUpper() === 'INSERT') {
      statement = this.parseInsert(statementComments);
    } else if (this.peekUpper() === 'UPDATE') {
      statement = this.parseUpdate(statementComments);
    } else if (this.peekUpper() === 'DELETE') {
      statement = this.parseDelete(statementComments);
    } else {
      throw new ParseError('query expression, INSERT, UPDATE, or DELETE', this.peek());
    }

    return {
      type: 'explain',
      planFor: planFor || undefined,
      analyze,
      verbose,
      costs,
      buffers,
      timing,
      summary,
      settings,
      wal,
      format,
      statement,
      leadingComments: comments,
    };
  }

  private createDmlContext(): DmlParser {
    return {
      expect: (value: string) => this.expect(value),
      advance: () => this.advance(),
      check: (value: string) => this.check(value),
      peekUpper: () => this.peekUpper(),
      peekUpperAt: (offset: number) => this.peekUpperAt(offset),
      isAtEnd: () => this.isAtEnd(),
      isJoinKeyword: () => this.isJoinKeyword(),
      parseJoin: () => this.parseJoin(),
      parseExpression: () => this.parseExpression(),
      parseExpressionList: () => this.parseExpressionList(),
      parseReturningList: () => this.parseReturningList(),
      parseFromItem: () => this.parseFromItem(),
      parseQueryExpression: () => this.parseQueryExpression(),
      tryParseQueryExpressionAtCurrent: () => this.tryParseQueryExpressionAtCurrent(),
      consumeComments: () => this.consumeComments(),
    };
  }

  private createDdlContext(): DdlParser {
    return {
      expect: (value: string) => this.expect(value),
      advance: () => this.advance(),
      check: (value: string) => this.check(value),
      peek: () => this.peek(),
      peekUpper: () => this.peekUpper(),
      peekUpperAt: (offset: number) => this.peekUpperAt(offset),
      peekType: () => this.peekType(),
      isAtEnd: () => this.isAtEnd(),
      getPos: () => this.pos,
      setPos: (pos: number) => { this.pos = pos; },
      parseError: (expected: string, token: Token) => new ParseError(expected, token),
      consumeIfNotExists: () => this.consumeIfNotExists(),
      consumeIfExists: () => this.consumeIfExists(),
      parseRawStatement: (reason: AST.RawReason) => this.parseRawStatement(reason),
      parseTableElements: () => this.parseTableElements(),
      parseExpression: () => this.parseExpression(),
      parseStatement: () => this.parseStatement(),
      collectTokensUntilTopLevelKeyword: (stopKeywords: Set<string>) =>
        this.collectTokensUntilTopLevelKeyword(stopKeywords),
      tokensToSql: (tokens: Token[]) => this.tokensToSql(tokens),
      tokensToSqlPreserveCase: (tokens: Token[]) => this.tokensToSqlPreserveCase(tokens),
      consumeTokensUntilActionBoundary: () => this.consumeTokensUntilActionBoundary(),
      hasImplicitStatementBoundary: () => this.hasImplicitStatementBoundary(),
    };
  }

  // INSERT INTO table (cols) VALUES (...), (...) | SELECT ...
  private parseInsert(comments: AST.CommentNode[]): AST.InsertStatement {
    return parseInsertStatement(this.createDmlContext(), comments);
  }

  // UPDATE table SET col = val, ... [FROM ...] WHERE ...
  private parseUpdate(comments: AST.CommentNode[]): AST.UpdateStatement {
    return parseUpdateStatement(this.createDmlContext(), comments);
  }

  private parseSetItem(): AST.SetItem {
    return parseDmlSetItem(this.createDmlContext());
  }

  // DELETE FROM table WHERE ...
  private parseDelete(comments: AST.CommentNode[]): AST.DeleteStatement {
    return parseDeleteStatement(this.createDmlContext(), comments);
  }

  // CREATE TABLE | CREATE INDEX | CREATE VIEW
  private parseCreate(comments: AST.CommentNode[]): AST.Node {
    const stmt = parseCreateStatement(this.createDdlContext(), comments);
    if (stmt.type !== 'raw' || comments.length === 0) return stmt;
    const commentPrefix = this.commentsToRaw(comments).text;
    if (!commentPrefix || stmt.text.startsWith(commentPrefix)) return stmt;
    return {
      ...stmt,
      text: `${commentPrefix}\n${stmt.text}`.trim(),
    };
  }

  private parseMerge(comments: AST.CommentNode[]): AST.MergeStatement {
    this.advance(); // MERGE
    this.expect('INTO');
    const targetTable = this.advance().value;
    let targetAlias: string | undefined;
    if (this.peekUpper() === 'AS') {
      this.advance();
      targetAlias = this.advance().value;
    } else if (this.peekType() === 'identifier' && this.peekUpper() !== 'USING') {
      targetAlias = this.advance().value;
    }

    this.consumeComments();
    this.expect('USING');
    let sourceTable: string | AST.Expression;
    if (this.check('(')) {
      const subquery = this.tryParseSubqueryAtCurrent();
      if (!subquery) {
        throw new ParseError('subquery in MERGE USING', this.peek());
      }
      sourceTable = subquery;
    } else {
      sourceTable = this.advance().value;
    }
    let sourceAlias: string | undefined;
    if (this.peekUpper() === 'AS') {
      this.advance();
      sourceAlias = this.advance().value;
    } else if (this.peekType() === 'identifier' && this.peekUpper() !== 'ON') {
      sourceAlias = this.advance().value;
    }

    this.consumeComments();
    this.expect('ON');
    const onExpr = this.parseExpression();

    const whenClauses: AST.MergeWhenClause[] = [];
    while (this.peekUpper() === 'WHEN') {
      this.advance(); // WHEN

      let matched = true;
      if (this.peekUpper() === 'NOT') {
        this.advance();
        matched = false;
      }
      this.expect('MATCHED');

      let condition: AST.Expression | undefined;
      if (this.peekUpper() === 'AND') {
        this.advance();
        condition = this.parseExpression();
      }

      this.expect('THEN');

      const actionKw = this.peekUpper();
      if (actionKw === 'DELETE') {
        this.advance();
        whenClauses.push({ matched, condition, action: 'delete' });
      } else if (actionKw === 'UPDATE') {
        this.advance();
        this.expect('SET');
        const setItems: AST.SetItem[] = [];
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
        this.expect('VALUES');
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
    const kind = this.advance().upper as 'GRANT' | 'REVOKE';
    let grantOptionFor = false;
    if (
      kind === 'REVOKE'
      && this.peekUpper() === 'GRANT'
      && this.peekUpperAt(1) === 'OPTION'
      && this.peekUpperAt(2) === 'FOR'
    ) {
      this.advance();
      this.advance();
      this.advance();
      grantOptionFor = true;
    }

    const recipientKeyword = kind === 'GRANT' ? 'TO' : 'FROM';
    const privilegeTokens = this.collectTokensUntilTopLevelKeyword(new Set(['ON', recipientKeyword]));

    const trailingClauseKeywords = new Set(['WITH', 'GRANTED', 'CASCADE', 'RESTRICT']);
    let objectTokens: Token[] = [];
    let hasExplicitRecipients = false;
    if (this.peekUpper() === 'ON') {
      this.advance();
      objectTokens = this.collectTokensUntilTopLevelKeyword(new Set([recipientKeyword, ...trailingClauseKeywords]));
      if (this.peekUpper() === recipientKeyword) {
        this.advance();
        hasExplicitRecipients = true;
      }
    } else if (this.peekUpper() === recipientKeyword) {
      this.advance();
      hasExplicitRecipients = true;
    } else {
      hasExplicitRecipients = false;
    }

    const recipientTokens: Token[] = [];
    if (hasExplicitRecipients) {
      while (!this.isAtEnd() && !this.check(';')) {
        if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'GRANT' && this.peekUpperAt(2) === 'OPTION') break;
        if (this.peekUpper() === 'GRANTED' && this.peekUpperAt(1) === 'BY') break;
        if (this.peekUpper() === 'CASCADE' || this.peekUpper() === 'RESTRICT') break;
        recipientTokens.push(this.advance());
      }
    }

    let withGrantOption = false;
    let grantedBy: string | undefined;
    let cascade = false;
    let restrict = false;
    while (!this.isAtEnd() && !this.check(';')) {
      if (this.peekUpper() === 'WITH' && this.peekUpperAt(1) === 'GRANT' && this.peekUpperAt(2) === 'OPTION') {
        this.advance();
        this.advance();
        this.advance();
        withGrantOption = true;
        continue;
      }
      if (this.peekUpper() === 'GRANTED' && this.peekUpperAt(1) === 'BY') {
        this.advance();
        this.advance();
        const grantedByTokens = this.collectTokensUntilTopLevelKeyword(new Set(['CASCADE', 'RESTRICT']));
        grantedBy = this.tokensToSql(grantedByTokens);
        continue;
      }
      if (this.peekUpper() === 'CASCADE') {
        this.advance();
        cascade = true;
        continue;
      }
      if (this.peekUpper() === 'RESTRICT') {
        this.advance();
        restrict = true;
        continue;
      }
      break;
    }

    const privileges = this.splitTopLevelByComma(privilegeTokens).map(toks => this.tokensToSql(toks)).filter(Boolean);
    const object = this.tokensToSql(objectTokens) || undefined;
    const recipients = this.splitTopLevelByComma(recipientTokens).map(toks => this.tokensToSql(toks)).filter(Boolean);

    return {
      type: 'grant',
      kind,
      grantOptionFor: grantOptionFor || undefined,
      privileges,
      object,
      recipientKeyword: recipientKeyword as 'TO' | 'FROM',
      recipients,
      withGrantOption: withGrantOption || undefined,
      grantedBy,
      cascade: cascade || undefined,
      restrict: restrict || undefined,
      leadingComments: comments,
    };
  }

  private parseTruncate(comments: AST.CommentNode[]): AST.TruncateStatement {
    this.advance(); // TRUNCATE
    let tableKeyword = false;
    if (this.peekUpper() === 'TABLE') {
      this.advance();
      tableKeyword = true;
    }

    const tables: string[] = [];
    tables.push(this.parseQualifiedName());
    while (this.check(',')) {
      this.advance();
      tables.push(this.parseQualifiedName());
    }

    let restartIdentity = false;
    let cascade = false;

    if (this.peekUpper() === 'RESTART') {
      this.advance();
      this.expect('IDENTITY');
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

  private parseTableElements(): { elements: AST.TableElement[]; trailingComma: boolean } {
    const elements: AST.TableElement[] = [];
    let trailingComma = false;

    while (!this.check(')') && !this.isAtEnd()) {
      const comments = this.consumeComments();
      if (comments.length > 0) {
        for (const comment of comments) {
          elements.push({
            elementType: 'comment',
            raw: comment.text,
          });
        }
        if (this.check(')')) break;
      }

      if (this.check(',')) {
        this.advance();
        if (this.check(')')) {
          trailingComma = true;
          break;
        }
        continue;
      }

      const elem = this.parseTableElement();
      let elementWithComment = elem;
      if (
        this.peekType() === 'line_comment'
        && this.pos > 0
        && this.peek().line === this.tokens[this.pos - 1].line
      ) {
        const trailing = this.advance();
        elementWithComment = {
          ...elem,
          trailingComment: trailing.value,
        };
      }
      elements.push(elementWithComment);
      if (this.check(',')) {
        this.advance();
        if (this.check(')')) {
          trailingComma = true;
          break;
        }
      }
    }

    return { elements, trailingComma };
  }

  private parseTableElement(): AST.TableElement {
    if (this.peekUpper() === 'PRIMARY' && this.peekUpperAt(1) === 'KEY') {
      return this.parsePrimaryKeyTableElement();
    }

    if (this.peekUpper() === 'FOREIGN' && this.peekUpperAt(1) === 'KEY') {
      return this.parseForeignKeyTableElement();
    }

    if (this.peekUpper() === 'UNIQUE') {
      return this.parseUniqueTableElement();
    }

    if (this.looksLikeKeyLikeTableElement()) {
      return this.parseKeyLikeTableElement();
    }

    if (this.peekUpper() === 'EXCLUDE') {
      return this.parseExcludeTableElement();
    }

    if (this.peekUpper() === 'CHECK') {
      this.advance();
      this.expect('(');
      const checkExpr = this.parseExpression();
      this.expect(')');
      return {
        elementType: 'constraint',
        raw: 'CHECK',
        constraintBody: 'CHECK',
        constraintType: 'check',
        checkExpr,
      };
    }

    if (this.peekUpper() === 'CONSTRAINT') {
      this.advance();
      const constraintName = this.advance().value;

      if (this.peekUpper() === 'CHECK') {
        this.advance();
        this.expect('(');
        const checkExpr = this.parseExpression();
        this.expect(')');
        const body = 'CHECK';
        return {
          elementType: 'constraint',
          raw: `CONSTRAINT ${constraintName} ${body}`,
          constraintName,
          constraintBody: body,
          constraintType: 'check',
          checkExpr,
        };
      }

      if (this.peekUpper() === 'FOREIGN' && this.peekUpperAt(1) === 'KEY') {
        return this.parseForeignKeyTableElement(constraintName);
      }

      if (this.peekUpper() === 'UNIQUE') {
        return this.parseUniqueTableElement(constraintName);
      }

      if (this.peekUpper() === 'PRIMARY' && this.peekUpperAt(1) === 'KEY') {
        return this.parsePrimaryKeyTableElement(constraintName);
      }

      let raw = `CONSTRAINT ${constraintName}`;
      while (!this.check(',') && !this.check(')') && !this.isAtEnd()) {
        raw += ' ' + this.advance().value;
      }
      return { elementType: 'constraint', raw, constraintName };
    }

    const colName = this.advance().value;
    if (this.check(',') || this.check(')')) {
      return {
        elementType: 'column',
        raw: colName,
        name: colName,
      };
    }
    const dataType = this.consumeTypeSpecifier();
    const constraintsStart = this.pos;
    const columnConstraints = this.parseColumnConstraints();
    const constraintTokens = this.tokens.slice(constraintsStart, this.pos);
    const constraints = constraintTokens.length > 0 ? this.tokensToSql(constraintTokens) : undefined;

    return {
      elementType: 'column',
      raw: `${colName} ${dataType}${constraints ? ' ' + constraints : ''}`,
      name: colName,
      dataType,
      constraints,
      columnConstraints: columnConstraints.length > 0 ? columnConstraints : undefined,
    };
  }

  private looksLikeKeyLikeTableElement(): boolean {
    const starter = this.peekUpper();
    if (starter === 'FULLTEXT' || starter === 'SPATIAL') {
      const next = this.peekUpperAt(1);
      if (next === 'KEY' || next === 'INDEX') return true;

      const nextToken = this.peekAt(1);
      if (!nextToken || nextToken.type === 'eof') return false;
      if (nextToken.value === '(') return true;

      const nextNext = this.peekAt(2);
      return nextNext?.value === '(';
    }
    if (starter !== 'KEY' && starter !== 'INDEX') return false;

    const next = this.peekAt(1);
    if (!next || next.type === 'eof') return false;
    if (next.value === '(') return true;
    if (next.type === 'keyword') {
      return next.upper === 'USING';
    }

    const nextNext = this.peekAt(2);
    return nextNext?.value === '(';
  }

  private parsePrimaryKeyTableElement(constraintName?: string): AST.TableElement {
    this.expect('PRIMARY');
    this.expect('KEY');

    const keyDetailTokens: Token[] = [];
    while (!this.check('(') && !this.isAtEnd()) {
      if (this.peekType() !== 'identifier' && this.peekType() !== 'keyword') break;
      keyDetailTokens.push(this.advance());
    }
    const keyDetails = this.tokensToSql(keyDetailTokens);

    const columns = this.parseTableElementColumnList();
    const tail = this.normalizeTableElementTailText(this.consumeTableElementTailText());
    const cols = columns.length > 0 ? ` (${columns.join(', ')})` : '';
    const body = `PRIMARY KEY${keyDetails ? ' ' + keyDetails : ''}${cols}${tail ? ' ' + tail : ''}`;

    if (constraintName) {
      return {
        elementType: 'constraint',
        raw: `CONSTRAINT ${constraintName} ${body}`,
        constraintName,
        constraintBody: body,
        constraintType: 'raw',
      };
    }

    return {
      elementType: 'primary_key',
      raw: body,
      name: columns.join(', '),
    };
  }

  private parseUniqueTableElement(constraintName?: string): AST.TableElement {
    this.expect('UNIQUE');

    let keyKeyword: string | undefined;
    let keyName: string | undefined;
    if (this.peekUpper() === 'KEY' || this.peekUpper() === 'INDEX') {
      keyKeyword = this.advance().upper;
      if (!this.check('(') && (this.peekType() === 'identifier' || this.peekType() === 'keyword')) {
        keyName = this.advance().value;
      }
    }

    const columns = this.parseTableElementColumnList();
    const tail = this.normalizeTableElementTailText(this.consumeTableElementTailText());
    const cols = columns.length > 0 ? ` (${columns.join(', ')})` : '';
    const body = `UNIQUE${keyKeyword ? ' ' + keyKeyword : ''}${keyName ? ' ' + keyName : ''}${cols}${tail ? ' ' + tail : ''}`;

    if (constraintName) {
      return {
        elementType: 'constraint',
        raw: `CONSTRAINT ${constraintName} ${body}`,
        constraintName,
        constraintBody: body,
        constraintType: 'raw',
      };
    }

    return {
      elementType: 'constraint',
      raw: body,
      constraintBody: body,
      constraintType: 'raw',
    };
  }

  private parseKeyLikeTableElement(constraintName?: string): AST.TableElement {
    const prefixParts: string[] = [];
    if (this.peekUpper() === 'FULLTEXT' || this.peekUpper() === 'SPATIAL') {
      prefixParts.push(this.advance().upper);
    }

    if (this.peekUpper() === 'KEY' || this.peekUpper() === 'INDEX') {
      prefixParts.push(this.advance().upper);
    } else if (prefixParts.length === 0) {
      throw new ParseError('KEY or INDEX', this.peek());
    }

    let keyName: string | undefined;
    if (!this.check('(') && (this.peekType() === 'identifier' || this.peekType() === 'keyword')) {
      keyName = this.advance().value;
    }

    const columns = this.parseTableElementColumnList();
    const tail = this.normalizeTableElementTailText(this.consumeTableElementTailText());
    const cols = columns.length > 0 ? ` (${columns.join(', ')})` : '';
    const body = `${prefixParts.join(' ')}${keyName ? ' ' + keyName : ''}${cols}${tail ? ' ' + tail : ''}`;

    if (constraintName) {
      return {
        elementType: 'constraint',
        raw: `CONSTRAINT ${constraintName} ${body}`,
        constraintName,
        constraintBody: body,
        constraintType: 'raw',
      };
    }

    return {
      elementType: 'constraint',
      raw: body,
      constraintBody: body,
      constraintType: 'raw',
    };
  }

  private parseExcludeTableElement(): AST.TableElement {
    const tokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd()) {
      if (depth === 0 && (this.check(',') || this.check(')'))) break;
      const token = this.advance();
      tokens.push(token);
      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }
    const body = this.tokensToSql(tokens);
    return {
      elementType: 'constraint',
      raw: body,
      constraintBody: body,
      constraintType: 'raw',
    };
  }

  private parseForeignKeyTableElement(constraintName?: string): AST.TableElement {
    this.expect('FOREIGN');
    this.expect('KEY');

    const fkCols = this.parseTableElementColumnList();
    this.expect('REFERENCES');
    const refTable = this.parseQualifiedName();
    const refCols = this.parseTableElementColumnList();

    const actions: string[] = [];
    if (this.peekUpper() === 'MATCH') {
      this.advance();
      const matchType = this.peekUpper();
      if (matchType === 'SIMPLE' || matchType === 'FULL' || matchType === 'PARTIAL') {
        actions.push(`MATCH ${this.advance().upper}`);
      } else {
        throw new ParseError('SIMPLE, FULL, or PARTIAL', this.peek());
      }
    }
    while (this.peekUpper() === 'ON' && (this.peekUpperAt(1) === 'DELETE' || this.peekUpperAt(1) === 'UPDATE')) {
      this.advance(); // ON
      const event = this.advance().upper;
      const action = this.parseReferentialActionText();
      actions.push(`ON ${event} ${action}`);
    }
    const actionText = actions.join('\n        ');

    const prefix = constraintName ? `CONSTRAINT ${constraintName} ` : '';
    const refColText = refCols.length > 0 ? ` (${refCols.join(', ')})` : '';
    const raw = `${prefix}FOREIGN KEY (${fkCols.join(', ')}) REFERENCES ${refTable}${refColText}${actionText ? ' ' + actionText : ''}`;
    return {
      elementType: 'foreign_key',
      raw,
      constraintName,
      fkColumns: fkCols.join(', '),
      fkRefTable: refTable,
      fkRefColumns: refCols.length > 0 ? refCols.join(', ') : undefined,
      fkActions: actionText || undefined,
    };
  }

  private parseTableElementColumnList(): string[] {
    const cols: string[] = [];
    if (!this.check('(')) return cols;
    this.advance();
    while (!this.check(')') && !this.isAtEnd()) {
      const itemTokens: Token[] = [];
      let depth = 0;
      while (!this.isAtEnd()) {
        if (depth === 0 && (this.check(',') || this.check(')'))) break;
        const token = this.advance();
        itemTokens.push(token);
        if (this.isOpenGroupToken(token)) depth++;
        else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
      }
      const item = this.tokensToSqlPreserveCase(itemTokens);
      if (item) cols.push(item);
      if (this.check(',')) this.advance();
    }
    this.expect(')');
    return cols;
  }

  private consumeTableElementTailText(): string {
    const tailTokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd()) {
      if (depth === 0 && (this.check(',') || this.check(')'))) break;
      const token = this.advance();
      tailTokens.push(token);
      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }
    return this.tokensToSql(tailTokens);
  }

  private normalizeTableElementTailText(text: string): string {
    if (!text) return text;
    return text.replace(/\bWITH\(/g, 'WITH (');
  }

  private parseReferentialActionText(): string {
    if (this.peekUpper() === 'NO' && this.peekUpperAt(1) === 'ACTION') {
      this.advance();
      this.advance();
      return 'NO ACTION';
    }
    if (this.peekUpper() === 'SET' && this.peekUpperAt(1) === 'NULL') {
      this.advance();
      this.advance();
      return 'SET NULL';
    }
    if (this.peekUpper() === 'SET' && this.peekUpperAt(1) === 'DEFAULT') {
      this.advance();
      this.advance();
      return 'SET DEFAULT';
    }
    if (this.peekUpper() === 'RESTRICT' || this.peekUpper() === 'CASCADE') {
      return this.advance().upper;
    }
    throw new ParseError('referential action', this.peek());
  }

  private parseColumnConstraints(): AST.ColumnConstraint[] {
    const constraints: AST.ColumnConstraint[] = [];
    while (!this.check(',') && !this.check(')') && !this.isAtEnd()) {
      if (this.peekType() === 'line_comment') {
        break;
      }
      if (this.peekType() === 'block_comment') {
        const inlineWithPreviousToken =
          this.pos > 0
          && this.peek().line === this.tokens[this.pos - 1].line;
        if (!inlineWithPreviousToken) {
          break;
        }
        constraints.push({
          type: 'raw',
          text: this.advance().value,
        });
        continue;
      }
      let name: string | undefined;
      if (this.peekUpper() === 'CONSTRAINT') {
        this.advance();
        name = this.advance().value;
      }

      const kw = this.peekUpper();
      if (kw === 'NOT' && this.peekUpperAt(1) === 'NULL') {
        this.advance();
        this.advance();
        constraints.push({ type: 'not_null', name });
        continue;
      }
      if (kw === 'NULL') {
        this.advance();
        constraints.push({ type: 'null', name });
        continue;
      }
      if (kw === 'DEFAULT') {
        this.advance();
        if (this.peekUpper() === 'NEXT' && this.peekUpperAt(1) === 'VALUE' && this.peekUpperAt(2) === 'FOR') {
          this.advance();
          this.advance();
          this.advance();
          const seqParts: string[] = [];
          if (
            this.peekType() === 'identifier'
            || this.peekType() === 'keyword'
            || this.peekType() === 'string'
          ) {
            seqParts.push(this.advance().value);
            while (this.check('.')) {
              this.advance();
              seqParts.push(this.advance().value);
            }
          } else {
            throw new ParseError('sequence name after NEXT VALUE FOR', this.peek());
          }
          constraints.push({
            type: 'default',
            name,
            expr: {
              type: 'raw',
              text: `NEXT VALUE FOR ${seqParts.join('.')}`,
              reason: 'verbatim',
            },
          });
        } else {
          constraints.push({ type: 'default', name, expr: this.parseExpression() });
        }
        continue;
      }
      if (kw === 'CHECK') {
        this.advance();
        this.expect('(');
        const expr = this.parseExpression();
        this.expect(')');
        constraints.push({ type: 'check', name, expr });
        continue;
      }
      if (kw === 'REFERENCES') {
        constraints.push(this.parseReferencesConstraint(name));
        continue;
      }
      if (kw === 'GENERATED') {
        if (this.peekUpperAt(1) === 'ALWAYS' && this.peekUpperAt(2) === 'AS' && this.peekAt(3).value === '(') {
          const rawTokens: Token[] = [];
          if (name) {
            rawTokens.push({
              type: 'keyword',
              value: 'CONSTRAINT',
              upper: 'CONSTRAINT',
              position: -1,
              line: 0,
              column: 0,
            });
            rawTokens.push({
              type: 'identifier',
              value: name,
              upper: name.toUpperCase(),
              position: -1,
              line: 0,
              column: 0,
            });
          }
          rawTokens.push(this.advance());
          rawTokens.push(...this.consumeTokensUntilColumnConstraintBoundary());
          constraints.push({
            type: 'raw',
            name,
            text: this.tokensToSql(rawTokens),
          });
        } else {
          constraints.push(this.parseGeneratedIdentityConstraint(name));
        }
        continue;
      }
      if (kw === 'IDENTITY') {
        const identityTokens: Token[] = [this.advance()];
        if (this.check('(')) {
          let depth = 0;
          do {
            const token = this.advance();
            identityTokens.push(token);
            if (token.value === '(') depth++;
            if (token.value === ')') depth--;
          } while (!this.isAtEnd() && depth > 0);
        }
        constraints.push({
          type: 'raw',
          name,
          text: this.tokensToSql(identityTokens),
        });
        continue;
      }
      if (kw === 'PRIMARY' && this.peekUpperAt(1) === 'KEY') {
        this.advance();
        this.advance();
        constraints.push({ type: 'primary_key', name });
        continue;
      }
      if (kw === 'UNIQUE') {
        this.advance();
        constraints.push({ type: 'unique', name });
        continue;
      }

      const rawTokens: Token[] = [];
      if (name) {
        rawTokens.push({
          type: 'keyword',
          value: 'CONSTRAINT',
          upper: 'CONSTRAINT',
          position: -1,
          line: 0,
          column: 0,
        });
        rawTokens.push({
          type: 'identifier',
          value: name,
          upper: name.toUpperCase(),
          position: -1,
          line: 0,
          column: 0,
        });
      }
      rawTokens.push(...this.consumeTokensUntilColumnConstraintBoundary());
      constraints.push({
        type: 'raw',
        name,
        text: this.tokensToSql(rawTokens),
      });
    }
    return constraints;
  }

  private parseReferencesConstraint(name?: string): AST.ColumnConstraintReferences {
    this.expect('REFERENCES');
    const table = this.parseQualifiedName();
    let columns: string[] | undefined;
    if (this.check('(')) {
      this.advance();
      columns = [];
      while (!this.check(')') && !this.isAtEnd()) {
        columns.push(this.advance().value);
        if (this.check(',')) this.advance();
      }
      this.expect(')');
    }

    const actions: AST.ReferentialAction[] = [];
    let matchType: AST.ColumnConstraintReferences['matchType'];
    let deferrable: AST.ColumnConstraintReferences['deferrable'];
    let initially: AST.ColumnConstraintReferences['initially'];

    while (!this.check(',') && !this.check(')') && !this.isAtEnd()) {
      if (this.peekUpper() === 'MATCH') {
        this.advance();
        if (this.peekUpper() === 'SIMPLE' || this.peekUpper() === 'FULL' || this.peekUpper() === 'PARTIAL') {
          matchType = this.advance().upper as AST.ColumnConstraintReferences['matchType'];
          continue;
        }
        throw new ParseError('SIMPLE, FULL, or PARTIAL', this.peek());
      }
      if (this.peekUpper() === 'ON' && (this.peekUpperAt(1) === 'DELETE' || this.peekUpperAt(1) === 'UPDATE')) {
        this.advance();
        const event = this.advance().upper as AST.ReferentialAction['event'];
        let action: AST.ReferentialAction['action'];
        if (this.peekUpper() === 'NO' && this.peekUpperAt(1) === 'ACTION') {
          this.advance();
          this.advance();
          action = 'NO ACTION';
        } else if (this.peekUpper() === 'SET' && this.peekUpperAt(1) === 'NULL') {
          this.advance();
          this.advance();
          action = 'SET NULL';
        } else if (this.peekUpper() === 'SET' && this.peekUpperAt(1) === 'DEFAULT') {
          this.advance();
          this.advance();
          action = 'SET DEFAULT';
        } else if (this.peekUpper() === 'RESTRICT' || this.peekUpper() === 'CASCADE') {
          action = this.advance().upper as AST.ReferentialAction['action'];
        } else {
          throw new ParseError('referential action', this.peek());
        }
        actions.push({ event, action });
        continue;
      }
      if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'DEFERRABLE') {
        this.advance();
        this.advance();
        deferrable = 'NOT DEFERRABLE';
        continue;
      }
      if (this.peekUpper() === 'DEFERRABLE') {
        this.advance();
        deferrable = 'DEFERRABLE';
        continue;
      }
      if (this.peekUpper() === 'INITIALLY') {
        this.advance();
        if (this.peekUpper() === 'DEFERRED') {
          this.advance();
          initially = 'DEFERRED';
        } else if (this.peekUpper() === 'IMMEDIATE') {
          this.advance();
          initially = 'IMMEDIATE';
        } else {
          throw new ParseError('DEFERRED or IMMEDIATE', this.peek());
        }
        continue;
      }
      break;
    }

    return {
      type: 'references',
      name,
      table,
      columns,
      matchType,
      actions: actions.length > 0 ? actions : undefined,
      deferrable,
      initially,
    };
  }

  private parseGeneratedIdentityConstraint(name?: string): AST.ColumnConstraintGeneratedIdentity {
    this.expect('GENERATED');
    let always = true;
    let onNull = false;
    if (this.peekUpper() === 'ALWAYS') {
      this.advance();
      always = true;
    } else if (this.peekUpper() === 'BY' && this.peekUpperAt(1) === 'DEFAULT') {
      this.advance();
      this.advance();
      always = false;
      if (this.peekUpper() === 'ON' && this.peekUpperAt(1) === 'NULL') {
        this.advance();
        this.advance();
        onNull = true;
      }
    } else {
      throw new ParseError('ALWAYS or BY DEFAULT', this.peek());
    }
    this.expect('AS');
    this.expect('IDENTITY');

    let options: string | undefined;
    if (this.check('(')) {
      const tokens: Token[] = [];
      let depth = 0;
      do {
        const t = this.advance();
        tokens.push(t);
        if (t.value === '(') depth++;
        if (t.value === ')') depth--;
      } while (!this.isAtEnd() && depth > 0);
      options = this.tokensToSql(tokens);
    }

    return { type: 'generated_identity', name, always, onNull: onNull || undefined, options };
  }

  private consumeTokensUntilColumnConstraintBoundary(): Token[] {
    const tokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd()) {
      if (depth === 0 && (this.check(',') || this.check(')') || this.isAtColumnConstraintBoundary())) break;
      const t = this.advance();
      tokens.push(t);
      if (this.isOpenGroupToken(t)) depth++;
      else if (this.isCloseGroupToken(t)) depth = Math.max(0, depth - 1);
    }
    return tokens;
  }

  private isAtColumnConstraintBoundary(): boolean {
    const kw = this.peekUpper();
    if (kw === 'CONSTRAINT' || kw === 'DEFAULT' || kw === 'CHECK' || kw === 'REFERENCES' || kw === 'GENERATED' || kw === 'IDENTITY' || kw === 'UNIQUE' || kw === 'NULL') {
      return true;
    }
    if (kw === 'NOT' && this.peekUpperAt(1) === 'NULL') return true;
    if (kw === 'PRIMARY' && this.peekUpperAt(1) === 'KEY') return true;
    return false;
  }


  // ALTER TABLE
  private parseAlter(comments: AST.CommentNode[]): AST.AlterTableStatement {
    return parseAlterStatement(this.createDdlContext(), comments);
  }

  // DROP object [CONCURRENTLY] [IF EXISTS] name [CASCADE|RESTRICT]
  private parseDrop(comments: AST.CommentNode[]): AST.DropTableStatement {
    return parseDropStatement(this.createDdlContext(), comments);
  }

  // CTE: WITH [RECURSIVE] name AS (...), name AS (...) <main statement>
  private parseCTE(
    comments: AST.CommentNode[],
    options: { queryOnly?: boolean } = {}
  ): AST.CTEStatement {
    this.expect('WITH');

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

    let search: AST.CTESearchClause | undefined;
    let cycle: AST.CTECycleClause | undefined;
    if (this.peekUpper() === 'SEARCH') {
      search = this.parseCTESearchClause();
    }
    if (this.peekUpper() === 'CYCLE') {
      cycle = this.parseCTECycleClause();
    }

    // Parse the main statement after WITH
    let mainQuery: AST.CTEStatement['mainQuery'];
    const mainComments = this.consumeComments();

    const result = this.tryParseQueryExpressionAtCurrent(mainComments);
    if (result && (result.type === 'select' || result.type === 'union')) {
      mainQuery = result;
    } else {
      if (!options.queryOnly) {
        const kw = this.peekUpper();
        if (kw === 'INSERT') {
          mainQuery = this.parseInsert(mainComments);
          return { type: 'cte', recursive, ctes, search, cycle, mainQuery, leadingComments: comments };
        }
        if (kw === 'UPDATE') {
          mainQuery = this.parseUpdate(mainComments);
          return { type: 'cte', recursive, ctes, search, cycle, mainQuery, leadingComments: comments };
        }
        if (kw === 'DELETE') {
          mainQuery = this.parseDelete(mainComments);
          return { type: 'cte', recursive, ctes, search, cycle, mainQuery, leadingComments: comments };
        }
        if (kw === 'MERGE') {
          mainQuery = this.parseMerge(mainComments);
          return { type: 'cte', recursive, ctes, search, cycle, mainQuery, leadingComments: comments };
        }
      }

      const first = this.parseSelect();
      const firstWithComments = { ...first, leadingComments: mainComments };

      if (this.checkUnionKeyword()) {
        const members: { statement: AST.QueryExpression; parenthesized: boolean }[] = [
          { statement: firstWithComments, parenthesized: false }
        ];
        const operators: string[] = [];
        while (this.checkUnionKeyword()) {
          operators.push(this.consumeUnionKeyword());
          const memberComments = this.consumeComments();
          members.push(this.parseQueryMember(memberComments));
        }
        mainQuery = { type: 'union', members, operators, leadingComments: mainComments };
      } else {
        mainQuery = firstWithComments;
      }
    }

    return { type: 'cte', recursive, ctes, search, cycle, mainQuery, leadingComments: comments };
  }

  private parseCTESearchClause(): AST.CTESearchClause {
    this.expect('SEARCH');
    let mode: 'DEPTH FIRST' | 'BREADTH FIRST';
    if (this.peekUpper() === 'DEPTH') {
      this.advance();
      this.expect('FIRST');
      mode = 'DEPTH FIRST';
    } else if (this.peekUpper() === 'BREADTH') {
      this.advance();
      this.expect('FIRST');
      mode = 'BREADTH FIRST';
    } else {
      throw new ParseError('DEPTH or BREADTH', this.peek());
    }
    this.expect('BY');
    const by: string[] = [];
    by.push(this.advance().value);
    while (this.check(',')) {
      this.advance();
      by.push(this.advance().value);
    }
    this.expect('SET');
    const set = this.advance().value;
    return { mode, by, set };
  }

  private parseCTECycleClause(): AST.CTECycleClause {
    this.expect('CYCLE');
    const columns: string[] = [];
    columns.push(this.advance().value);
    while (this.check(',')) {
      this.advance();
      columns.push(this.advance().value);
    }

    this.expect('SET');
    const set = this.advance().value;

    let to: AST.Expression | undefined;
    let defaultExpr: AST.Expression | undefined;
    if (this.peekUpper() === 'TO') {
      this.advance();
      to = this.parseExpression();
    }
    if (this.peekUpper() === 'DEFAULT') {
      this.advance();
      defaultExpr = this.parseExpression();
    }

    let using: string | undefined;
    if (this.peekUpper() === 'USING') {
      this.advance();
      using = this.advance().value;
    }

    return { columns, set, to, default: defaultExpr, using };
  }

  private consumeCommentsIfFollowedByKeyword(keyword: string): number {
    let lookahead = this.pos;
    while (lookahead < this.tokens.length) {
      const token = this.tokens[lookahead];
      if (token.type === 'line_comment' || token.type === 'block_comment') {
        lookahead++;
        continue;
      }
      break;
    }

    if (lookahead >= this.tokens.length) return 0;
    if (this.tokens[lookahead].upper !== keyword) return 0;

    let consumed = 0;
    while (this.peekType() === 'line_comment' || this.peekType() === 'block_comment') {
      this.advance();
      consumed++;
    }
    return consumed;
  }

  private consumeCommentsIfFollowedByJoinKeyword(): void {
    let lookahead = this.pos;
    while (lookahead < this.tokens.length) {
      const token = this.tokens[lookahead];
      if (token.type === 'line_comment' || token.type === 'block_comment') {
        lookahead++;
        continue;
      }
      break;
    }

    if (!this.isJoinKeywordAt(lookahead)) return;
    while (this.peekType() === 'line_comment' || this.peekType() === 'block_comment') {
      this.advance();
    }
  }

  private isJoinKeywordAt(index: number): boolean {
    const kw = this.tokens[index]?.upper ?? '';
    if (kw === 'JOIN') return true;
    if (kw === 'STRAIGHT_JOIN') return true;
    if ((kw === 'CROSS' || kw === 'OUTER') && (this.tokens[index + 1]?.upper ?? '') === 'APPLY') return true;
    if (kw === 'INNER' || kw === 'LEFT' || kw === 'RIGHT' || kw === 'FULL' || kw === 'CROSS' || kw === 'NATURAL') {
      const next1 = this.tokens[index + 1]?.upper ?? '';
      if (next1 === 'JOIN') return true;
      if (next1 === 'OUTER' || next1 === 'INNER') {
        const next2 = this.tokens[index + 2]?.upper ?? '';
        return next2 === 'JOIN';
      }
      return false;
    }
    return false;
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

    this.expect('AS');
    const postAsComments = this.consumeComments();

    // MATERIALIZED / NOT MATERIALIZED hints
    let materializedHint: 'materialized' | 'not_materialized' | undefined;
    if (this.peekUpper() === 'MATERIALIZED') {
      this.advance();
      materializedHint = 'materialized';
    } else if (this.peekUpper() === 'NOT' && this.peekUpperAt(1) === 'MATERIALIZED') {
      this.advance(); this.advance();
      materializedHint = 'not_materialized';
    }

    const postHintComments = this.consumeComments();
    this.expect('(');

    let query: AST.SelectStatement | AST.UnionStatement | AST.ValuesClause;
    const queryComments = [...postAsComments, ...postHintComments, ...this.consumeComments()];
    const first = this.peekUpper() === 'VALUES' ? this.parseValuesClause() : this.parseSelect();
    const firstWithComments = queryComments.length > 0
      ? { ...first, leadingComments: queryComments }
      : first;
    if (this.checkUnionKeyword()) {
      const members: { statement: AST.QueryExpression; parenthesized: boolean }[] = [
        { statement: firstWithComments, parenthesized: false }
      ];
      const operators: string[] = [];
      while (this.checkUnionKeyword()) {
        operators.push(this.consumeUnionKeyword());
        const memberComments = this.consumeComments();
        members.push(this.parseQueryMember(memberComments));
      }
      query = { type: 'union', members, operators, leadingComments: queryComments };
    } else {
      query = firstWithComments;
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
        if (t.value === ')') { depth--; continue; }
        return false;
      }
      if (depth > 1) {
        if (t.value === ')') { depth--; continue; }
        continue;
      }
      if (t.value === ')') return false;
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
    const tokenSql = (token: Token): string => (token.type === 'keyword' ? token.upper : token.value);
    const first = this.advance();
    let typeName = tokenSql(first);
    let continuationAnchor = first.upper;

    // Schema-qualified and dotted type names: schema.type, db.schema.type
    while (this.check('.')) {
      this.advance();
      const nextToken = this.peek();
      if (nextToken.type !== 'keyword' && nextToken.type !== 'identifier' && nextToken.type !== 'string') {
        throw new ParseError('type name', nextToken);
      }
      const consumed = this.advance();
      typeName += '.' + tokenSql(consumed);
      continuationAnchor = consumed.upper;
    }

    // Multi-word type names: DOUBLE PRECISION, TIMESTAMP WITH TIME ZONE, etc.
    while (true) {
      const nextToken = this.peek();
      if (nextToken.type !== 'keyword' && nextToken.type !== 'identifier') break;
      const validNext = TYPE_CONTINUATIONS[continuationAnchor];
      if (validNext === undefined || !validNext.has(nextToken.upper)) break;
      const consumed = this.advance();
      typeName += ' ' + tokenSql(consumed);
      continuationAnchor = consumed.upper;
    }

    return typeName;
  }

  private looksLikeMergeIntoValuesShorthand(): boolean {
    if (!(this.peekUpper() === 'MERGE' && this.peekUpperAt(1) === 'INTO')) return false;
    let lookahead = this.pos + 2;
    // Optional target name (possibly dotted).
    if (this.tokens[lookahead]?.type === 'identifier' || this.tokens[lookahead]?.type === 'keyword') {
      lookahead++;
      while (this.tokens[lookahead]?.value === '.') {
        lookahead += 2;
      }
    }
    return this.tokens[lookahead]?.value === '(';
  }

  // Consume a full type specifier: type name + optional (params) + optional []
  private consumeTypeSpecifier(): string {
    let typeName = this.consumeTypeNameToken();
    if (this.check('(')) {
      const typeArgsTokens: Token[] = [];
      let depth = 0;
      do {
        const token = this.advance();
        typeArgsTokens.push(token);
        if (token.value === '(') depth++;
        else if (token.value === ')') depth--;
      } while (!this.isAtEnd() && depth > 0);
      typeName += this.tokensToSqlPreserveCase(typeArgsTokens);
    }
    while (this.check('[')) {
      typeName += this.advance().value;
      if (this.check(']')) {
        typeName += this.advance().value;
      }
    }
    return typeName;
  }

  private collectTokensUntilTopLevelKeyword(stopKeywords: Set<string>): Token[] {
    const tokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd() && !this.check(';')) {
      const t = this.peek();
      if (depth === 0 && t.type === 'keyword' && stopKeywords.has(t.upper)) break;
      this.advance();
      tokens.push(t);
      if (this.isOpenGroupToken(t)) depth++;
      else if (this.isCloseGroupToken(t)) depth = Math.max(0, depth - 1);
    }
    return tokens;
  }

  private splitTopLevelByComma(tokens: Token[]): Token[][] {
    const groups: Token[][] = [];
    let current: Token[] = [];
    let depth = 0;
    for (const t of tokens) {
      if (depth === 0 && t.value === ',') {
        groups.push(current);
        current = [];
        continue;
      }
      current.push(t);
      if (this.isOpenGroupToken(t)) depth++;
      else if (this.isCloseGroupToken(t)) depth = Math.max(0, depth - 1);
    }
    if (current.length > 0) groups.push(current);
    return groups;
  }

  private consumeTokensUntilActionBoundary(): Token[] {
    const tokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd() && !this.check(';')) {
      const t = this.peek();
      if (depth === 0 && t.value === ',') break;
      if (depth === 0 && tokens.length > 0 && this.hasImplicitStatementBoundary()) break;
      this.advance();
      tokens.push(t);
      if (this.isOpenGroupToken(t)) depth++;
      else if (this.isCloseGroupToken(t)) depth = Math.max(0, depth - 1);
    }
    return tokens;
  }

  private hasTopLevelKeywordBeforeFunctionClose(keyword: string): boolean {
    let depth = 0;
    for (let i = this.pos; i < this.tokens.length; i++) {
      const token = this.tokens[i];
      if (depth === 0 && token.value === ')') return false;
      if (depth === 0 && token.upper === keyword) return true;
      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }
    return false;
  }

  private consumeTokensUntilTopLevelFunctionClose(): Token[] {
    const tokens: Token[] = [];
    let depth = 0;
    while (!this.isAtEnd()) {
      if (depth === 0 && this.check(')')) break;
      const token = this.advance();
      tokens.push(token);
      if (this.isOpenGroupToken(token)) depth++;
      else if (this.isCloseGroupToken(token)) depth = Math.max(0, depth - 1);
    }
    return tokens;
  }

  private isOpenGroupToken(token: Token): boolean {
    return token.value === '(' || token.value === '[' || token.value === '{';
  }

  private isCloseGroupToken(token: Token): boolean {
    return token.value === ')' || token.value === ']' || token.value === '}';
  }

  private tokenToSqlValue(token: Token): string {
    if (token.upper === 'TABLES') return 'TABLES';
    return token.type === 'keyword' ? token.upper : token.value;
  }

  private isStandaloneSlashTerminator(): boolean {
    const token = this.peek();
    if (token.value !== '/') return false;
    if (token.column !== 1) return false;
    const next = this.peekAt(1);
    if (next.type === 'eof') return true;
    return next.line > token.line;
  }

  private isSyntheticSlashSemicolon(token: Token): boolean {
    if (token.value !== ';') return false;
    if (this.source === undefined) return false;
    if (this.source[token.position] !== '/') return false;
    return token.column === 1;
  }

  private tokensToSqlPreserveCase(tokens: Token[]): string {
    if (tokens.length === 0) return '';
    let out = '';
    for (let i = 0; i < tokens.length; i++) {
      const currToken = tokens[i];
      const curr = currToken.value;
      const prevToken = i > 0 ? tokens[i - 1] : undefined;
      const prev = prevToken?.value ?? '';
      if (i === 0) {
        out += curr;
        continue;
      }

      if (prevToken?.type === 'line_comment') {
        out += '\n' + curr;
        continue;
      }

      const noSpaceBefore = curr === ',' || curr === ')' || curr === ']' || curr === ';' || curr === '.' || curr === '(' || curr === ':';
      const noSpaceAfterPrev = prev === '(' || prev === '[' || prev === '.' || prev === '::' || prev === ':';
      const noSpaceAroundPair = curr === '::' || curr === '[' || prev === '::' || curr === '@' || prev === '@';
      if (noSpaceBefore || noSpaceAfterPrev || noSpaceAroundPair) {
        out += curr;
      } else {
        out += ' ' + curr;
      }
    }
    return out.trim();
  }

  private tokensToSqlKeywordNormalized(tokens: Token[]): string {
    if (tokens.length === 0) return '';
    if (this.source === undefined) return this.tokensToSql(tokens);

    let out = '';
    let cursor = tokens[0].position;
    let inRoutineSignature = false;
    let routineParamDepth = 0;
    let previousSignificant: Token | undefined;
    const routineParamModes = new Set(['IN', 'OUT', 'INOUT', 'VARIADIC']);

    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i];
      if (token.position > cursor) {
        out += this.source.slice(cursor, token.position);
      }
      const prev = i > 0 ? tokens[i - 1] : undefined;
      const next = i < tokens.length - 1 ? tokens[i + 1] : undefined;
      const prevSig = previousSignificant;
      const prevSigValue = prevSig?.value;
      const prevSigUpper = prevSig?.upper;
      const nextSig = this.getNextSignificantToken(tokens, i + 1);

      if (routineParamDepth > 0) {
        if (token.value === '(') routineParamDepth++;
        else if (token.value === ')') routineParamDepth = Math.max(0, routineParamDepth - 1);
      } else {
        if (token.upper === 'FUNCTION' || token.upper === 'PROCEDURE') {
          inRoutineSignature = true;
        } else if (
          inRoutineSignature
          && (token.upper === 'RETURNS' || token.upper === 'AS' || token.upper === 'IS' || token.upper === 'BEGIN')
        ) {
          inRoutineSignature = false;
        } else if (inRoutineSignature && token.value === '(') {
          routineParamDepth = 1;
        }
      }

      const inQualifiedIdentifier =
        token.type === 'keyword'
        && (prev?.value === '.' || next?.value === '.');
      const keywordLooksLikeRoutineParamName =
        token.type === 'keyword'
        && routineParamDepth === 1
        && (
          prevSigValue === '('
          || prevSigValue === ','
          || prevSigUpper === 'IN'
          || prevSigUpper === 'OUT'
          || prevSigUpper === 'INOUT'
          || prevSigUpper === 'VARIADIC'
        )
        && !routineParamModes.has(token.upper);

      const keywordLooksLikeSelectProjectionIdentifier =
        token.type === 'keyword'
        && (prevSigUpper === 'SELECT' || prevSigValue === ',')
        && this.isSelectProjectionBoundaryToken(nextSig);

      out += (
        token.type === 'keyword'
        && !inQualifiedIdentifier
        && !keywordLooksLikeRoutineParamName
        && !keywordLooksLikeSelectProjectionIdentifier
      ) ? token.upper : token.value;
      cursor = token.position + token.value.length;

      if (token.type !== 'line_comment' && token.type !== 'block_comment') {
        previousSignificant = token;
      }
    }
    return out.trim();
  }

  private getNextSignificantToken(tokens: Token[], startIndex: number): Token | undefined {
    for (let i = startIndex; i < tokens.length; i++) {
      const token = tokens[i];
      if (token.type === 'line_comment' || token.type === 'block_comment') continue;
      return token;
    }
    return undefined;
  }

  private isSelectProjectionBoundaryToken(token: Token | undefined): boolean {
    if (!token) return false;
    if (token.value === ',') return true;
    if (token.value === ')') return true;
    return token.type === 'keyword' && SELECT_PROJECTION_BOUNDARY_KEYWORDS.has(token.upper);
  }

  private tokensToSql(tokens: Token[]): string {
    if (tokens.length === 0) return '';
    let out = '';
    for (let i = 0; i < tokens.length; i++) {
      const currToken = tokens[i];
      const curr = this.tokenToSqlValue(currToken);
      const prevToken = i > 0 ? tokens[i - 1] : undefined;
      const prev = prevToken ? this.tokenToSqlValue(prevToken) : '';
      if (i === 0) {
        out += curr;
        continue;
      }

      if (prevToken?.type === 'line_comment') {
        out += '\n' + curr;
        continue;
      }

      const noSpaceBefore = curr === ',' || curr === ')' || curr === ']' || curr === ';' || curr === '.' || curr === '(' || curr === ':';
      const noSpaceAfterPrev = prev === '(' || prev === '[' || prev === '.' || prev === '::' || prev === ':';
      const noSpaceAroundPair = curr === '::' || curr === '[' || prev === '::' || curr === '@' || prev === '@';
      if (noSpaceBefore || noSpaceAfterPrev || noSpaceAroundPair) {
        out += curr;
      } else {
        out += ' ' + curr;
      }
    }
    return out.trim();
  }

  private rawExpressionFromTokenRange(startIndex: number, endIndex: number): AST.RawExpression {
    const tokens = this.tokens.slice(startIndex, endIndex);
    return {
      type: 'raw',
      text: this.tokensToSqlPreserveCase(tokens),
      reason: 'verbatim',
    };
  }

  // Helper methods

  private consumeComments(): AST.CommentNode[] {
    const comments: AST.CommentNode[] = [];
    let previousLine = this.pos > 0 ? this.tokens[this.pos - 1].line : this.peek().line;
    while (this.peekType() === 'line_comment' || this.peekType() === 'block_comment') {
      const t = this.advance();
      comments.push({
        type: 'comment',
        style: t.type === 'line_comment' ? 'line' : 'block',
        text: t.value,
        startsOnOwnLine: t.line > previousLine,
        blankLinesBefore: this.blankLinesBeforeToken.get(t.position) || 0,
      });
      previousLine = t.line;
    }
    if (comments.length > 0 && !this.isAtEnd()) {
      const blanksAfter = this.blankLinesBeforeToken.get(this.peek().position) || 0;
      if (blanksAfter > 0) {
        const last = comments[comments.length - 1];
        comments[comments.length - 1] = { ...last, blankLinesAfter: blanksAfter };
      }
    }
    return comments;
  }

  private static readonly EOF_TOKEN: Token = { type: 'eof', value: '', upper: '', position: -1, line: 0, column: 0 };

  private peekAt(offset: number): Token {
    const idx = this.pos + offset;
    if (idx < 0 || idx >= this.tokens.length) return Parser.EOF_TOKEN;
    return this.tokens[idx];
  }

  private peek(): Token {
    return this.peekAt(0);
  }

  private peekType(): Token['type'] {
    return this.peekAt(0).type;
  }

  private peekUpper(): string {
    return this.peekAt(0).upper;
  }

  private peekUpperAt(offset: number): string {
    return this.peekAt(offset).upper;
  }

  private peekTypeAt(offset: number): Token['type'] {
    return this.peekAt(offset).type;
  }

  private check(value: string): boolean {
    const token = this.peek();
    return token.value === value || token.upper === value;
  }

  private advance(): Token {
    const token = this.peek();
    if (token.type === 'eof') {
      throw new ParseError('unexpected end of input', token);
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

  private isAtEnd(): boolean {
    return this.pos >= this.tokens.length || this.tokens[this.pos].type === 'eof';
  }

  private withDepth<T>(fn: () => T): T {
    this.depth++;
    if (this.depth > this.maxDepth) {
      this.depth--;
      throw new MaxDepthError(this.maxDepth, this.peek());
    }
    try {
      return fn();
    } finally {
      this.depth--;
    }
  }
}

/**
 * Parse a SQL string into an array of AST nodes.
 *
 * This is a convenience wrapper that tokenizes the input and runs the
 * {@link Parser}. Each top-level SQL statement becomes one node in the
 * returned array.
 *
 * By default, **recovery mode** is enabled: statements that cannot be parsed
 * are captured as `RawExpression` nodes (type `'raw'`) so the formatter can
 * pass them through unchanged. If recovery cannot produce raw text (for
 * example, an error at end-of-input), the statement is dropped and callers can
 * observe it via `onDropStatement`.
 *
 * To get strict parsing where every syntax error throws, set `recover: false`.
 *
 * Note: {@link MaxDepthError} always throws even in recovery mode, because
 * exceeding the nesting limit is a security boundary, not a syntax issue.
 *
 * @param input    Raw SQL text containing one or more statements.
 * @param options  Parser options (recovery mode, max nesting depth).
 * @returns An array of {@link AST.Node} trees, one per statement. In
 *   recovery mode (default), some entries may be `RawExpression` nodes
 *   for statements that could not be parsed. Returns an empty array for
 *   blank input.
 * @throws {TokenizeError} When the input contains unterminated literals or comments.
 * @throws {ParseError} When `recover` is `false` and a statement cannot be parsed.
 * @throws {MaxDepthError} When nesting depth exceeds `maxDepth` (always, regardless
 *   of recovery mode).
 *
 * @example
 * import { parse } from 'holywell';
 *
 * const ast = parse('SELECT id, name FROM users WHERE active = TRUE;');
 * // ast[0].type === 'select'
 *
 * @example
 * // Strict mode -- throws on parse errors instead of recovering
 * const ast = parse('SELECT ...', { recover: false });
 *
 * @example
 * // Detect unrecognized statements in recovery mode
 * const nodes = parse('SELECT 1; FOOBAR baz;');
 * for (const node of nodes) {
 *   if (node.type === 'raw') console.warn('Unparsed:', node);
 * }
 */
export function parse(input: string, options: ParseOptions = {}): AST.Node[] {
  if (!input.trim()) return [];
  const recover = options.recover ?? true;
  const parser = new Parser(
    tokenize(input, {
      dialect: options.dialect,
      allowMetaCommands: true,
      maxTokenCount: options.maxTokenCount,
    }),
    options,
    input,
  );
  return parser.parseStatements();
}
