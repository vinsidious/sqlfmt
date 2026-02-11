import { isKeyword } from './keywords';
import { MAX_IDENTIFIER_LENGTH, MAX_TOKEN_COUNT } from './constants';
import type { SQLDialect } from './dialect';

export type TokenType =
  | 'keyword'
  | 'identifier'
  | 'parameter'
  | 'number'
  | 'string'
  | 'operator'
  | 'punctuation'
  | 'line_comment'
  | 'block_comment'
  | 'whitespace'
  | 'eof';

/**
 * A single token produced by the SQL tokenizer.
 */
export interface Token {
  /** Semantic category of the token. */
  type: TokenType;
  /** Original source text of the token. */
  value: string;
  /** Upper-cased value (meaningful for keywords; empty for whitespace). */
  upper: string;
  /** Zero-based character offset in the input string. */
  position: number;
  /** One-based line number in the input. */
  line: number;
  /** One-based column number in the input. */
  column: number;
}

export interface TokenizeOptions {
  dialect?: SQLDialect;
  /**
   * Allow psql/MySQL script-control backslash lines in tokenization.
   *
   * When enabled, backslash-prefixed line commands (for example `\d users`
   * and `\.`) are tokenized as `line_comment` so parser recovery can preserve
   * them instead of failing at the tokenizer boundary.
   *
   * @default false
   */
  allowMetaCommands?: boolean;

  /**
   * Maximum number of tokens emitted before failing fast.
   *
   * @default MAX_TOKEN_COUNT
   */
  maxTokenCount?: number;
}

/**
 * Thrown when the tokenizer encounters invalid input such as an unterminated
 * string literal, quoted identifier, or block comment.
 *
 * Carries positional information so callers can produce useful diagnostics.
 *
 * @example
 * ```typescript
 * import { tokenize, TokenizeError } from 'holywell';
 *
 * try {
 *   tokenize("SELECT 'unterminated");
 * } catch (err) {
 *   if (err instanceof TokenizeError) {
 *     console.error(`Error at ${err.line}:${err.column}: ${err.message}`);
 *   }
 * }
 * ```
 */
export class TokenizeError extends Error {
  /** Zero-based character offset where the error was detected. */
  readonly position: number;
  /** One-based line number where the error was detected. */
  readonly line: number;
  /** One-based column number where the error was detected. */
  readonly column: number;

  constructor(message: string, position: number, line: number = 1, column: number = 1) {
    super(`${message} at line ${line}, column ${column}`);
    this.name = 'TokenizeError';
    this.position = position;
    this.line = line;
    this.column = column;
  }
}

// \p{L} matches any Unicode letter (Latin, CJK, Cyrillic, Greek, etc.)
const IDENT_START_RE = /[\p{L}_]/u;
// \p{L} = Unicode letter, \p{N} = Unicode number (digits in any script),
// \p{M} = combining marks (needed for locale-sensitive case mappings, e.g. i + dot-above)
const IDENT_CONT_RE = /[\p{L}\p{N}\p{M}_]/u;

function isAsciiDigitCode(code: number): boolean {
  return code >= 48 && code <= 57;
}

function isDigit(ch: string | undefined): boolean {
  return !!ch && isAsciiDigitCode(ch.charCodeAt(0));
}

function isHexDigitCode(code: number): boolean {
  return isAsciiDigitCode(code)
    || (code >= 65 && code <= 70)
    || (code >= 97 && code <= 102);
}

function isHexDigit(ch: string | undefined): boolean {
  return !!ch && isHexDigitCode(ch.charCodeAt(0));
}

function isWhitespaceCode(code: number): boolean {
  return code === 9 || code === 10 || code === 11 || code === 12 || code === 13 || code === 32;
}

function isAsciiLetterCode(code: number): boolean {
  return (code >= 65 && code <= 90) || (code >= 97 && code <= 122);
}

function isDollarTagStart(ch: string | undefined): boolean {
  if (!ch) return false;
  const code = ch.charCodeAt(0);
  return isAsciiLetterCode(code) || code === 95;
}

function isDollarTagCont(ch: string | undefined): boolean {
  if (!ch) return false;
  const code = ch.charCodeAt(0);
  return isAsciiLetterCode(code) || isAsciiDigitCode(code) || code === 95;
}

function isIdentifierStart(ch: string): boolean {
  const code = ch.charCodeAt(0);
  if (isAsciiLetterCode(code) || code === 95) return true;
  return IDENT_START_RE.test(ch);
}

function isIdentifierContinuation(ch: string): boolean {
  const code = ch.charCodeAt(0);
  if (isAsciiLetterCode(code) || isAsciiDigitCode(code) || code === 95) return true;
  return IDENT_CONT_RE.test(ch);
}

function isBoxDrawingChar(ch: string): boolean {
  const cp = ch.codePointAt(0);
  if (cp === undefined) return false;
  return (
    (cp >= 0x2500 && cp <= 0x257F) // Box Drawing
    || (cp >= 0x2580 && cp <= 0x259F) // Block Elements
    || (cp >= 0x25A0 && cp <= 0x25FF) // Geometric shapes often used in ASCII-art dumps
  );
}

function isLineStartOrIndented(input: string, pos: number): boolean {
  let i = pos - 1;
  while (i >= 0) {
    const ch = input[i];
    if (ch === '\n' || ch === '\r') return true;
    if (ch !== ' ' && ch !== '\t') return false;
    i--;
  }
  return true;
}

function isInlineMetaCommand(input: string, pos: number): boolean {
  if (pos <= 0) return false;
  const prev = input[pos - 1];
  if (prev !== ' ' && prev !== '\t') return false;
  const next = input[pos + 1];
  if (!next) return false;
  const code = next.charCodeAt(0);
  return isAsciiLetterCode(code);
}

const SLASH_TERMINATOR_NEXT_KEYWORDS = new Set([
  'SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'MERGE',
  'CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'GRANT', 'REVOKE',
  'VALUES', 'COPY', 'COMMENT', 'CALL', 'BEGIN', 'COMMIT',
  'ROLLBACK', 'SAVEPOINT', 'RELEASE', 'START', 'SET', 'RESET',
  'USE', 'SHOW', 'DESC', 'DESCRIBE', 'ANALYZE', 'VACUUM',
  'REINDEX', 'DECLARE', 'PREPARE', 'EXECUTE', 'EXEC', 'DO',
  'IF', 'GO', 'DELIMITER', 'PRAGMA', 'LOCK', 'UNLOCK',
  'BACKUP', 'BULK', 'CLUSTER', 'DBCC', 'REORG',
]);

function readAsciiWordUpper(input: string, start: number): string {
  let pos = start;
  while (pos < input.length) {
    const code = input.charCodeAt(pos);
    if (isAsciiLetterCode(code) || (pos > start && isAsciiDigitCode(code)) || code === 95) {
      pos++;
      continue;
    }
    break;
  }
  return input.slice(start, pos).toUpperCase();
}

function previousNonWhitespaceChar(input: string, start: number): string | undefined {
  let pos = start - 1;
  while (pos >= 0) {
    const ch = input[pos];
    if (ch !== ' ' && ch !== '\t' && ch !== '\n' && ch !== '\r') return ch;
    pos--;
  }
  return undefined;
}

function nextNonWhitespacePos(input: string, start: number): number {
  let pos = start;
  while (pos < input.length) {
    const ch = input[pos];
    if (ch !== ' ' && ch !== '\t' && ch !== '\n' && ch !== '\r') break;
    pos++;
  }
  return pos;
}

function isLikelyDivisionOnStandaloneSlashLine(
  input: string,
  slashStart: number,
  lineEnd: number,
  groupDepth: number,
): boolean {
  if (groupDepth > 0) return true;

  const prev = previousNonWhitespaceChar(input, slashStart);
  if (!prev || prev === ';' || prev === ',' || prev === '(' || prev === '[' || prev === '{') return false;

  const nextPos = nextNonWhitespacePos(input, lineEnd);
  if (nextPos >= input.length) return false;
  const next = input[nextPos];

  if (
    next === '(' || next === '[' || next === '{'
    || next === "'" || next === '"' || next === '`'
    || next === ':' || next === '$' || next === '@'
    || isDigit(next)
  ) {
    return true;
  }

  if (isIdentifierStart(next)) {
    const word = readAsciiWordUpper(input, nextPos);
    if (word && SLASH_TERMINATOR_NEXT_KEYWORDS.has(word)) return false;
    return true;
  }

  return false;
}

function isBindParameterBoundaryChar(ch: string | undefined): boolean {
  if (!ch) return true;
  const code = ch.charCodeAt(0);
  if (isWhitespaceCode(code)) return true;
  return '([,{=<>!+-*/%|&^:;'.includes(ch);
}

function shouldTreatBackslashQuoteAsStringEscape(input: string, backslashPos: number): boolean {
  const next = input[backslashPos + 2];
  if (!next) return false;

  if (next === "'" || next === '\\') return true;

  const code = next.charCodeAt(0);
  return isAsciiLetterCode(code) || isAsciiDigitCode(code) || next === '_';
}

function previousSignificantToken(tokens: Token[]): Token | undefined {
  for (let i = tokens.length - 1; i >= 0; i--) {
    if (tokens[i].type !== 'whitespace') return tokens[i];
  }
  return undefined;
}

function shouldPreferHashIdentifierAtLineStart(prev: Token | undefined): boolean {
  if (!prev) return false;
  if (prev.value === '.') return true;
  if (prev.type !== 'keyword') return false;
  return (
    prev.upper === 'FROM'
    || prev.upper === 'JOIN'
    || prev.upper === 'INTO'
    || prev.upper === 'UPDATE'
    || prev.upper === 'TABLE'
    || prev.upper === 'DELETE'
    || prev.upper === 'TRUNCATE'
    || prev.upper === 'LOCK'
    || prev.upper === 'UNLOCK'
    || prev.upper === 'AS'
    || prev.upper === 'USING'
    || prev.upper === 'ON'
  );
}

const ANGLE_TEMPLATE_IDENTIFIER_PRECEDERS = new Set([
  'TABLE',
  'VIEW',
  'INDEX',
  'TRIGGER',
  'PROCEDURE',
  'FUNCTION',
  'SCHEMA',
  'DATABASE',
  'FROM',
  'JOIN',
  'INTO',
  'UPDATE',
  'DELETE',
  'TRUNCATE',
  'DROP',
  'CREATE',
  'ALTER',
  'ON',
  'REFERENCES',
]);

function shouldPreferAngleTemplateIdentifier(prev: Token | undefined): boolean {
  if (!prev) return true;
  if (prev.value === '.' || prev.value === ',' || prev.value === '(') return true;
  if (prev.type !== 'keyword') return false;
  return ANGLE_TEMPLATE_IDENTIFIER_PRECEDERS.has(prev.upper);
}

function readAngleTemplateIdentifier(input: string, start: number): number | null {
  if (input[start] !== '<') return null;
  let pos = start + 1;
  let sawComma = false;
  while (pos < input.length) {
    const ch = input[pos];
    if (ch === '>') {
      if (!sawComma || pos === start + 1) return null;
      return pos + 1;
    }
    if (ch === ',' && !sawComma) sawComma = true;
    if (ch === '<' || ch === ';' || ch === '\n' || ch === '\r') return null;
    pos++;
  }
  return null;
}

function readDollarDelimiter(input: string, start: number): string | null {
  if (input[start] !== '$') return null;

  // $$...$$
  if (input[start + 1] === '$') return '$$';

  // $tag$...$tag$
  if (!isDollarTagStart(input[start + 1])) return null;
  let pos = start + 2;
  while (pos < input.length && isDollarTagCont(input[pos])) pos++;
  if (input[pos] !== '$') return null;
  return input.slice(start, pos + 1);
}

function readQuotedString(
  input: string,
  start: number,
  allowBackslashEscapes: boolean,
  lineOffsets?: number[],
): number {
  let pos = start;
  while (pos < input.length) {
    const ch = input[pos];
    if (allowBackslashEscapes && ch === '\\') {
      if (pos + 1 < input.length) {
        pos += 2;
      } else {
        pos += 1;
      }
      continue;
    }

    if (ch === '\\' && pos + 1 < input.length && input[pos + 1] === "'") {
      // Compatibility mode for mixed-dialect inputs:
      // keep supporting common MySQL-style \' escapes, but avoid swallowing
      // PostgreSQL-style literals where backslash is ordinary text.
      //
      // Only an odd-length backslash run can escape the quote:
      //   \'   -> potential escaped quote
      //   \\'  -> literal backslash + closing quote
      let runStart = pos;
      while (runStart > start && input[runStart - 1] === '\\') {
        runStart--;
      }
      const backslashRunLength = pos - runStart + 1;
      if (backslashRunLength % 2 === 1) {
        if (shouldTreatBackslashQuoteAsStringEscape(input, pos)) {
          pos += 2;
          continue;
        }
      }
    }
    if (ch === "'") {
      if (pos + 1 < input.length && input[pos + 1] === "'") {
        pos += 2;
        continue;
      }
      return pos + 1;
    }
    pos += 1;
  }
  const errPos = pos;
  if (lineOffsets) {
    const { line, column } = posToLineCol(lineOffsets, errPos);
    throw new TokenizeError('Unterminated string literal', errPos, line, column);
  }
  throw new TokenizeError('Unterminated string literal', errPos);
}

function readSmartQuotedString(
  input: string,
  start: number,
  openQuote: '‘' | '’',
  lineOffsets?: number[],
): number {
  const closeQuote = openQuote === '‘' ? '’' : openQuote;
  let pos = start;
  while (pos < input.length) {
    const ch = input[pos];
    if (ch === closeQuote) {
      // Treat doubled smart quotes as an escaped quote.
      if (pos + 1 < input.length && input[pos + 1] === closeQuote) {
        pos += 2;
        continue;
      }
      return pos + 1;
    }
    pos += 1;
  }
  const errPos = pos;
  if (lineOffsets) {
    const { line, column } = posToLineCol(lineOffsets, errPos);
    throw new TokenizeError('Unterminated string literal', errPos, line, column);
  }
  throw new TokenizeError('Unterminated string literal', errPos);
}

function normalizeSmartQuotes(value: string): string {
  return value.replace(/[‘’]/g, "'");
}

// Precompute line start offsets for O(1) line/column lookup.
// Note: positions and columns use JavaScript string indices (UTF-16 code units),
// which is consistent with how most editors (VS Code, etc.) report columns.
// Characters outside the BMP (e.g., emojis) occupy two UTF-16 code units and
// will therefore count as two columns. This is standard behavior.
function buildLineOffsets(input: string): number[] {
  const offsets = [0]; // line 1 starts at offset 0
  for (let i = 0; i < input.length; i++) {
    if (input[i] === '\n') offsets.push(i + 1);
  }
  return offsets;
}

function posToLineCol(lineOffsets: number[], pos: number): { line: number; column: number } {
  // Binary search for the line containing this position.
  // Column values are in UTF-16 code units (standard for editors like VS Code).
  let lo = 0;
  let hi = lineOffsets.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (lineOffsets[mid] <= pos) lo = mid;
    else hi = mid - 1;
  }
  return { line: lo + 1, column: pos - lineOffsets[lo] + 1 };
}

/**
 * Tokenize a SQL string into an array of tokens.
 *
 * Splits the input into typed tokens: keywords, identifiers, literals (strings,
 * numbers), operators, punctuation, comments, whitespace, and a trailing EOF
 * sentinel. The tokenizer handles PostgreSQL-specific syntax including
 * dollar-quoted strings, positional parameters (`$1`), type-cast operators
 * (`::`), JSON/path operators, and prefixed string literals (`E'...'`,
 * `B'...'`, `X'...'`).
 *
 * Each token carries `line` and `column` information for error reporting.
 *
 * @param input  Raw SQL text to tokenize.
 * @returns An array of {@link Token} objects ending with an `eof` token.
 * @throws {TokenizeError} When the input contains an unterminated string
 *   literal, quoted identifier, or block comment, or when the token count
 *   exceeds the safety limit.
 *
 * @example
 * import { tokenize } from 'holywell';
 *
 * const tokens = tokenize('SELECT 1;');
 * // [
 * //   { type: 'keyword',     value: 'SELECT', upper: 'SELECT', position: 0, line: 1, column: 1 },
 * //   { type: 'whitespace',  value: ' ',      upper: '',       position: 6, line: 1, column: 7 },
 * //   { type: 'number',      value: '1',      upper: '1',      position: 7, line: 1, column: 8 },
 * //   { type: 'punctuation', value: ';',      upper: ';',      position: 8, line: 1, column: 9 },
 * //   { type: 'eof',         value: '',       upper: '',       position: 9, line: 1, column: 10 },
 * // ]
 */
export function tokenize(input: string, options: TokenizeOptions = {}): Token[] {
  const tokens: Token[] = [];
  let pos = 0;
  const len = input.length;
  let groupDepth = 0;
  let statementStartTokenIndex = 0;
  let inCopyFromStdinData = false;
  const lineOffsets = buildLineOffsets(input);
  const allowMetaCommands = options.allowMetaCommands ?? false;
  const maxTokenCount = options.maxTokenCount ?? MAX_TOKEN_COUNT;
  const additionalKeywords = new Set(
    (options.dialect?.additionalKeywords ?? []).map(k => k.toUpperCase())
  );

  function lc(p: number) {
    const { line, column } = posToLineCol(lineOffsets, p);
    return { line, column };
  }

  /** Build a Token object and push it onto the output array. */
  function emit(type: TokenType, value: string, upper: string, position: number): void {
    if (tokens.length >= maxTokenCount) {
      const { line, column } = lc(position);
      throw new TokenizeError(
        `Token count exceeds maximum of ${maxTokenCount}. Use the maxTokenCount option to increase the limit for large SQL inputs`,
        position,
        line,
        column,
      );
    }
    const { line, column } = lc(position);
    tokens.push({ type, value, upper, position, line, column });

    if (type === 'punctuation') {
      if (value === '(' || value === '[' || value === '{') {
        groupDepth++;
      } else if (value === ')' || value === ']' || value === '}') {
        groupDepth = Math.max(0, groupDepth - 1);
      }

      if (value === ';') {
        const statementTokens = tokens.slice(statementStartTokenIndex, tokens.length);
        if (isCopyFromStdinStatement(statementTokens)) {
          inCopyFromStdinData = true;
        }
        statementStartTokenIndex = tokens.length;
      }
    }
  }

  function isCopyFromStdinStatement(statementTokens: Token[]): boolean {
    let sawCopy = false;
    let sawFrom = false;
    for (const token of statementTokens) {
      if (token.type === 'line_comment' || token.type === 'block_comment' || token.type === 'whitespace') {
        continue;
      }
      if (token.value === ';') break;
      if (!sawCopy) {
        if (token.upper === 'COPY') sawCopy = true;
        continue;
      }
      if (token.upper === 'FROM') {
        sawFrom = true;
        continue;
      }
      if (sawFrom) {
        return token.upper === 'STDIN';
      }
    }
    return false;
  }

  while (pos < len) {
    const start = pos;
    const ch = input[pos];

    // Whitespace
    if (isWhitespaceCode(ch.charCodeAt(0))) {
      while (pos < len && isWhitespaceCode(input.charCodeAt(pos))) pos++;
      emit('whitespace', input.slice(start, pos), '', start);
      continue;
    }

    if (inCopyFromStdinData) {
      while (pos < len && input[pos] !== '\n' && input[pos] !== '\r') pos++;
      const lineText = input.slice(start, pos);
      emit('line_comment', lineText, '', start);
      if (lineText.trim() === '\\.') {
        inCopyFromStdinData = false;
      }
      continue;
    }

    // Line comment: -- ...
    if (ch === '-' && pos + 1 < len && input[pos + 1] === '-') {
      pos += 2;
      while (pos < len && input[pos] !== '\n') pos++;
      // Trim trailing whitespace from line comments
      let end = pos;
      while (end > start && isWhitespaceCode(input.charCodeAt(end - 1))) end--;
      const commentText = input.slice(start, end);
      emit('line_comment', commentText, '', start);
      continue;
    }

    // Oracle SQL*Plus line comment: REM ...
    if (
      isLineStartOrIndented(input, start)
      && (ch === 'R' || ch === 'r')
      && start + 3 <= len
      && input.slice(start, start + 3).toUpperCase() === 'REM'
    ) {
      const afterRem = input[start + 3];
      const isCommentPrefix =
        afterRem === undefined
        || afterRem === ' '
        || afterRem === '\t'
        || afterRem === '\n'
        || afterRem === '\r';
      if (isCommentPrefix) {
        pos += 3;
        while (pos < len && input[pos] !== '\n') pos++;
        let end = pos;
        while (end > start && isWhitespaceCode(input.charCodeAt(end - 1))) end--;
        const commentText = input.slice(start, end);
        emit('line_comment', commentText, '', start);
        continue;
      }
    }

    // MySQL line comment: # ...
    // Treat # as a comment at line starts even without an extra space, while
    // preserving SQL Server #temp identifiers in common table-name contexts.
    if (ch === '#') {
      const next = input[pos + 1];
      const prev = previousSignificantToken(tokens);
      const startsMySqlComment =
        isLineStartOrIndented(input, start)
        && next !== '>'
        && !shouldPreferHashIdentifierAtLineStart(prev);
      if (startsMySqlComment) {
        pos += 1;
        while (pos < len && input[pos] !== '\n') pos++;
        let end = pos;
        while (end > start && isWhitespaceCode(input.charCodeAt(end - 1))) end--;
        const commentText = input.slice(start, end);
        emit('line_comment', commentText, '', start);
        continue;
      }
    }

    // Block comment: /* ... */
    // SQL*Plus statement terminator: slash on its own line.
    if (ch === '/' && isLineStartOrIndented(input, start)) {
      let lookahead = pos + 1;
      while (lookahead < len && (input[lookahead] === ' ' || input[lookahead] === '\t')) {
        lookahead++;
      }
      if (lookahead >= len || input[lookahead] === '\n' || input[lookahead] === '\r') {
        const treatAsDivision = isLikelyDivisionOnStandaloneSlashLine(input, start, lookahead, groupDepth);
        if (!treatAsDivision) {
          pos = lookahead;
          emit('punctuation', ';', ';', start);
          continue;
        }
      }
    }

    if (ch === '/' && pos + 1 < len && input[pos + 1] === '*') {
      pos += 2;
      while (pos < len && !(input[pos] === '*' && pos + 1 < len && input[pos + 1] === '/')) pos++;
      if (pos < len) {
        pos += 2; // skip */
      } else {
        const { line: eLine, column: eCol } = lc(start);
        throw new TokenizeError('Unterminated block comment', start, eLine, eCol);
      }
      emit('block_comment', input.slice(start, pos), '', start);
      continue;
    }

    // Template placeholders commonly used by BI/reporting tools: {{var}}
    if (ch === '{' && pos + 1 < len && input[pos + 1] === '{') {
      const close = input.indexOf('}}', pos + 2);
      if (close >= 0) {
        pos = close + 2;
      } else {
        pos = len;
      }
      const val = input.slice(start, pos);
      emit('parameter', val, val, start);
      continue;
    }

    // psql/meta commands in recovery mode:
    //   \d users
    //   \.
    // plus escaped semicolons used in some scripts: \;
    if (ch === '\\' && allowMetaCommands) {
      if (input[pos + 1] === ';') {
        pos += 2;
        emit('punctuation', ';', ';', start);
        continue;
      }

      const inlineMetaCommand = isInlineMetaCommand(input, start);
      if (isLineStartOrIndented(input, start) || inlineMetaCommand) {
        // Inline psql commands (e.g. "... \gset") implicitly terminate the
        // SQL statement in psql. Emit a synthetic semicolon to preserve this.
        if (inlineMetaCommand) {
          emit('punctuation', ';', ';', start);
        }
        pos++;
        while (pos < len && input[pos] !== '\n' && input[pos] !== '\r') pos++;
        let end = pos;
        while (end > start && (input[end - 1] === ' ' || input[end - 1] === '\t')) end--;
        emit('line_comment', input.slice(start, end), '', start);
        continue;
      }
    }

    // Dollar-quoted strings ($$...$$ or $tag$...$tag$) and positional parameters ($1, $2)
    if (ch === '$') {
      if (isDigit(input[pos + 1])) {
        pos++;
        while (pos < len && isDigit(input[pos])) pos++;
        const val = input.slice(start, pos);
        emit('parameter', val, val, start);
        continue;
      }

      const delim = readDollarDelimiter(input, pos);
      if (delim) {
        let close = -1;
        // Scan for the next matching delimiter.
        // This keeps delimiter matching explicit and avoids accidental partial
        // matches when tags are adjacent to other dollar-prefixed tokens.
        const bodyStart = pos + delim.length;
        for (let i = bodyStart; i <= len - delim.length; i++) {
          if (input[i] !== '$') continue;
          if (input.startsWith(delim, i)) {
            close = i;
            break;
          }
        }
        if (close >= 0) {
          pos = close + delim.length;
          const val = input.slice(start, pos);
          emit('string', val, val, start);
          continue;
        }
      }

      // Bare '$' with no valid dollar-quote or positional parameter --
      // emit as operator so the parser's recovery mode can handle it
      // gracefully instead of throwing an unrecoverable TokenizeError.
      pos++;
      emit('operator', '$', '$', start);
      continue;
    }

    // Unicode-escape prefixed strings: U&'...'
    if ((ch === 'U' || ch === 'u') && input[pos + 1] === '&' && input[pos + 2] === "'") {
      pos += 3;
      pos = readQuotedString(input, pos, true, lineOffsets);
      const val = input.slice(start, pos);
      emit('string', val, val, start);
      continue;
    }

    // Oracle alternative quoting: q'[...]', q'{...}', q'(...)', q'<...>', q'!...!'
    if ((ch === 'Q' || ch === 'q') && input[pos + 1] === "'") {
      const open = input[pos + 2];
      if (!open) {
        const { line: eLine, column: eCol } = lc(start);
        throw new TokenizeError('Unterminated string literal', start, eLine, eCol);
      }

      const closeMap: Record<string, string> = {
        '[': ']',
        '{': '}',
        '(': ')',
        '<': '>',
      };
      const close = closeMap[open] ?? open;
      pos += 3; // q'X
      let closed = false;
      while (pos < len) {
        if (input[pos] === close && input[pos + 1] === "'") {
          pos += 2;
          closed = true;
          break;
        }
        pos++;
      }
      if (!closed) {
        const { line: eLine, column: eCol } = lc(start);
        throw new TokenizeError('Unterminated string literal', start, eLine, eCol);
      }
      const val = input.slice(start, pos);
      emit('string', val, val, start);
      continue;
    }

    // Prefixed strings: E'...', B'...', X'...', N'...'
    if ('EeBbXxNn'.includes(ch) && input[pos + 1] === "'") {
      const allowBackslashEscapes = ch === 'E' || ch === 'e';
      pos += 2;
      pos = readQuotedString(input, pos, allowBackslashEscapes, lineOffsets);
      const val = input.slice(start, pos);
      emit('string', val, val, start);
      continue;
    }

    // String literal: 'text' (with '' escape)
    if (ch === "'") {
      pos++;
      pos = readQuotedString(input, pos, false, lineOffsets);
      const val = input.slice(start, pos);
      emit('string', val, val, start);
      continue;
    }

    // Smart-quote string literals copied from rich-text editors.
    if (ch === '‘' || ch === '’') {
      pos++;
      pos = readSmartQuotedString(input, pos, ch, lineOffsets);
      const raw = input.slice(start, pos);
      const normalized = normalizeSmartQuotes(raw);
      emit('string', normalized, normalized, start);
      continue;
    }

    // Quoted identifier: "identifier"
    if (ch === '"') {
      pos++;
      let closed = false;
      while (pos < len) {
        if (input[pos] === '"' && pos + 1 < len && input[pos + 1] === '"') {
          pos += 2; // escaped double quote inside identifier
        } else if (input[pos] === '"') {
          pos++;
          closed = true;
          break;
        } else {
          pos++;
        }
        // Enforce same length limit as unquoted identifiers
        if (pos - start > MAX_IDENTIFIER_LENGTH) {
          const { line: eLine, column: eCol } = lc(start);
          throw new TokenizeError(
            `Identifier exceeds maximum length of ${MAX_IDENTIFIER_LENGTH} characters`,
            start,
            eLine,
            eCol,
          );
        }
      }
      if (!closed) {
        const { line: eLine, column: eCol } = lc(start);
        throw new TokenizeError('Unterminated quoted identifier', start, eLine, eCol);
      }
      const val = input.slice(start, pos);
      emit('identifier', val, val, start);
      continue;
    }

    // SQL Server bracket-quoted identifier: [identifier]
    // Distinguish from array subscripts by context (dot/no-gap patterns).
    if (ch === '[') {
      // Exasol Lua long bracket strings: [[ ... ]]
      if (pos + 1 < len && input[pos + 1] === '[') {
        const close = input.indexOf(']]', pos + 2);
        if (close < 0) {
          const { line: eLine, column: eCol } = lc(start);
          throw new TokenizeError('Unterminated bracket string literal', start, eLine, eCol);
        }
        pos = close + 2;
        const val = input.slice(start, pos);
        emit('string', val, val, start);
        continue;
      }

      const prev = previousSignificantToken(tokens);
      const hasGapFromPrev = !!prev && start > (prev.position + prev.value.length);
      const prevCanStartSubscript =
        !!prev
        && (
          prev.type === 'identifier'
          || prev.type === 'keyword'
          || prev.type === 'number'
          || prev.type === 'string'
          || prev.type === 'parameter'
          || prev.value === ')'
          || prev.value === ']'
        );
      const canStartBracketIdentifier =
        !prev
        || prev.value === '.'
        || hasGapFromPrev
        || !prevCanStartSubscript;

      if (canStartBracketIdentifier) {
        pos++;
        let closed = false;
        while (pos < len) {
          if (input[pos] === ']') {
            if (input[pos + 1] === ']') {
              pos += 2; // escaped closing bracket
              continue;
            }
            pos++;
            closed = true;
            break;
          }
          pos++;
          if (pos - start > MAX_IDENTIFIER_LENGTH) {
            const { line: eLine, column: eCol } = lc(start);
            throw new TokenizeError(
              `Identifier exceeds maximum length of ${MAX_IDENTIFIER_LENGTH} characters`,
              start,
              eLine,
              eCol,
            );
          }
        }
        if (!closed) {
          const { line: eLine, column: eCol } = lc(start);
          throw new TokenizeError('Unterminated bracket-quoted identifier', start, eLine, eCol);
        }
        const val = input.slice(start, pos);
        emit('identifier', val, val, start);
        continue;
      }
    }

    // Backtick-quoted identifier (MySQL style)
    if (ch === '`') {
      pos++;
      while (pos < len) {
        if (input[pos] === '`') {
          if (input[pos + 1] === '`') {
            pos += 2; // escaped backtick
            continue;
          }
          break;
        }
        pos++;
      }
      if (pos >= len) {
        const { line: eLine, column: eCol } = lc(start);
        throw new TokenizeError('Unterminated backtick-quoted identifier', start, eLine, eCol);
      }
      pos++; // skip closing backtick
      const val = input.slice(start, pos);
      emit('identifier', val, val, start);
      continue;
    }

    // Number
    if (isDigit(ch) || (ch === '.' && isDigit(input[pos + 1]))) {
      const consumeDigitsWithUnderscores = (digitCheck: (c: string | undefined) => boolean): void => {
        let sawDigit = false;
        let canUnderscore = false;
        while (pos < len) {
          const curr = input[pos];
          if (digitCheck(curr)) {
            sawDigit = true;
            canUnderscore = true;
            pos++;
            continue;
          }
          if (curr === '_' && canUnderscore && digitCheck(input[pos + 1])) {
            canUnderscore = false;
            pos++;
            continue;
          }
          break;
        }
        if (!sawDigit) return;
      };

      // Hex numeric literal: 0xFF / 0XFF
      if (ch === '0' && (input[pos + 1] === 'x' || input[pos + 1] === 'X') && isHexDigit(input[pos + 2])) {
        pos += 2;
        consumeDigitsWithUnderscores(isHexDigit);
      } else {
        if (ch === '.') {
          pos++;
          consumeDigitsWithUnderscores(isDigit);
        } else {
          consumeDigitsWithUnderscores(isDigit);
          if (pos < len && input[pos] === '.') {
            pos++;
            consumeDigitsWithUnderscores(isDigit);
          }
        }

        // Scientific notation: 1e5, 1.2E-4, .5e+2
        // When e/E is not followed by a digit (with optional sign), we backtrack
        // so that e.g. `1e` is tokenized as number `1` followed by identifier `e`.
        // This is necessary because SQL allows identifiers like `e` as column
        // aliases: `SELECT 1e FROM t` means column "1" aliased as "e", not 1*10^(nothing).
        if (pos < len && (input[pos] === 'e' || input[pos] === 'E')) {
          const expStart = pos;
          pos++;
          if (input[pos] === '+' || input[pos] === '-') pos++;
          if (!isDigit(input[pos])) {
            pos = expStart;
          } else {
            consumeDigitsWithUnderscores(isDigit);
          }
        }
      }
      const val = input.slice(start, pos);
      emit('number', val, val, start);
      continue;
    }

    // Operator scanning — longest match first
    // We handle multi-char operators by checking the current char and looking ahead.

    // :: — PostgreSQL type cast
    if (ch === ':' && pos + 1 < len && input[pos + 1] === ':') {
      pos += 2;
      emit('operator', '::', '::', start);
      continue;
    }

    // Named argument / assignment operator used by PostgreSQL extensions.
    if (ch === ':' && pos + 1 < len && input[pos + 1] === '=') {
      pos += 2;
      emit('operator', ':=', ':=', start);
      continue;
    }

    // Oracle/SQL*Plus bind parameters: :name, :1, :schema.object
    if (ch === ':' && input[pos + 1] !== ':') {
      const next = input[pos + 1];
      if (next === "'" || next === '"') {
        const quote = next;
        pos += 2;
        let closed = false;
        while (pos < len) {
          if (input[pos] === quote) {
            if (input[pos + 1] === quote) {
              pos += 2;
              continue;
            }
            pos++;
            closed = true;
            break;
          }
          pos++;
        }
        if (!closed) {
          const { line: eLine, column: eCol } = lc(start);
          throw new TokenizeError('Unterminated parameter interpolation', start, eLine, eCol);
        }
        const val = input.slice(start, pos);
        emit('parameter', val, val, start);
        continue;
      }
      const prevChar = start > 0 ? input[start - 1] : undefined;
      const startsBind = !!next && (isIdentifierStart(next) || isDigit(next));
      if (startsBind && isBindParameterBoundaryChar(prevChar)) {
        pos++;
        while (pos < len) {
          const curr = input[pos];
          if (isIdentifierContinuation(curr) || isDigit(curr)) {
            pos++;
            continue;
          }
          if (
            curr === '.'
            && (isIdentifierStart(input[pos + 1] ?? '') || isDigit(input[pos + 1]))
          ) {
            pos++;
            continue;
          }
          break;
        }
        const val = input.slice(start, pos);
        emit('parameter', val, val, start);
        continue;
      }
    }

    // ! operators: !~* then !~ then !=
    if (ch === '!') {
      if (pos + 2 < len && input[pos + 1] === '~' && input[pos + 2] === '*') {
        pos += 3;
        emit('operator', '!~*', '!~*', start);
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '~') {
        pos += 2;
        emit('operator', '!~', '!~', start);
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '=') {
        pos += 2;
        emit('operator', '!=', '!=', start);
        continue;
      }
      // bare ! (not standard SQL but consume it)
      pos++;
      emit('operator', '!', '!', start);
      continue;
    }

    // Compound assignment operators: +=, -=, *=, /=, %=, &=, ^=, |=
    if (
      pos + 1 < len
      && input[pos + 1] === '='
      && (ch === '+' || ch === '-' || ch === '*' || ch === '/' || ch === '%' || ch === '&' || ch === '^' || ch === '|')
    ) {
      pos += 2;
      const op = input.slice(start, pos);
      emit('operator', op, op, start);
      continue;
    }

    // Bare backslashes can appear in COPY ... FROM stdin data rows and should
    // not terminate tokenization.
    if (ch === '\\') {
      pos++;
      emit('operator', '\\', '\\', start);
      continue;
    }

    // < operators: <@ then <> then << then <= then <
    if (ch === '<') {
      const prev = previousSignificantToken(tokens);
      if (shouldPreferAngleTemplateIdentifier(prev)) {
        const end = readAngleTemplateIdentifier(input, start);
        if (end !== null) {
          pos = end;
          const val = input.slice(start, pos);
          emit('identifier', val, val.toUpperCase(), start);
          continue;
        }
      }
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '@') {
          pos += 2;
          emit('operator', '<@', '<@', start);
          continue;
        }
        if (next === '>') {
          pos += 2;
          emit('operator', '<>', '<>', start);
          continue;
        }
        if (next === '<') {
          pos += 2;
          emit('operator', '<<', '<<', start);
          continue;
        }
        if (next === '=') {
          pos += 2;
          emit('operator', '<=', '<=', start);
          continue;
        }
      }
      pos++;
      emit('operator', '<', '<', start);
      continue;
    }

    // > operators: >= then >> then >
    if (ch === '>') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '=') {
          pos += 2;
          emit('operator', '>=', '>=', start);
          continue;
        }
        if (next === '>') {
          pos += 2;
          emit('operator', '>>', '>>', start);
          continue;
        }
      }
      pos++;
      emit('operator', '>', '>', start);
      continue;
    }

    // - operators: ->> then -> then - (line comment already handled above)
    if (ch === '-') {
      if (pos + 2 < len && input[pos + 1] === '>' && input[pos + 2] === '>') {
        pos += 3;
        emit('operator', '->>', '->>', start);
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '>') {
        pos += 2;
        emit('operator', '->', '->', start);
        continue;
      }
      pos++;
      emit('operator', '-', '-', start);
      continue;
    }

    // # operators: #>> then #> then #
    if (ch === '#') {
      // SQL Server temporary tables: #tmp / ##tmp / #1
      const hashNext = input[pos + 1];
      const hashNext2 = input[pos + 2];
      const hashStartsIdent = !!hashNext && (isIdentifierContinuation(hashNext) || isDigit(hashNext));
      const hashStartsGlobalTemp = hashNext === '#' && !!hashNext2 && (isIdentifierContinuation(hashNext2) || isDigit(hashNext2));
      if (
        hashStartsGlobalTemp
        || hashStartsIdent
      ) {
        pos++;
        if (input[pos] === '#') pos++;
        while (pos < len && (isIdentifierContinuation(input[pos]) || isDigit(input[pos]))) {
          pos++;
        }
        const val = input.slice(start, pos);
        emit('identifier', val, val.toUpperCase(), start);
        continue;
      }

      if (pos + 2 < len && input[pos + 1] === '>' && input[pos + 2] === '>') {
        pos += 3;
        emit('operator', '#>>', '#>>', start);
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '>') {
        pos += 2;
        emit('operator', '#>', '#>', start);
        continue;
      }
      pos++;
      emit('operator', '#', '#', start);
      continue;
    }

    // @ operators: @> then @? then @@ then bare @
    if (ch === '@') {
      // T-SQL variables: @name / @@name.
      // Require a token boundary so MySQL user@host stays as identifier @ operator identifier.
      const prevChar = start > 0 ? input[start - 1] : '';
      const atTokenBoundary =
        start === 0
        || isWhitespaceCode(prevChar.charCodeAt(0))
        || '([=,+-*/%<>;'.includes(prevChar);
      const atNext = input[pos + 1];
      const atNext2 = input[pos + 2];
      const atStartsIdent = !!atNext && (isIdentifierContinuation(atNext) || isDigit(atNext));
      const atStartsGlobal = atNext === '@' && !!atNext2 && (isIdentifierContinuation(atNext2) || isDigit(atNext2));
      if (
        atTokenBoundary
        && (
        atStartsGlobal
        || atStartsIdent
        )
      ) {
        pos++;
        if (input[pos] === '@') pos++;
        while (pos < len && (isIdentifierContinuation(input[pos]) || isDigit(input[pos]))) {
          pos++;
        }
        const val = input.slice(start, pos);
        emit('identifier', val, val.toUpperCase(), start);
        continue;
      }

      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '>') {
          pos += 2;
          emit('operator', '@>', '@>', start);
          continue;
        }
        if (next === '?') {
          pos += 2;
          emit('operator', '@?', '@?', start);
          continue;
        }
        if (next === '@') {
          pos += 2;
          emit('operator', '@@', '@@', start);
          continue;
        }
      }
      pos++;
      emit('operator', '@', '@', start);
      continue;
    }

    // ? operators: ?| then ?& then ?
    if (ch === '?') {
      if (isDigit(input[pos + 1])) {
        pos++;
        while (pos < len && isDigit(input[pos])) pos++;
        const val = input.slice(start, pos);
        emit('parameter', val, val, start);
        continue;
      }
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '|') {
          pos += 2;
          emit('operator', '?|', '?|', start);
          continue;
        }
        if (next === '&') {
          pos += 2;
          emit('operator', '?&', '?&', start);
          continue;
        }
      }
      pos++;
      emit('operator', '?', '?', start);
      continue;
    }

    // ~ operators: ~* then ~
    if (ch === '~') {
      if (pos + 1 < len && input[pos + 1] === '*') {
        pos += 2;
        emit('operator', '~*', '~*', start);
        continue;
      }
      pos++;
      emit('operator', '~', '~', start);
      continue;
    }

    // & operators: && then &
    if (ch === '&') {
      // SQL*Plus substitution vars: &name / &&name
      const prev = start > 0 ? input[start - 1] : '';
      const atTokenBoundary = start === 0 || isWhitespaceCode(prev.charCodeAt(0)) || '([=,+-*/%<>;'.includes(prev);
      if (atTokenBoundary) {
        const ampNext = input[pos + 1];
        const ampNext2 = input[pos + 2];
        const ampStartsIdent = !!ampNext && (isIdentifierContinuation(ampNext) || isDigit(ampNext));
        const ampStartsDouble = ampNext === '&' && !!ampNext2 && (isIdentifierContinuation(ampNext2) || isDigit(ampNext2));

        if (ampStartsDouble) {
          pos += 2;
          while (pos < len && (isIdentifierContinuation(input[pos]) || isDigit(input[pos]))) {
            pos++;
          }
          const val = input.slice(start, pos);
          emit('parameter', val, val, start);
          continue;
        }
        if (ampStartsIdent) {
          pos++;
          while (pos < len && (isIdentifierContinuation(input[pos]) || isDigit(input[pos]))) {
            pos++;
          }
          const val = input.slice(start, pos);
          emit('parameter', val, val, start);
          continue;
        }
      }

      if (pos + 1 < len && input[pos + 1] === '&') {
        pos += 2;
        emit('operator', '&&', '&&', start);
        continue;
      }
      pos++;
      emit('operator', '&', '&', start);
      continue;
    }

    // | operators: || then |
    if (ch === '|') {
      if (pos + 1 < len && input[pos + 1] === '|') {
        pos += 2;
        emit('operator', '||', '||', start);
        continue;
      }
      pos++;
      emit('operator', '|', '|', start);
      continue;
    }

    // Remaining simple single-char operators: = + * / % ^
    if ('=+*/%^'.includes(ch)) {
      pos++;
      emit('operator', ch, ch, start);
      continue;
    }

    // Punctuation (including [ ], and : for array slices)
    if ('(),;.[]:{}'.includes(ch)) {
      pos++;
      emit('punctuation', ch, ch, start);
      continue;
    }

    // Identifier or keyword
    if (isIdentifierStart(ch)) {
      const prev = previousSignificantToken(tokens);
      const allowTrailingDollar = prev?.value === '.';
      while (pos < len) {
        const curr = input[pos];
        if (isIdentifierContinuation(curr)) {
          pos++;
          continue;
        }

        // Oracle-style identifier chars: $, #.
        if (curr === '$') {
          const next = input[pos + 1];
          // Preserve $$ delimiter handling (dollar-quoted strings).
          if (next === '$') break;
          if (next && (isIdentifierContinuation(next) || next === '$' || next === '#')) {
            pos++;
            continue;
          }
          if (allowTrailingDollar) pos++;
          break;
        }
        if (curr === '#') {
          const next = input[pos + 1];
          if (next === '>') break; // preserve #> / #>> operators
          if (next && (isIdentifierContinuation(next) || next === '$' || next === '#')) {
            pos++;
            continue;
          }
          // Allow trailing # (e.g. Oracle SERIAL#).
          pos++;
          break;
        }
        break;
      }
      // Single check after loop instead of N checks for N-character identifiers
      if (pos - start > MAX_IDENTIFIER_LENGTH) {
        const { line: eLine, column: eCol } = lc(start);
        throw new TokenizeError(
          `Identifier exceeds maximum length of ${MAX_IDENTIFIER_LENGTH} characters`,
          start,
          eLine,
          eCol,
        );
      }
      const val = input.slice(start, pos);
      const upper = val.toUpperCase();
      if (isKeyword(val) || additionalKeywords.has(upper)) {
        emit('keyword', val, upper, start);
      } else {
        emit('identifier', val, upper, start);
      }
      continue;
    }

    // Box-drawing and similar decorative glyphs are often pasted into SQL files
    // as result separators. Treat the whole line as a comment to avoid fatal errors.
    if (isBoxDrawingChar(ch)) {
      pos += 1;
      while (pos < len && input[pos] !== '\n') pos++;
      emit('line_comment', input.slice(start, pos), '', start);
      continue;
    }

    // Unknown character
    const codePoint = ch.codePointAt(0) ?? ch.charCodeAt(0);
    const printable =
      codePoint <= 0x1f || codePoint === 0x7f
        ? `U+${codePoint.toString(16).toUpperCase().padStart(4, '0')}`
        : `'${ch}'`;
    const { line: eLine, column: eCol } = lc(start);
    throw new TokenizeError(`Unexpected character ${printable}`, start, eLine, eCol);
  }

  emit('eof', '', '', pos);
  return tokens;
}
