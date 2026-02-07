import { isKeyword } from './keywords';

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

/**
 * Thrown when the tokenizer encounters invalid input such as an unterminated
 * string literal, quoted identifier, or block comment.
 *
 * Carries positional information so callers can produce useful diagnostics.
 *
 * @example
 * ```typescript
 * import { tokenize, TokenizeError } from '@vcoppola/sqlfmt';
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
// \p{L} = Unicode letter, \p{N} = Unicode number (digits in any script)
const IDENT_CONT_RE = /[\p{L}\p{N}_]/u;

/** Maximum number of tokens before the tokenizer aborts to prevent DoS. */
const MAX_TOKEN_COUNT = 1_000_000;

/** Maximum length for an unquoted identifier. */
const MAX_IDENTIFIER_LENGTH = 10_000;

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
      pos += 2;
      continue;
    }
    if (ch === "'" && pos + 1 < input.length && input[pos + 1] === "'") {
      pos += 2;
      continue;
    }
    if (ch === "'") {
      return pos + 1;
    }
    pos++;
  }
  const errPos = pos;
  if (lineOffsets) {
    const { line, column } = posToLineCol(lineOffsets, errPos);
    throw new TokenizeError('Unterminated string literal', errPos, line, column);
  }
  throw new TokenizeError('Unterminated string literal', errPos);
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
 * import { tokenize } from '@vcoppola/sqlfmt';
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
export function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;
  const len = input.length;
  const lineOffsets = buildLineOffsets(input);

  function lc(p: number) {
    const { line, column } = posToLineCol(lineOffsets, p);
    return { line, column };
  }

  /** Build a Token object and push it onto the output array. */
  function emit(type: TokenType, value: string, upper: string, position: number): void {
    if (tokens.length >= MAX_TOKEN_COUNT) {
      const { line, column } = lc(position);
      throw new TokenizeError(
        `Token count exceeds maximum of ${MAX_TOKEN_COUNT}`,
        position,
        line,
        column,
      );
    }
    const { line, column } = lc(position);
    tokens.push({ type, value, upper, position, line, column });
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

    // Line comment: -- ...
    if (ch === '-' && pos + 1 < len && input[pos + 1] === '-') {
      pos += 2;
      while (pos < len && input[pos] !== '\n') pos++;
      // Trim trailing whitespace from line comments
      const commentText = input.slice(start, pos).replace(/\s+$/, '');
      emit('line_comment', commentText, '', start);
      continue;
    }

    // Block comment: /* ... */
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
        pos += delim.length;
        // Per PostgreSQL spec, dollar-quoted strings cannot nest with the same
        // delimiter tag. Using indexOf to find the closing delimiter is correct.
        // Different tags (e.g., $outer$ vs $inner$) allow pseudo-nesting because
        // the inner delimiters are just literal text within the outer string.
        const close = input.indexOf(delim, pos);
        if (close === -1) {
          const { line: eLine, column: eCol } = lc(start);
          throw new TokenizeError(
            `Unterminated dollar-quoted string (expected closing ${delim})`,
            start,
            eLine,
            eCol,
          );
        }
        pos = close + delim.length;
        const val = input.slice(start, pos);
        emit('string', val, val, start);
        continue;
      }
    }

    // Unicode-escape prefixed strings: U&'...'
    if ((ch === 'U' || ch === 'u') && input[pos + 1] === '&' && input[pos + 2] === "'") {
      pos += 3;
      pos = readQuotedString(input, pos, true, lineOffsets);
      const val = input.slice(start, pos);
      emit('string', val, val, start);
      continue;
    }

    // Prefixed strings: E'...', B'...', X'...'
    if ('EeBbXx'.includes(ch) && input[pos + 1] === "'") {
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

    // < operators: <@ then <> then << then <= then <
    if (ch === '<') {
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
    if ('(),;.[]:'.includes(ch)) {
      pos++;
      emit('punctuation', ch, ch, start);
      continue;
    }

    // Identifier or keyword
    if (isIdentifierStart(ch)) {
      while (pos < len && isIdentifierContinuation(input[pos])) {
        pos++;
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
      if (isKeyword(val)) {
        emit('keyword', val, upper, start);
      } else {
        emit('identifier', val, upper, start);
      }
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
