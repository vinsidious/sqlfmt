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

export interface Token {
  type: TokenType;
  value: string;
  // Upper-cased value for keywords
  upper: string;
  position: number;
  line: number;
  column: number;
}

export class TokenizeError extends Error {
  readonly position: number;
  readonly line: number;
  readonly column: number;

  constructor(message: string, position: number, line: number = 1, column: number = 1) {
    super(`${message} at line ${line}, column ${column}`);
    this.name = 'TokenizeError';
    this.position = position;
    this.line = line;
    this.column = column;
  }
}

const IDENT_START_RE = /[\p{L}_]/u;
const IDENT_CONT_RE = /[\p{L}\p{N}_]/u;

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
  const errPos = start - 1;
  if (lineOffsets) {
    const { line, column } = posToLineCol(lineOffsets, errPos);
    throw new TokenizeError('Unterminated string literal', errPos, line, column);
  }
  throw new TokenizeError('Unterminated string literal', errPos);
}

// Precompute line start offsets for O(1) line/column lookup
function buildLineOffsets(input: string): number[] {
  const offsets = [0]; // line 1 starts at offset 0
  for (let i = 0; i < input.length; i++) {
    if (input[i] === '\n') offsets.push(i + 1);
  }
  return offsets;
}

function posToLineCol(lineOffsets: number[], pos: number): { line: number; column: number } {
  // Binary search for the line containing this position
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
 *   literal, quoted identifier, or block comment.
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

  while (pos < len) {
    const start = pos;
    const ch = input[pos];

    // Whitespace
    if (isWhitespaceCode(ch.charCodeAt(0))) {
      while (pos < len && isWhitespaceCode(input.charCodeAt(pos))) pos++;
      tokens.push({ type: 'whitespace', value: input.slice(start, pos), upper: '', position: start, ...lc(start) });
      continue;
    }

    // Line comment: -- ...
    if (ch === '-' && pos + 1 < len && input[pos + 1] === '-') {
      pos += 2;
      while (pos < len && input[pos] !== '\n') pos++;
      // Trim trailing whitespace from line comments
      const commentText = input.slice(start, pos).replace(/\s+$/, '');
      tokens.push({ type: 'line_comment', value: commentText, upper: '', position: start, ...lc(start) });
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
      tokens.push({ type: 'block_comment', value: input.slice(start, pos), upper: '', position: start, ...lc(start) });
      continue;
    }

    // Dollar-quoted strings ($$...$$ or $tag$...$tag$) and positional parameters ($1, $2)
    if (ch === '$') {
      if (isDigit(input[pos + 1])) {
        pos++;
        while (pos < len && isDigit(input[pos])) pos++;
        const val = input.slice(start, pos);
        tokens.push({ type: 'parameter', value: val, upper: val, position: start, ...lc(start) });
        continue;
      }

      const delim = readDollarDelimiter(input, pos);
      if (delim) {
        pos += delim.length;
        const close = input.indexOf(delim, pos);
        if (close === -1) {
          const { line: eLine, column: eCol } = lc(start);
          throw new TokenizeError('Unterminated dollar-quoted string', start, eLine, eCol);
        }
        pos = close + delim.length;
        const val = input.slice(start, pos);
        tokens.push({ type: 'string', value: val, upper: val, position: start, ...lc(start) });
        continue;
      }
    }

    // Prefixed strings: E'...', B'...', X'...'
    if ('EeBbXx'.includes(ch) && input[pos + 1] === "'") {
      const allowBackslashEscapes = ch === 'E' || ch === 'e';
      pos += 2;
      pos = readQuotedString(input, pos, allowBackslashEscapes, lineOffsets);
      const val = input.slice(start, pos);
      tokens.push({ type: 'string', value: val, upper: val, position: start, ...lc(start) });
      continue;
    }

    // String literal: 'text' (with '' escape)
    if (ch === "'") {
      pos++;
      pos = readQuotedString(input, pos, false, lineOffsets);
      const val = input.slice(start, pos);
      tokens.push({ type: 'string', value: val, upper: val, position: start, ...lc(start) });
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
      }
      if (!closed) {
        const { line: eLine, column: eCol } = lc(start);
        throw new TokenizeError('Unterminated quoted identifier', start, eLine, eCol);
      }
      const val = input.slice(start, pos);
      tokens.push({ type: 'identifier', value: val, upper: val, position: start, ...lc(start) });
      continue;
    }

    // Number
    if (isDigit(ch) || (ch === '.' && isDigit(input[pos + 1]))) {
      // Hex numeric literal: 0xFF / 0XFF
      if (ch === '0' && (input[pos + 1] === 'x' || input[pos + 1] === 'X') && isHexDigit(input[pos + 2])) {
        pos += 2;
        while (pos < len && isHexDigit(input[pos])) pos++;
      } else {
        if (ch === '.') {
          pos++;
          while (pos < len && isDigit(input[pos])) pos++;
        } else {
          while (pos < len && isDigit(input[pos])) pos++;
          if (pos < len && input[pos] === '.') {
            pos++;
            while (pos < len && isDigit(input[pos])) pos++;
          }
        }

        // Scientific notation: 1e5, 1.2E-4, .5e+2
        if (pos < len && (input[pos] === 'e' || input[pos] === 'E')) {
          const expStart = pos;
          pos++;
          if (input[pos] === '+' || input[pos] === '-') pos++;
          if (!isDigit(input[pos])) {
            pos = expStart;
          } else {
            while (pos < len && isDigit(input[pos])) pos++;
          }
        }
      }
      const val = input.slice(start, pos);
      tokens.push({ type: 'number', value: val, upper: val, position: start, ...lc(start) });
      continue;
    }

    // Operator scanning — longest match first
    // We handle multi-char operators by checking the current char and looking ahead.

    // :: — PostgreSQL type cast
    if (ch === ':' && pos + 1 < len && input[pos + 1] === ':') {
      pos += 2;
      tokens.push({ type: 'operator', value: '::', upper: '::', position: start, ...lc(start) });
      continue;
    }

    // ! operators: !~* then !~ then !=
    if (ch === '!') {
      if (pos + 2 < len && input[pos + 1] === '~' && input[pos + 2] === '*') {
        pos += 3;
        tokens.push({ type: 'operator', value: '!~*', upper: '!~*', position: start, ...lc(start) });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '~') {
        pos += 2;
        tokens.push({ type: 'operator', value: '!~', upper: '!~', position: start, ...lc(start) });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '=') {
        pos += 2;
        tokens.push({ type: 'operator', value: '!=', upper: '!=', position: start, ...lc(start) });
        continue;
      }
      // bare ! (not standard SQL but consume it)
      pos++;
      tokens.push({ type: 'operator', value: '!', upper: '!', position: start, ...lc(start) });
      continue;
    }

    // < operators: <@ then <> then << then <= then <
    if (ch === '<') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '@') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<@', upper: '<@', position: start, ...lc(start) });
          continue;
        }
        if (next === '>') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<>', upper: '<>', position: start, ...lc(start) });
          continue;
        }
        if (next === '<') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<<', upper: '<<', position: start, ...lc(start) });
          continue;
        }
        if (next === '=') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<=', upper: '<=', position: start, ...lc(start) });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '<', upper: '<', position: start, ...lc(start) });
      continue;
    }

    // > operators: >= then >> then >
    if (ch === '>') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '=') {
          pos += 2;
          tokens.push({ type: 'operator', value: '>=', upper: '>=', position: start, ...lc(start) });
          continue;
        }
        if (next === '>') {
          pos += 2;
          tokens.push({ type: 'operator', value: '>>', upper: '>>', position: start, ...lc(start) });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '>', upper: '>', position: start, ...lc(start) });
      continue;
    }

    // - operators: ->> then -> then - (line comment already handled above)
    if (ch === '-') {
      if (pos + 2 < len && input[pos + 1] === '>' && input[pos + 2] === '>') {
        pos += 3;
        tokens.push({ type: 'operator', value: '->>', upper: '->>', position: start, ...lc(start) });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '>') {
        pos += 2;
        tokens.push({ type: 'operator', value: '->', upper: '->', position: start, ...lc(start) });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '-', upper: '-', position: start, ...lc(start) });
      continue;
    }

    // # operators: #>> then #> then #
    if (ch === '#') {
      if (pos + 2 < len && input[pos + 1] === '>' && input[pos + 2] === '>') {
        pos += 3;
        tokens.push({ type: 'operator', value: '#>>', upper: '#>>', position: start, ...lc(start) });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '>') {
        pos += 2;
        tokens.push({ type: 'operator', value: '#>', upper: '#>', position: start, ...lc(start) });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '#', upper: '#', position: start, ...lc(start) });
      continue;
    }

    // @ operators: @> then @? then @@ then bare @
    if (ch === '@') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '>') {
          pos += 2;
          tokens.push({ type: 'operator', value: '@>', upper: '@>', position: start, ...lc(start) });
          continue;
        }
        if (next === '?') {
          pos += 2;
          tokens.push({ type: 'operator', value: '@?', upper: '@?', position: start, ...lc(start) });
          continue;
        }
        if (next === '@') {
          pos += 2;
          tokens.push({ type: 'operator', value: '@@', upper: '@@', position: start, ...lc(start) });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '@', upper: '@', position: start, ...lc(start) });
      continue;
    }

    // ? operators: ?| then ?& then ?
    if (ch === '?') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '|') {
          pos += 2;
          tokens.push({ type: 'operator', value: '?|', upper: '?|', position: start, ...lc(start) });
          continue;
        }
        if (next === '&') {
          pos += 2;
          tokens.push({ type: 'operator', value: '?&', upper: '?&', position: start, ...lc(start) });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '?', upper: '?', position: start, ...lc(start) });
      continue;
    }

    // ~ operators: ~* then ~
    if (ch === '~') {
      if (pos + 1 < len && input[pos + 1] === '*') {
        pos += 2;
        tokens.push({ type: 'operator', value: '~*', upper: '~*', position: start, ...lc(start) });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '~', upper: '~', position: start, ...lc(start) });
      continue;
    }

    // & operators: && then &
    if (ch === '&') {
      if (pos + 1 < len && input[pos + 1] === '&') {
        pos += 2;
        tokens.push({ type: 'operator', value: '&&', upper: '&&', position: start, ...lc(start) });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '&', upper: '&', position: start, ...lc(start) });
      continue;
    }

    // | operators: || then |
    if (ch === '|') {
      if (pos + 1 < len && input[pos + 1] === '|') {
        pos += 2;
        tokens.push({ type: 'operator', value: '||', upper: '||', position: start, ...lc(start) });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '|', upper: '|', position: start, ...lc(start) });
      continue;
    }

    // Remaining simple single-char operators: = + * /
    if ('=+*/'.includes(ch)) {
      pos++;
      tokens.push({ type: 'operator', value: ch, upper: ch, position: start, ...lc(start) });
      continue;
    }

    // Punctuation (including [ and ])
    if ('(),;.[]'.includes(ch)) {
      pos++;
      tokens.push({ type: 'punctuation', value: ch, upper: ch, position: start, ...lc(start) });
      continue;
    }

    // Identifier or keyword
    if (isIdentifierStart(ch)) {
      while (pos < len && isIdentifierContinuation(input[pos])) pos++;
      const val = input.slice(start, pos);
      const upper = val.toUpperCase();
      if (isKeyword(val)) {
        tokens.push({ type: 'keyword', value: val, upper, position: start, ...lc(start) });
      } else {
        tokens.push({ type: 'identifier', value: val, upper, position: start, ...lc(start) });
      }
      continue;
    }

    // Unknown character — just consume it
    pos++;
    tokens.push({ type: 'identifier', value: ch, upper: ch, position: start, ...lc(start) });
  }

  tokens.push({ type: 'eof', value: '', upper: '', position: pos, ...lc(pos) });
  return tokens;
}
