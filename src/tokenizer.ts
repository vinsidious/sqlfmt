import { isKeyword } from './keywords';

export type TokenType =
  | 'keyword'
  | 'identifier'
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
  // Number of blank lines (newlines - 1) preceding this token; 0 = none
  blankLinesBefore?: number;
}

export function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;
  const len = input.length;

  while (pos < len) {
    const start = pos;
    const ch = input[pos];

    // Whitespace
    if (/\s/.test(ch)) {
      while (pos < len && /\s/.test(input[pos])) pos++;
      tokens.push({ type: 'whitespace', value: input.slice(start, pos), upper: '', position: start });
      continue;
    }

    // Line comment: -- ...
    if (ch === '-' && pos + 1 < len && input[pos + 1] === '-') {
      pos += 2;
      while (pos < len && input[pos] !== '\n') pos++;
      // Trim trailing whitespace from line comments
      const commentText = input.slice(start, pos).replace(/\s+$/, '');
      tokens.push({ type: 'line_comment', value: commentText, upper: '', position: start });
      continue;
    }

    // Block comment: /* ... */
    if (ch === '/' && pos + 1 < len && input[pos + 1] === '*') {
      pos += 2;
      while (pos < len && !(input[pos] === '*' && pos + 1 < len && input[pos + 1] === '/')) pos++;
      if (pos < len) pos += 2; // skip */
      tokens.push({ type: 'block_comment', value: input.slice(start, pos), upper: '', position: start });
      continue;
    }

    // String literal: 'text' (with '' escape)
    if (ch === "'") {
      pos++;
      while (pos < len) {
        if (input[pos] === "'" && pos + 1 < len && input[pos + 1] === "'") {
          pos += 2; // escaped quote
        } else if (input[pos] === "'") {
          pos++;
          break;
        } else {
          pos++;
        }
      }
      const val = input.slice(start, pos);
      tokens.push({ type: 'string', value: val, upper: val, position: start });
      continue;
    }

    // Quoted identifier: "identifier"
    if (ch === '"') {
      pos++;
      while (pos < len) {
        if (input[pos] === '"' && pos + 1 < len && input[pos + 1] === '"') {
          pos += 2; // escaped double quote inside identifier
        } else if (input[pos] === '"') {
          pos++;
          break;
        } else {
          pos++;
        }
      }
      const val = input.slice(start, pos);
      tokens.push({ type: 'identifier', value: val, upper: val, position: start });
      continue;
    }

    // Number
    if (/[0-9]/.test(ch) || (ch === '.' && pos + 1 < len && /[0-9]/.test(input[pos + 1]))) {
      while (pos < len && /[0-9]/.test(input[pos])) pos++;
      if (pos < len && input[pos] === '.') {
        pos++;
        while (pos < len && /[0-9]/.test(input[pos])) pos++;
      }
      const val = input.slice(start, pos);
      tokens.push({ type: 'number', value: val, upper: val, position: start });
      continue;
    }

    // Operator scanning — longest match first
    // We handle multi-char operators by checking the current char and looking ahead.

    // :: — PostgreSQL type cast
    if (ch === ':' && pos + 1 < len && input[pos + 1] === ':') {
      pos += 2;
      tokens.push({ type: 'operator', value: '::', upper: '::', position: start });
      continue;
    }

    // ! operators: !~* then !~ then !=
    if (ch === '!') {
      if (pos + 2 < len && input[pos + 1] === '~' && input[pos + 2] === '*') {
        pos += 3;
        tokens.push({ type: 'operator', value: '!~*', upper: '!~*', position: start });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '~') {
        pos += 2;
        tokens.push({ type: 'operator', value: '!~', upper: '!~', position: start });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '=') {
        pos += 2;
        tokens.push({ type: 'operator', value: '!=', upper: '!=', position: start });
        continue;
      }
      // bare ! (not standard SQL but consume it)
      pos++;
      tokens.push({ type: 'operator', value: '!', upper: '!', position: start });
      continue;
    }

    // < operators: <@ then <> then << then <= then <
    if (ch === '<') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '@') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<@', upper: '<@', position: start });
          continue;
        }
        if (next === '>') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<>', upper: '<>', position: start });
          continue;
        }
        if (next === '<') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<<', upper: '<<', position: start });
          continue;
        }
        if (next === '=') {
          pos += 2;
          tokens.push({ type: 'operator', value: '<=', upper: '<=', position: start });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '<', upper: '<', position: start });
      continue;
    }

    // > operators: >= then >> then >
    if (ch === '>') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '=') {
          pos += 2;
          tokens.push({ type: 'operator', value: '>=', upper: '>=', position: start });
          continue;
        }
        if (next === '>') {
          pos += 2;
          tokens.push({ type: 'operator', value: '>>', upper: '>>', position: start });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '>', upper: '>', position: start });
      continue;
    }

    // - operators: ->> then -> then - (line comment already handled above)
    if (ch === '-') {
      if (pos + 2 < len && input[pos + 1] === '>' && input[pos + 2] === '>') {
        pos += 3;
        tokens.push({ type: 'operator', value: '->>', upper: '->>', position: start });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '>') {
        pos += 2;
        tokens.push({ type: 'operator', value: '->', upper: '->', position: start });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '-', upper: '-', position: start });
      continue;
    }

    // # operators: #>> then #> then #
    if (ch === '#') {
      if (pos + 2 < len && input[pos + 1] === '>' && input[pos + 2] === '>') {
        pos += 3;
        tokens.push({ type: 'operator', value: '#>>', upper: '#>>', position: start });
        continue;
      }
      if (pos + 1 < len && input[pos + 1] === '>') {
        pos += 2;
        tokens.push({ type: 'operator', value: '#>', upper: '#>', position: start });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '#', upper: '#', position: start });
      continue;
    }

    // @ operators: @> then @? then @@ then bare @
    if (ch === '@') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '>') {
          pos += 2;
          tokens.push({ type: 'operator', value: '@>', upper: '@>', position: start });
          continue;
        }
        if (next === '?') {
          pos += 2;
          tokens.push({ type: 'operator', value: '@?', upper: '@?', position: start });
          continue;
        }
        if (next === '@') {
          pos += 2;
          tokens.push({ type: 'operator', value: '@@', upper: '@@', position: start });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '@', upper: '@', position: start });
      continue;
    }

    // ? operators: ?| then ?& then ?
    if (ch === '?') {
      if (pos + 1 < len) {
        const next = input[pos + 1];
        if (next === '|') {
          pos += 2;
          tokens.push({ type: 'operator', value: '?|', upper: '?|', position: start });
          continue;
        }
        if (next === '&') {
          pos += 2;
          tokens.push({ type: 'operator', value: '?&', upper: '?&', position: start });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: '?', upper: '?', position: start });
      continue;
    }

    // ~ operators: ~* then ~
    if (ch === '~') {
      if (pos + 1 < len && input[pos + 1] === '*') {
        pos += 2;
        tokens.push({ type: 'operator', value: '~*', upper: '~*', position: start });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '~', upper: '~', position: start });
      continue;
    }

    // & operators: && then &
    if (ch === '&') {
      if (pos + 1 < len && input[pos + 1] === '&') {
        pos += 2;
        tokens.push({ type: 'operator', value: '&&', upper: '&&', position: start });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '&', upper: '&', position: start });
      continue;
    }

    // | operators: || then |
    if (ch === '|') {
      if (pos + 1 < len && input[pos + 1] === '|') {
        pos += 2;
        tokens.push({ type: 'operator', value: '||', upper: '||', position: start });
        continue;
      }
      pos++;
      tokens.push({ type: 'operator', value: '|', upper: '|', position: start });
      continue;
    }

    // Remaining simple single-char operators: = + * /
    if ('=+*/'.includes(ch)) {
      pos++;
      tokens.push({ type: 'operator', value: ch, upper: ch, position: start });
      continue;
    }

    // Punctuation (including [ and ])
    if ('(),;.[]'.includes(ch)) {
      pos++;
      tokens.push({ type: 'punctuation', value: ch, upper: ch, position: start });
      continue;
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(ch)) {
      while (pos < len && /[a-zA-Z0-9_]/.test(input[pos])) pos++;
      const val = input.slice(start, pos);
      const upper = val.toUpperCase();
      if (isKeyword(val)) {
        tokens.push({ type: 'keyword', value: val, upper, position: start });
      } else {
        tokens.push({ type: 'identifier', value: val, upper, position: start });
      }
      continue;
    }

    // Unknown character — just consume it
    pos++;
    tokens.push({ type: 'identifier', value: ch, upper: ch, position: start });
  }

  tokens.push({ type: 'eof', value: '', upper: '', position: pos });
  return tokens;
}

// Filter out whitespace tokens for parsing, but keep comments
export function filterTokens(tokens: Token[]): Token[] {
  return tokens.filter(t => t.type !== 'whitespace');
}
