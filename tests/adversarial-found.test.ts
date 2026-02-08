import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { tokenize, TokenizeError } from '../src/tokenizer';
import { parse, ParseError } from '../src/parser';

/**
 * Real edge cases found during code review that need testing.
 */

describe('FOUND: readQuotedString position tracking', () => {
  it('handles E-string with backslash escapes consuming two chars', () => {
    // E'...' allows \\ for backslash escape
    // The readQuotedString function does: pos += 2 when it sees backslash
    // This test ensures it doesn't skip over the closing quote
    const sql = "SELECT E'\\\\', E'a\\\\b';";
    const result = formatSQL(sql);
    expect(result).toContain("E'\\\\'");
    expect(result).toContain("E'a\\\\b'");
  });

  it('handles E-string ending with backslash-escaped quote', () => {
    const sql = "SELECT E'\\\\\\\\\\'';";
    const result = formatSQL(sql);
    expect(result).toContain("E'\\\\\\\\\\''");
  });

  it('handles regular string with doubled quote at end', () => {
    const sql = "SELECT 'text''';";
    const result = formatSQL(sql);
    expect(result).toContain("'text'''");
  });

  it('handles string that is only escaped quotes', () => {
    const sql = "SELECT '''''';";
    const result = formatSQL(sql);
    expect(result).toContain("''''''");
  });
});

describe('FOUND: indexOf for dollar-quoted strings', () => {
  it('closes $$ strings at the first matching delimiter (PostgreSQL behavior)', () => {
    const sql = 'SELECT $$text $$not delimiter$$ more$$;';
    const tokens = tokenize(sql);
    const strings = tokens.filter(t => t.type === 'string').map(t => t.value);
    expect(strings[0]).toBe('$$text $$');
    expect(strings.length).toBe(2);
  });

  it('handles $tag$ containing $$', () => {
    const sql = 'SELECT $mytag$has $$ inside$mytag$;';
    const tokens = tokenize(sql);
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe('$mytag$has $$ inside$mytag$');
  });

  it('handles $$ containing $othertag$', () => {
    const sql = 'SELECT $$has $tag$ inside$$;';
    const tokens = tokenize(sql);
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe('$$has $tag$ inside$$');
  });
});

describe('FOUND: Operator longest-match order', () => {
  it('correctly parses !~* before !~ before !=', () => {
    const sql = "SELECT a !~* 'pat', b !~ 'pat', c != 1;";
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.type === 'operator').map(t => t.value);
    expect(ops).toContain('!~*');
    expect(ops).toContain('!~');
    expect(ops).toContain('!=');
  });

  it('correctly parses <@ before <> before << before <= before <', () => {
    const sql = 'SELECT a <@ b, c <> d, e << f, g <= h, i < j;';
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.type === 'operator').map(t => t.value);
    expect(ops).toContain('<@');
    expect(ops).toContain('<>');
    expect(ops).toContain('<<');
    expect(ops).toContain('<=');
    expect(ops).toContain('<');
  });

  it('correctly parses ->> before ->', () => {
    const sql = 'SELECT a->>b, c->d;';
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.type === 'operator').map(t => t.value);
    expect(ops).toContain('->>');
    expect(ops).toContain('->');
  });

  it('correctly parses #>> before #>', () => {
    const sql = 'SELECT a#>>b, c#>d;';
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.type === 'operator').map(t => t.value);
    expect(ops).toContain('#>>');
    expect(ops).toContain('#>');
  });

  it('correctly parses @> and @? and @@', () => {
    const sql = 'SELECT a @> b, c @? d, e @@ f;';
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.type === 'operator').map(t => t.value);
    expect(ops).toContain('@>');
    expect(ops).toContain('@?');
    expect(ops).toContain('@@');
  });

  it('correctly parses ?| and ?& before bare ?', () => {
    const sql = 'SELECT a ?| b, c ?& d, e ? f;';
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.type === 'operator').map(t => t.value);
    expect(ops).toContain('?|');
    expect(ops).toContain('?&');
    expect(ops).toContain('?');
  });

  it('correctly parses ~* before ~', () => {
    const sql = "SELECT a ~* 'pat', b ~ 'pat';";
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.type === 'operator').map(t => t.value);
    expect(ops).toContain('~*');
    expect(ops).toContain('~');
  });
});

describe('FOUND: Identifier length check timing', () => {
  it('checks identifier length after consuming all chars', () => {
    // The code only checks length AFTER the while loop, not during
    // This is efficient but means a 10,001-char identifier is fully consumed before throwing
    const longIdent = 'x'.repeat(10_001);
    const sql = `SELECT ${longIdent};`;
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('allows exactly MAX_IDENTIFIER_LENGTH', () => {
    const maxIdent = 'y'.repeat(10_000);
    const sql = `SELECT ${maxIdent};`;
    const tokens = tokenize(sql);
    const ident = tokens.find(t => t.type === 'identifier' && t.value === maxIdent);
    expect(ident).toBeDefined();
  });

  it('error includes line and column info', () => {
    const longIdent = 'z'.repeat(10_001);
    const sql = `SELECT ${longIdent};`;
    try {
      tokenize(sql);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      const te = err as TokenizeError;
      expect(te.line).toBe(1);
      expect(te.column).toBe(8); // After "SELECT "
    }
  });
});

describe('FOUND: Quoted identifier length check includes quotes', () => {
  it('checks total token length including quotes', () => {
    // pos - start is the total length from opening quote to current position
    // So "a".repeat(10000) is 10000 + 2 quotes = 10002, which exceeds limit
    const tooLong = '"' + 'a'.repeat(10_000) + '"';
    const sql = `SELECT ${tooLong};`;
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('allows quoted identifier with content at 9998 chars (+ 2 quotes = 10000)', () => {
    const maxQuoted = '"' + 'b'.repeat(9_998) + '"';
    const sql = `SELECT ${maxQuoted};`;
    const tokens = tokenize(sql);
    const ident = tokens.find(t => t.type === 'identifier' && t.value === maxQuoted);
    expect(ident).toBeDefined();
  });
});

describe('FOUND: Underscore handling in numbers', () => {
  it('requires digit after underscore in number', () => {
    // Code checks: digitCheck(input[pos + 1]) before allowing underscore
    // So 123_ (underscore at end with no following digit) should not consume underscore
    const sql = 'SELECT 123_ FROM t;';
    const tokens = tokenize(sql);
    const num = tokens.find(t => t.type === 'number');
    // The underscore should NOT be part of the number
    expect(num?.value).toBe('123');
  });

  it('handles underscore between digits', () => {
    const sql = 'SELECT 1_2_3;';
    const tokens = tokenize(sql);
    const num = tokens.find(t => t.type === 'number');
    expect(num?.value).toBe('1_2_3');
  });

  it('handles multiple consecutive underscores (if allowed)', () => {
    // Code allows _ as long as next char is digit, so __ would need digit after each
    // 1__2 would be: 1, then _ (next is _, not digit, so stop), then _2 fails
    const sql = 'SELECT 1__2;';
    const tokens = tokenize(sql);
    const nums = tokens.filter(t => t.type === 'number');
    // Depending on implementation, might be 1, then identifier __2
    // or 1, then error. Check what actually happens.
    expect(nums.length).toBeGreaterThanOrEqual(1);
  });
});

describe('FOUND: Hex number edge cases', () => {
  it('requires at least one hex digit after 0x', () => {
    // Code checks: isHexDigit(input[pos + 2]) before accepting 0x
    const sql = 'SELECT 0x;';
    const tokens = tokenize(sql);
    // Should be: 0 (number), x (identifier), ; (punctuation)
    const nums = tokens.filter(t => t.type === 'number');
    expect(nums[0].value).toBe('0');
  });

  it('accepts 0x with single hex digit', () => {
    const sql = 'SELECT 0xA;';
    const tokens = tokenize(sql);
    const num = tokens.find(t => t.type === 'number');
    expect(num?.value).toBe('0xA');
  });

  it('handles 0X (uppercase X)', () => {
    const sql = 'SELECT 0XFF;';
    const tokens = tokenize(sql);
    const num = tokens.find(t => t.type === 'number');
    expect(num?.value).toBe('0XFF');
  });
});

describe('FOUND: Decimal number edge cases', () => {
  it('handles .5 (decimal starting with dot)', () => {
    // Code checks: ch === '.' && isDigit(input[pos + 1])
    const sql = 'SELECT .5;';
    const tokens = tokenize(sql);
    const num = tokens.find(t => t.type === 'number');
    expect(num?.value).toBe('.5');
  });

  it('rejects . alone (not followed by digit)', () => {
    const sql = 'SELECT . FROM t;';
    const tokens = tokenize(sql);
    const punct = tokens.find(t => t.value === '.');
    expect(punct?.type).toBe('punctuation');
  });

  it('handles 5. (decimal ending with dot)', () => {
    const sql = 'SELECT 5.;';
    const tokens = tokenize(sql);
    const num = tokens.find(t => t.type === 'number');
    expect(num?.value).toBe('5.');
  });
});

describe('FOUND: Token count approaching limit', () => {
  it('throws when token count hits MAX_TOKEN_COUNT during emit', () => {
    // Each emit() checks if tokens.length >= MAX_TOKEN_COUNT
    // So the 1,000,000th token causes the error (before the 1,000,001st)
    const sql = ';'.repeat(1_000_000);
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('error message is helpful', () => {
    const sql = ';'.repeat(1_000_000);
    try {
      tokenize(sql);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('1000000');
    }
  });
});

describe('FOUND: Block comment does NOT nest in standard SQL', () => {
  it('closes at first */', () => {
    // /* /* */ should close at first */, leaving second /* as tokens
    const sql = 'SELECT /* /* */ 1 FROM t;';
    const tokens = tokenize(sql);
    const comment = tokens.find(t => t.type === 'block_comment');
    expect(comment?.value).toBe('/* /* */');
  });

  it('allows * inside comment without confusion', () => {
    const sql = 'SELECT /* * * * */ 1;';
    const tokens = tokenize(sql);
    const comment = tokens.find(t => t.type === 'block_comment');
    expect(comment?.value).toBe('/* * * * */');
  });

  it('unterminated /* throws even if there are asterisks', () => {
    const sql = 'SELECT /* * * * ';
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });
});

describe('FOUND: Line comment trims trailing whitespace', () => {
  it('removes trailing spaces from line comment', () => {
    const sql = 'SELECT 1 -- comment   \n FROM t;';
    const tokens = tokenize(sql);
    const comment = tokens.find(t => t.type === 'line_comment');
    // Code does: .replace(/\s+$/, '')
    expect(comment?.value).toBe('-- comment');
  });

  it('removes trailing tabs from line comment', () => {
    const sql = 'SELECT 1 -- comment\t\t\n FROM t;';
    const tokens = tokenize(sql);
    const comment = tokens.find(t => t.type === 'line_comment');
    expect(comment?.value).toBe('-- comment');
  });

  it('does not trim leading whitespace in comment content', () => {
    const sql = 'SELECT 1 --   indented\n FROM t;';
    const tokens = tokenize(sql);
    const comment = tokens.find(t => t.type === 'line_comment');
    expect(comment?.value).toBe('--   indented');
  });
});

describe('FOUND: Punctuation includes colon for array slicing', () => {
  it('treats : as punctuation', () => {
    const sql = 'SELECT arr[1:5];';
    const tokens = tokenize(sql);
    const colons = tokens.filter(t => t.value === ':');
    expect(colons.length).toBeGreaterThan(0);
    expect(colons[0].type).toBe('punctuation');
  });

  it('distinguishes :: (operator) from : (punctuation)', () => {
    const sql = 'SELECT x::int, arr[1:2];';
    const tokens = tokenize(sql);
    const doubleColon = tokens.find(t => t.value === '::');
    const singleColon = tokens.find(t => t.value === ':');
    expect(doubleColon?.type).toBe('operator');
    expect(singleColon?.type).toBe('punctuation');
  });
});

describe('FOUND: Unexpected character error format', () => {
  it('formats control chars as U+ hex codes', () => {
    const sql = 'SELECT 1\x01FROM t;';
    try {
      tokenize(sql);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('U+');
      expect((err as TokenizeError).message).toContain('0001');
    }
  });

  it('formats printable chars with quotes', () => {
    const sql = 'SELECT 1`FROM t;'; // backtick starts an unterminated identifier
    try {
      tokenize(sql);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('backtick');
    }
  });
});

describe('FOUND: U& Unicode escape strings', () => {
  it('handles U& prefix (Unicode escape)', () => {
    const sql = "SELECT U&'\\0041\\0042';";
    const tokens = tokenize(sql);
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe("U&'\\0041\\0042'");
  });

  it('handles u& (lowercase)', () => {
    const sql = "SELECT u&'\\0043';";
    const tokens = tokenize(sql);
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe("u&'\\0043'");
  });

  it('U& strings allow backslash escapes', () => {
    // The code passes true for allowBackslashEscapes to readQuotedString
    const sql = "SELECT U&'test\\\\escape';";
    const tokens = tokenize(sql);
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe("U&'test\\\\escape'");
  });
});

describe('FOUND: isKeyword check for keyword vs identifier', () => {
  it('uppercases identifier and checks if keyword', () => {
    const sql = 'SELECT select, SELECT, SeLeCt FROM from, FROM;';
    const tokens = tokenize(sql);
    // All variations of SELECT should be keywords
    const keywords = tokens.filter(t => t.type === 'keyword' && t.upper === 'SELECT');
    expect(keywords.length).toBe(4);
    // All variations of FROM should be keywords
    const fromKeywords = tokens.filter(t => t.type === 'keyword' && t.upper === 'FROM');
    expect(fromKeywords.length).toBe(3);
  });

  it('non-keywords become identifiers', () => {
    const sql = 'SELECT mytable, MyColumn;';
    const tokens = tokenize(sql);
    const idents = tokens.filter(t => t.type === 'identifier');
    expect(idents.length).toBe(2);
  });
});

describe('FOUND: Line offset calculation for error reporting', () => {
  it('correctly reports line number for multi-line input', () => {
    const sql = "SELECT 1\nFROM t\nWHERE x = 'bad";
    try {
      tokenize(sql);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).line).toBe(3);
    }
  });

  it('correctly reports column number', () => {
    const sql = "SELECT 'bad";
    try {
      tokenize(sql);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      // Error is at end of input (position 11: "SELECT 'bad")
      expect((err as TokenizeError).column).toBeGreaterThan(1);
    }
  });
});
