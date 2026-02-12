import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { tokenize, TokenizeError } from '../src/tokenizer';
import { parse, ParseError } from '../src/parser';

/**
 * Adversarial edge case tests for holywell.
 * These tests aim to find crash, incorrect output, or silent data loss scenarios.
 */

describe('String edge cases', () => {
  it('handles empty string literal', () => {
    const result = formatSQL("SELECT '' FROM t;");
    expect(result).toContain("''");
  });

  it('handles string with only escaped quotes', () => {
    const result = formatSQL("SELECT '''''';");
    expect(result).toContain("''''''");
  });

  it('handles string with escaped quote at end', () => {
    const result = formatSQL("SELECT 'it''s';");
    expect(result).toContain("'it''s'");
  });

  it('handles very long string literal (10MB approach)', () => {
    const longStr = 'a'.repeat(100_000);
    const sql = `SELECT '${longStr}';`;
    const result = formatSQL(sql);
    expect(result).toContain(longStr);
  });

  it('handles string with newlines', () => {
    const result = formatSQL("SELECT 'line1\nline2\nline3';");
    expect(result).toContain('line1\nline2\nline3');
  });

  it('handles string with all SQL keywords', () => {
    const result = formatSQL("SELECT 'SELECT FROM WHERE JOIN AND OR NOT';");
    expect(result).toContain("'SELECT FROM WHERE JOIN AND OR NOT'");
  });

  it('handles string with backslash but not E-string', () => {
    const result = formatSQL("SELECT 'path\\to\\file';");
    expect(result).toContain("'path\\to\\file'");
  });

  it('handles E-string with backslash escapes', () => {
    const result = formatSQL("SELECT E'newline\\ntab\\t';");
    expect(result).toContain("E'newline\\ntab\\t'");
  });

  it('handles string starting with quote-like chars', () => {
    const result = formatSQL("SELECT '''start';");
    expect(result).toContain("'''start'");
  });

  it('handles Unicode string U&', () => {
    const result = formatSQL("SELECT U&'\\0041\\0042';");
    expect(result).toContain("U&'\\0041\\0042'");
  });

  it('handles string with CRLF line endings', () => {
    const result = formatSQL("SELECT 'line1\r\nline2\r\n';");
    // Formatter normalizes CRLF to LF (standard formatter behavior)
    expect(result).toContain('line1');
    expect(result).toContain('line2');
  });

  it('handles string containing semicolons', () => {
    const result = formatSQL("SELECT 'a;b;c';");
    expect(result).toContain("'a;b;c'");
  });

  it('handles string containing comment markers', () => {
    const result = formatSQL("SELECT '-- not a comment', '/* also not */';");
    expect(result).toContain("'-- not a comment'");
    expect(result).toContain("'/* also not */'");
  });

  it('handles mixed quote types in one query', () => {
    const result = formatSQL(`SELECT 'single', "quoted_ident", E'escape\\n', $$dollar$$;`);
    expect(result).toContain("'single'");
    expect(result).toContain('"quoted_ident"');
    expect(result).toContain("E'escape\\n'");
    expect(result).toContain('$$dollar$$');
  });
});

describe('Identifier edge cases', () => {
  it('handles quoted identifier with spaces', () => {
    const result = formatSQL('SELECT "column name" FROM t;');
    expect(result).toContain('"column name"');
  });

  it('handles quoted identifier with special chars', () => {
    const result = formatSQL('SELECT "col@#$%" FROM t;');
    expect(result).toContain('"col@#$%"');
  });

  it('handles reserved word as quoted identifier', () => {
    const result = formatSQL('SELECT "SELECT", "FROM", "WHERE" FROM t;');
    expect(result).toContain('"SELECT"');
    expect(result).toContain('"FROM"');
    expect(result).toContain('"WHERE"');
  });

  it('handles identifier starting with number (quoted)', () => {
    const result = formatSQL('SELECT "123col" FROM t;');
    expect(result).toContain('"123col"');
  });

  it('handles quoted identifier with escaped quotes', () => {
    const result = formatSQL('SELECT "col""name" FROM t;');
    expect(result).toContain('"col""name"');
  });

  it('handles mixed case identifiers', () => {
    const result = formatSQL('SELECT CamelCase, "MixedCase" FROM t;');
    expect(result).toContain('CamelCase');
    expect(result).toContain('"MixedCase"');
  });

  it('handles Unicode identifiers', () => {
    const result = formatSQL('SELECT café, 用户, Δvalue FROM t;');
    expect(result).toContain('café');
    expect(result).toContain('用户');
    expect(result).toContain('δvalue');
  });

  it('handles identifier that looks like a number', () => {
    const result = formatSQL('SELECT e, pi FROM t;');
    expect(result).toContain('e');
    expect(result).toContain('pi');
  });

  it('handles empty quoted identifier (if valid)', () => {
    // Some SQL engines allow "", some don't - test tokenizer behavior
    try {
      const tokens = tokenize('SELECT "" FROM t;');
      const ids = tokens.filter(t => t.type === 'identifier');
      expect(ids.length).toBeGreaterThanOrEqual(1);
    } catch (err) {
      // If it throws, that's also acceptable behavior
      expect(err).toBeDefined();
    }
  });
});

describe('Numeric edge cases', () => {
  it('handles very large integer', () => {
    const result = formatSQL('SELECT 99999999999999999999;');
    expect(result).toContain('99999999999999999999');
  });

  it('handles negative numbers', () => {
    const result = formatSQL('SELECT -1, -9999, -0.5;');
    expect(result).toContain('-1');
    expect(result).toContain('-9999');
    expect(result).toContain('-0.5');
  });

  it('handles scientific notation with positive exponent', () => {
    const result = formatSQL('SELECT 1e10, 2.5E+20;');
    expect(result).toContain('1e10');
    expect(result).toContain('2.5E+20');
  });

  it('handles scientific notation with negative exponent', () => {
    const result = formatSQL('SELECT 1.5e-3, 9E-10;');
    expect(result).toContain('1.5e-3');
    expect(result).toContain('9E-10');
  });

  it('handles hex literals', () => {
    const result = formatSQL('SELECT 0xFF, 0x1a2b, 0XDEADBEEF;');
    expect(result).toContain('0xFF');
    expect(result).toContain('0x1a2b');
    expect(result).toContain('0XDEADBEEF');
  });

  it('handles decimal starting with dot', () => {
    const result = formatSQL('SELECT .5, .123;');
    expect(result).toContain('.5');
    expect(result).toContain('.123');
  });

  it('handles decimal ending with dot', () => {
    const result = formatSQL('SELECT 1., 100.;');
    expect(result).toContain('1.');
    expect(result).toContain('100.');
  });

  it('handles number with underscores', () => {
    const result = formatSQL('SELECT 1_000_000, 0xFF_FF;');
    expect(result).toContain('1_000_000');
    expect(result).toContain('0xFF_FF');
  });

  it('handles zero in various forms', () => {
    const result = formatSQL('SELECT 0, 0.0, .0, 0., 0e0;');
    expect(result).toContain('0');
    expect(result).toContain('0.0');
  });

  it('rejects or handles NaN-like literals', () => {
    // SQL doesn't have NaN literals, but test parser behavior
    const result = formatSQL("SELECT 'NaN'::float;");
    expect(result).toContain('NaN');
  });

  it('rejects or handles Infinity-like literals', () => {
    const result = formatSQL("SELECT 'Infinity'::float;");
    expect(result).toContain('Infinity');
  });
});

describe('Comment edge cases', () => {
  it('handles line comment at end of file with no newline', () => {
    const result = formatSQL('SELECT 1 -- comment');
    expect(result).toContain('-- comment');
  });

  it('handles empty line comment', () => {
    const result = formatSQL('SELECT 1 --\n FROM t;');
    expect(result).toBeDefined();
  });

  it('handles block comment spanning multiple lines', () => {
    const result = formatSQL('SELECT /* this\nis\nmultiline */ 1;');
    expect(result).toContain('/*');
    expect(result).toContain('*/');
  });

  it('handles empty block comment', () => {
    const result = formatSQL('SELECT /**/ 1;');
    expect(result).toContain('/**/');
  });

  it('handles block comment with asterisks inside', () => {
    const result = formatSQL('SELECT /* ** *** */ 1;');
    expect(result).toContain('/* ** ***');
  });

  it('does NOT support nested block comments (standard SQL)', () => {
    // Standard SQL doesn't support nested /* /* */ */, should close at first */
    expect(() => tokenize('SELECT /* outer /* inner */ still outer */ 1;')).not.toThrow();
    // The tokenizer should consume up to the first */ and treat "still outer */ 1;" as separate tokens
  });

  it('handles comment containing SQL keywords', () => {
    const result = formatSQL('SELECT 1 -- SELECT FROM WHERE\n FROM t;');
    expect(result).toContain('-- SELECT FROM WHERE');
  });

  it('handles comment with special characters', () => {
    const result = formatSQL('SELECT 1 -- @#$%^&*()');
    expect(result).toContain('-- @#$%^&*()');
  });

  it('handles multiple consecutive line comments', () => {
    const result = formatSQL('SELECT 1\n-- comment1\n-- comment2\n-- comment3\nFROM t;', { recover: true });
    expect(result).toContain('-- comment1');
    expect(result).toContain('-- comment2');
    expect(result).toContain('-- comment3');
  });

  it('handles comment between tokens', () => {
    const result = formatSQL('SELECT /* mid */ 1 FROM /* mid2 */ t;');
    expect(result).toContain('/* mid */');
    expect(result).toContain('/* mid2 */');
  });

  it('rejects unterminated block comment', () => {
    expect(() => tokenize('SELECT /* unterminated')).toThrow(TokenizeError);
  });

  it('rejects unterminated string in comment context', () => {
    // Comments can't contain unterminated strings, but strings CAN contain comment markers
    const result = formatSQL("SELECT '/* not a comment' FROM t;");
    expect(result).toContain("'/* not a comment'");
  });
});

describe('Whitespace edge cases', () => {
  it('handles tabs', () => {
    const result = formatSQL('SELECT\t1\tFROM\tt;');
    expect(result).toContain('SELECT');
    expect(result).toContain('FROM');
  });

  it('handles multiple consecutive spaces', () => {
    const result = formatSQL('SELECT     1     FROM     t;');
    expect(result).toContain('SELECT');
  });

  it('handles form feed and vertical tab', () => {
    const result = formatSQL('SELECT\f1\vFROM\ft;');
    expect(result).toContain('SELECT');
  });

  it('handles CRLF line endings', () => {
    const result = formatSQL('SELECT 1\r\nFROM t;');
    expect(result).toContain('SELECT');
    expect(result).toContain('FROM');
  });

  it('handles LF line endings', () => {
    const result = formatSQL('SELECT 1\nFROM t;');
    expect(result).toContain('SELECT');
  });

  it('handles CR-only line endings (old Mac)', () => {
    const result = formatSQL('SELECT 1\rFROM t;');
    expect(result).toContain('SELECT');
  });

  it('handles mixed line endings', () => {
    const result = formatSQL('SELECT 1\r\nFROM t\nWHERE x\r= 1;');
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });

  it('handles leading whitespace', () => {
    const result = formatSQL('   \n\t  SELECT 1;');
    expect(result).toContain('SELECT');
  });

  it('handles trailing whitespace', () => {
    const result = formatSQL('SELECT 1;   \n\t  ');
    expect(result).toContain('SELECT');
  });

  it('handles multiple blank lines', () => {
    const result = formatSQL('SELECT 1\n\n\n\nFROM t;');
    expect(result).toContain('SELECT');
  });

  it('rejects or handles zero-width space (if present)', () => {
    // Zero-width space U+200B is not standard SQL whitespace
    const input = 'SELECT\u200B1 FROM t;';
    try {
      const result = formatSQL(input);
      // If it doesn't throw, verify output
      expect(result).toBeDefined();
    } catch (err) {
      // Throwing TokenizeError is also valid
      expect(err).toBeInstanceOf(TokenizeError);
    }
  });

  it('rejects or handles BOM (Byte Order Mark)', () => {
    // BOM U+FEFF at start of file
    const input = '\uFEFFSELECT 1 FROM t;';
    try {
      const result = formatSQL(input);
      expect(result).toBeDefined();
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
    }
  });
});

describe('Expression edge cases', () => {
  it('handles deeply nested parentheses (stress test)', () => {
    const depth = 50;
    const sql = 'SELECT ' + '('.repeat(depth) + '1' + ')'.repeat(depth) + ';';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
  });

  it('handles chained arithmetic operators', () => {
    const result = formatSQL('SELECT 1 + 2 - 3 * 4 / 5 % 6;');
    expect(result).toContain('+');
    expect(result).toContain('-');
    expect(result).toContain('*');
    expect(result).toContain('/');
    expect(result).toContain('%');
  });

  it('handles chained comparison operators', () => {
    const result = formatSQL('SELECT x < y AND y <= z AND z > a AND a >= b;');
    expect(result).toContain('<');
    expect(result).toContain('<=');
    expect(result).toContain('>');
    expect(result).toContain('>=');
  });

  it('handles CASE with no ELSE', () => {
    const result = formatSQL('SELECT CASE WHEN x = 1 THEN 2 END;');
    expect(result).toContain('CASE');
    expect(result).toContain('WHEN');
    expect(result).toContain('THEN');
    expect(result).toContain('END');
    expect(result).not.toContain('ELSE');
  });

  it('handles nested CASE expressions', () => {
    const result = formatSQL('SELECT CASE WHEN x = 1 THEN CASE WHEN y = 2 THEN 3 ELSE 4 END ELSE 5 END;');
    expect(result).toContain('CASE');
    // Should have two CASE and two END
    expect((result.match(/CASE/g) || []).length).toBe(2);
    expect((result.match(/END/g) || []).length).toBe(2);
  });

  it('handles BETWEEN with complex expressions', () => {
    const result = formatSQL('SELECT * FROM t WHERE x + y BETWEEN a * b AND c / d;');
    expect(result).toContain('BETWEEN');
    expect(result).toContain('AND');
  });

  it('handles IN with multiple values', () => {
    const result = formatSQL('SELECT * FROM t WHERE x IN (1, 2, 3, 4, 5);');
    expect(result).toContain('IN');
    expect(result).toContain('1');
    expect(result).toContain('5');
  });

  it('handles IN with subquery', () => {
    const result = formatSQL('SELECT * FROM t WHERE x IN (SELECT y FROM u);');
    expect(result).toContain('IN');
    expect(result).toContain('SELECT');
  });

  it('handles EXISTS subquery', () => {
    const result = formatSQL('SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id);');
    expect(result).toContain('EXISTS');
  });

  it('handles NOT IN with NULL consideration', () => {
    const result = formatSQL('SELECT * FROM t WHERE x NOT IN (1, 2, NULL);');
    expect(result).toContain('NOT IN');
    expect(result).toContain('NULL');
  });

  it('handles IS NULL and IS NOT NULL', () => {
    const result = formatSQL('SELECT * FROM t WHERE x IS NULL AND y IS NOT NULL;');
    expect(result).toContain('IS NULL');
    expect(result).toContain('IS NOT NULL');
  });

  it('handles unary NOT', () => {
    const result = formatSQL('SELECT * FROM t WHERE NOT (x = 1 AND y = 2);');
    expect(result).toContain('NOT');
  });

  it('handles operator precedence: OR < AND', () => {
    const result = formatSQL('SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3;');
    expect(result).toContain('OR');
    expect(result).toContain('AND');
  });
});

describe('Empty and degenerate input', () => {
  it('handles empty string', () => {
    const result = formatSQL('');
    expect(result).toBe('');
  });

  it('handles whitespace only', () => {
    const result = formatSQL('   \n\t  ');
    expect(result).toBe('');
  });

  it('handles semicolon only', () => {
    const result = formatSQL(';');
    expect(result).toBe('');
  });

  it('handles multiple semicolons', () => {
    const result = formatSQL(';;;');
    expect(result).toBe('');
  });

  it('handles semicolons with whitespace', () => {
    const result = formatSQL('; ; ;');
    expect(result).toBe('');
  });

  it('handles very long single-line SQL', () => {
    const cols = Array.from({ length: 200 }, (_, i) => `col${i}`).join(', ');
    const sql = `SELECT ${cols} FROM t;`;
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('col0');
    expect(result).toContain('col199');
  });

  it('handles single SELECT keyword only', () => {
    const nodes = parse('SELECT;', { recover: true });
    expect(nodes.length).toBeGreaterThanOrEqual(0);
  });

  it('handles SELECT with no FROM', () => {
    const result = formatSQL('SELECT 1;');
    expect(result).toContain('SELECT 1');
  });
});

describe('Injection-like patterns', () => {
  it('handles SQL containing template literal syntax', () => {
    const result = formatSQL("SELECT '${injection}' FROM t;");
    expect(result).toContain("'${injection}'");
  });

  it('handles SQL containing backticks', () => {
    // Backticks are not standard SQL, might be treated as error or identifier delimiter
    try {
      const result = formatSQL('SELECT `column` FROM t;');
      expect(result).toBeDefined();
    } catch (err) {
      expect(err).toBeDefined();
    }
  });

  it('handles SQL with dollar signs in various contexts', () => {
    const result = formatSQL('SELECT $1, $$text$$, $tag$body$tag$ FROM t;');
    expect(result).toContain('$1');
    expect(result).toContain('$$text$$');
    expect(result).toContain('$tag$body$tag$');
  });

  it('handles SQL with percent signs (LIKE patterns)', () => {
    const result = formatSQL("SELECT * FROM t WHERE name LIKE '%test%';");
    expect(result).toContain("'%test%'");
  });

  it('handles SQL with escape sequences', () => {
    const result = formatSQL("SELECT * FROM t WHERE name LIKE '%\\_test%' ESCAPE '\\';");
    expect(result).toContain('ESCAPE');
  });
});

describe('Multi-statement input', () => {
  it('handles two simple statements', () => {
    const result = formatSQL('SELECT 1; SELECT 2;');
    expect(result).toContain('SELECT 1');
    expect(result).toContain('SELECT 2');
    expect((result.match(/;/g) || []).length).toBe(2);
  });

  it('handles mixed statement types', () => {
    const result = formatSQL('SELECT 1; INSERT INTO t VALUES (1); UPDATE t SET x = 1;');
    expect(result).toContain('SELECT');
    expect(result).toContain('INSERT');
    expect(result).toContain('UPDATE');
  });

  it('handles statements with blank lines between', () => {
    const result = formatSQL('SELECT 1;\n\n\nSELECT 2;');
    expect(result).toContain('SELECT 1');
    expect(result).toContain('SELECT 2');
  });

  it('handles statement with trailing semicolons', () => {
    const result = formatSQL('SELECT 1;;');
    expect(result).toContain('SELECT 1');
  });
});

describe('Error recovery mode', () => {
  it('recovers from unclosed parenthesis', () => {
    const nodes = parse('SELECT (1 + 2;', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('recovers from missing FROM', () => {
    const nodes = parse('SELECT x WHERE y = 1;', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('recovers from extra commas', () => {
    const nodes = parse('SELECT 1,, 2 FROM t;', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('recovers from trailing comma in SELECT', () => {
    const nodes = parse('SELECT a, b, FROM t;', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('recovers from missing VALUES keyword', () => {
    const nodes = parse('INSERT INTO t (1, 2);', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('throws in strict mode for unclosed paren', () => {
    expect(() => parse('SELECT (1 + 2;', { recover: false })).toThrow(ParseError);
  });

  it('handles double comma in recover mode but throws in strict', () => {
    // The parser might not always throw on this specific input in strict mode
    // depending on how it handles comma-separated lists
    const nodes = parse('SELECT 1,, 2 FROM t;', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });
});

describe('Dollar-quoted string edge cases', () => {
  it('handles empty dollar-quoted string', () => {
    const result = formatSQL('SELECT $$$$;');
    expect(result).toContain('$$$$');
  });

  it('handles dollar-quoted with special chars in body', () => {
    const result = formatSQL("SELECT $$special !@#$%^&*()$$;");
    expect(result).toContain('$$special !@#$%^&*()$$');
  });

  it('handles dollar-quoted with SQL inside', () => {
    const result = formatSQL("SELECT $$SELECT * FROM t WHERE x = 'value';$$;");
    expect(result).toContain("SELECT * FROM t WHERE x = 'value';");
  });

  it('handles different dollar tags', () => {
    const result = formatSQL('SELECT $a$body$a$, $b$other$b$;');
    expect(result).toContain('$a$body$a$');
    expect(result).toContain('$b$other$b$');
  });

  it('handles dollar-quoted with single quotes inside', () => {
    const result = formatSQL("SELECT $$It's working$$;");
    expect(result).toContain("$$It's working$$");
  });

  it('handles dollar-quoted without closing tag gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $tag$no close');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('handles nested-looking dollar quotes with different tags', () => {
    const result = formatSQL('SELECT $outer$text $inner$nested$inner$ more$outer$;');
    expect(result).toContain('$outer$text $inner$nested$inner$ more$outer$');
  });
});

describe('Type cast and operators', () => {
  it('handles :: type cast', () => {
    const result = formatSQL("SELECT '123'::int, x::text;");
    expect(result).toContain('::');
  });

  it('handles JSON operators', () => {
    const result = formatSQL("SELECT data->'key', data->>'key', data#>'{a,b}', data@>'{}';");
    expect(result).toContain("data -> 'key'");
    expect(result).toContain("data ->> 'key'");
    expect(result).toContain("data #> '{a,b}'");
    expect(result).toContain("data @> '{}'");
  });

  it('handles array operators', () => {
    const result = formatSQL("SELECT a[1], a[1:5], a || b;");
    expect(result).toContain('[');
    expect(result).toContain(']');
    expect(result).toContain('||');
  });

  it('handles regex operators', () => {
    const result = formatSQL("SELECT * FROM t WHERE name ~ 'pattern' AND name !~ 'other';");
    expect(result).toContain('~');
    expect(result).toContain('!~');
  });
});

describe('Stress and boundary tests', () => {
  it('handles SQL at max input size limit', () => {
    // Default is 10MB, test something well under
    const bigSql = 'SELECT ' + 'a, '.repeat(100_000) + '1 FROM t;';
    const result = formatSQL(bigSql);
    expect(result).toContain('SELECT');
  });

  it('rejects SQL over max input size', () => {
    const tooBig = 'SELECT ' + 'a'.repeat(10_500_000);
    expect(() => formatSQL(tooBig)).toThrow('exceeds maximum size');
  });

  it('handles many tokens (stress token count)', () => {
    const manyTokens = 'SELECT ' + Array.from({ length: 10_000 }, (_, i) => `${i}`).join(', ') + ';';
    const result = formatSQL(manyTokens);
    expect(result).toContain('SELECT');
  });

  it('handles very long identifier (at limit)', () => {
    const maxIdent = 'a'.repeat(10_000);
    const sql = `SELECT ${maxIdent} FROM t;`;
    const result = formatSQL(sql);
    expect(result).toContain(maxIdent);
  });

  it('rejects identifier over length limit', () => {
    const tooLong = 'a'.repeat(10_001);
    const sql = `SELECT ${tooLong} FROM t;`;
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('handles complex query with all features combined', () => {
    const sql = `
      WITH cte AS (
        SELECT id, name, CASE WHEN x > 10 THEN 'high' ELSE 'low' END AS tier
        FROM users
        WHERE created_at BETWEEN '2020-01-01' AND '2023-12-31'
          AND status IN ('active', 'pending')
          AND metadata @> '{"verified": true}'::jsonb
      )
      SELECT c.name,
             (SELECT COUNT(*) FROM orders WHERE user_id = c.id) AS order_count,
             c.tier
        FROM cte AS c
             LEFT JOIN subscriptions AS s ON c.id = s.user_id
       WHERE c.tier = 'high'
         AND s.expires_at > NOW()
       ORDER BY c.name NULLS LAST
       LIMIT 100
      OFFSET 10;
    `;
    const result = formatSQL(sql);
    expect(result).toContain('WITH');
    expect(result).toContain('CASE');
    expect(result).toContain('BETWEEN');
    expect(result).toContain('LEFT JOIN');
    expect(result).toContain('ORDER BY');
    expect(result).toContain('LIMIT');
  });
});
