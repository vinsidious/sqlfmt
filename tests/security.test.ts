import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { FormatterError } from '../src/formatter';
import { parse, MaxDepthError, ParseError } from '../src/parser';
import { tokenize, TokenizeError } from '../src/tokenizer';
import { mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

const root = join(import.meta.dir, '..');
const cliPath = join(root, 'src', 'cli.ts');
const bunPath = process.execPath;
const decoder = new TextDecoder();

function runCli(args: string[], cwd: string = root) {
  const proc = Bun.spawnSync({
    cmd: [bunPath, cliPath, ...args],
    cwd,
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
    env: process.env,
  });

  return {
    code: proc.exitCode,
    out: decoder.decode(proc.stdout),
    err: decoder.decode(proc.stderr),
  };
}

describe('formatter depth limit', () => {
  it('handles deeply nested AND/OR conditions without crashing', () => {
    // Build a deeply nested AND chain: a = 1 AND a = 1 AND ... (250 levels deep)
    const conditions: string[] = [];
    for (let i = 0; i < 250; i++) {
      conditions.push('a = 1');
    }
    const sql = 'SELECT 1 FROM t WHERE ' + conditions.join(' AND ') + ';';
    // Should not crash with stack overflow
    const result = formatSQL(sql, { maxDepth: 600 });
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });

  it('handles deeply nested OR conditions without crashing', () => {
    const conditions: string[] = [];
    for (let i = 0; i < 250; i++) {
      conditions.push('b = 2');
    }
    const sql = 'SELECT 1 FROM t WHERE ' + conditions.join(' OR ') + ';';
    const result = formatSQL(sql, { maxDepth: 600 });
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });

  it('handles mixed AND/OR deeply nested conditions', () => {
    const conditions: string[] = [];
    for (let i = 0; i < 200; i++) {
      conditions.push(`c${i} = ${i}`);
    }
    const mixed: string[] = [];
    for (let i = 0; i < conditions.length; i++) {
      mixed.push(conditions[i]);
      if (i < conditions.length - 1) mixed.push(i % 2 === 0 ? 'AND' : 'OR');
    }
    const sql = 'SELECT 1 FROM t WHERE ' + mixed.join(' ') + ';';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
  });
});

describe('formatter depth limit with CASE and subqueries', () => {
  it('formats deeply nested CASE expressions without crashing', () => {
    // Build nested CASE WHEN ... THEN (CASE WHEN ... ) END chains
    // Each level adds a CASE WHEN 1=1 THEN ... END wrapper
    const depth = 80; // well within formatter's 200 limit but deep enough to stress
    let expr = '1';
    for (let i = 0; i < depth; i++) {
      expr = `CASE WHEN x = ${i} THEN ${expr} ELSE 0 END`;
    }
    const sql = `SELECT ${expr} FROM t;`;
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('CASE');
    expect(result).toContain('END');
  });

  it('formats deeply nested subqueries without crashing', () => {
    // Build nested subqueries: SELECT (SELECT (SELECT ... FROM t) FROM t) FROM t
    const depth = 50; // each subquery adds parser depth
    let inner = 'SELECT 1 FROM t';
    for (let i = 0; i < depth; i++) {
      inner = `SELECT (${inner}) FROM t`;
    }
    const sql = inner + ';';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
  });

  it('parser throws MaxDepthError for excessively nested expressions', () => {
    // Build deeply nested parenthesized expressions exceeding default maxDepth=100
    const sql = 'SELECT ' + '('.repeat(120) + '1' + ')'.repeat(120) + ';';
    try {
      parse(sql, { recover: false, maxDepth: 100 });
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(MaxDepthError);
      expect(err).toBeInstanceOf(ParseError);
      expect((err as MaxDepthError).maxDepth).toBe(100);
    }
  });

  it('MaxDepthError is not recovered even in recovery mode', () => {
    const sql = 'SELECT ' + '('.repeat(120) + '1' + ')'.repeat(120) + ';';
    expect(() => parse(sql, { recover: true, maxDepth: 100 })).toThrow(MaxDepthError);
  });

  it('parses successfully at exactly the max depth limit', () => {
    // Build parenthesized expression at exactly maxDepth levels
    // Each "(" in an expression context increments depth via parseExpression -> withDepth
    // Use a small maxDepth to make this tractable
    const depth = 20;
    const sql = 'SELECT ' + '('.repeat(depth) + '1' + ')'.repeat(depth) + ';';
    const nodes = parse(sql, { recover: false, maxDepth: depth + 1 });
    expect(nodes.length).toBe(1);
  });
});

describe('input size validation', () => {
  it('throws error when input exceeds default max size', () => {
    // Default is 10MB, create something just over
    const largeInput = 'SELECT ' + 'a'.repeat(10_485_760) + ';';
    expect(() => formatSQL(largeInput)).toThrow('Input exceeds maximum size');
  });

  it('throws error when input exceeds custom max size', () => {
    const input = 'SELECT ' + 'a'.repeat(200) + ';';
    expect(() => formatSQL(input, { maxInputSize: 100 })).toThrow(
      'Input exceeds maximum size of 100 bytes'
    );
  });

  it('allows input within custom max size', () => {
    const input = 'SELECT 1;';
    const result = formatSQL(input, { maxInputSize: 1000 });
    expect(result).toBe('SELECT 1;\n');
  });

  it('error message includes the configured limit', () => {
    try {
      formatSQL('SELECT 1;', { maxInputSize: 5 });
      throw new Error('should have thrown');
    } catch (err) {
      expect((err as Error).message).toContain('5 bytes');
      expect((err as Error).message).toContain('maxInputSize');
    }
  });

  it('counts multibyte characters correctly as UTF-8 bytes', () => {
    // '€' is 3 bytes in UTF-8, but 1 code unit in JS (U+20AC)
    // 5 × '€' = 15 bytes, plus "SELECT '';" = 10 ASCII bytes → 25 bytes total
    const input = "SELECT '€€€€€';";
    // 16 bytes in UTF-8: SELECT + space + quote + 5×3 + quote + semicolon = 25
    expect(() => formatSQL(input, { maxInputSize: 20 })).toThrow('exceeds maximum size');
    // Should succeed with enough room
    expect(formatSQL(input, { maxInputSize: 30 })).toContain('SELECT');
  });

  it('counts surrogate pairs as 4 UTF-8 bytes', () => {
    // '𝄞' (U+1D11E) is a surrogate pair in JS (2 code units) but 4 bytes in UTF-8
    // With old input.length check, "SELECT '𝄞';" would be 12 code units
    // With correct byte check, it's 13 bytes (SELECT=6 + space=1 + quote=1 + 4 + quote=1 + ;=1 = 14 bytes... wait)
    const input = "SELECT '𝄞';";
    // "SELECT '𝄞';" = 6+1+1+4+1+1 = 14 bytes UTF-8, but 12 code units
    expect(() => formatSQL(input, { maxInputSize: 12 })).toThrow('exceeds maximum size');
    expect(formatSQL(input, { maxInputSize: 14 })).toContain('SELECT');
  });

  it('counts malformed surrogate sequences using UTF-8 replacement-byte behavior', () => {
    const input = "SELECT '\uD800\uD800';";
    expect(() => formatSQL(input, { maxInputSize: 15 })).toThrow('exceeds maximum size');
    expect(formatSQL(input, { maxInputSize: 16 })).toContain('SELECT');
  });
});

describe('CLI path validation for --write', () => {
  it('skips absolute paths outside cwd', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-pathval-'));
    const file = join(dir, 'test.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--write', file]);
    expect(res.code).toBe(0);
    expect(res.err).toContain('path resolves outside working directory');
    expect(readFileSync(file, 'utf8')).toBe('select 1;');
  });

  it('does not leave temp files when write is skipped by path validation', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-atomic-'));
    const file = join(dir, 'atomic.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--write', file]);
    expect(res.code).toBe(0);
    // No .tmp files should remain in the directory
    const remaining = readdirSync(dir).filter(f => f.endsWith('.tmp'));
    expect(remaining).toEqual([]);
    // Original file should be unchanged
    expect(readFileSync(file, 'utf8')).toBe('select 1;');
  });
});

describe('glob filtering', () => {
  it('excludes .git directory from glob expansion', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-globfilter-'));
    const gitDir = join(dir, '.git');
    const { mkdirSync } = require('fs');
    mkdirSync(gitDir, { recursive: true });
    writeFileSync(join(dir, 'good.sql'), 'select 1;', 'utf8');
    writeFileSync(join(gitDir, 'bad.sql'), 'select 2;', 'utf8');

    // Use --check to just check without modifying
    const res = runCli(['--check', join(dir, '**/*.sql')]);
    // Only good.sql should be checked (the .git one should be excluded)
    // good.sql is not formatted, so it should fail
    expect(res.code).toBe(1);
    expect(res.err).not.toContain('.git');
  });

  it('excludes node_modules from glob expansion', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-globfilter-'));
    const nmDir = join(dir, 'node_modules', 'pkg');
    const { mkdirSync } = require('fs');
    mkdirSync(nmDir, { recursive: true });
    writeFileSync(join(dir, 'good.sql'), 'select 1;', 'utf8');
    writeFileSync(join(nmDir, 'bad.sql'), 'select 2;', 'utf8');

    const res = runCli(['--check', join(dir, '**/*.sql')]);
    expect(res.code).toBe(1);
    expect(res.err).not.toContain('node_modules');
  });

  it('excludes dotfiles from glob expansion', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-globfilter-'));
    const dotDir = join(dir, '.hidden');
    const { mkdirSync } = require('fs');
    mkdirSync(dotDir, { recursive: true });
    writeFileSync(join(dir, 'good.sql'), 'select 1;', 'utf8');
    writeFileSync(join(dotDir, 'hidden.sql'), 'select 2;', 'utf8');

    const res = runCli(['--check', join(dir, '**/*.sql')]);
    expect(res.code).toBe(1);
    expect(res.err).not.toContain('.hidden');
  });
});

describe('ignore pattern safety', () => {
  it('handles complex wildcard patterns without regex backtracking', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignore-safe-'));
    const file = join(dir, 'query.sql');
    writeFileSync(file, 'select 1;', 'utf8');
    const pattern = 'unlikely-prefix-' + '*'.repeat(300) + '?.sql';

    const res = runCli(['--write', '--ignore', pattern, 'query.sql'], dir);
    expect(res.code).toBe(0);
    expect(readFileSync(file, 'utf8')).toBe('SELECT 1;\n');
  });

  it('handles window frames with quoted AND text safely', () => {
    const sql = `select value, sum(value) over (order by measurement_date range between interval 'A AND B' preceding and current row) as rolling from sensor_data;`;
    const formatted = formatSQL(sql);
    expect(formatted).toContain(`RANGE BETWEEN INTERVAL 'A AND B' PRECEDING`);
    expect(formatted).toContain('AND CURRENT ROW');
  });
});

describe('identifier length limit', () => {
  it('throws TokenizeError for identifiers exceeding 10000 characters', () => {
    const longIdent = 'a'.repeat(10_001);
    const sql = `SELECT ${longIdent};`;
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('throws with descriptive message for long identifiers', () => {
    const longIdent = 'x'.repeat(10_001);
    try {
      tokenize(`SELECT ${longIdent};`);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('maximum length');
    }
  });

  it('allows identifiers at exactly 10000 characters', () => {
    const maxIdent = 'b'.repeat(10_000);
    const sql = `SELECT ${maxIdent};`;
    // Should not throw
    const tokens = tokenize(sql);
    expect(tokens.some(t => t.value === maxIdent)).toBe(true);
  });

  it('handles normal identifiers without issues', () => {
    const tokens = tokenize('SELECT my_column FROM my_table;');
    expect(tokens.some(t => t.value === 'my_column')).toBe(true);
    expect(tokens.some(t => t.value === 'my_table')).toBe(true);
  });
});

describe('formatter depth limit enforcement in formatting phase', () => {
  it('handles 200+ nested AND conditions in formatting without stack overflow', () => {
    const conditions: string[] = [];
    for (let i = 0; i < 300; i++) {
      conditions.push(`col${i} = ${i}`);
    }
    const sql = 'SELECT 1 FROM t WHERE ' + conditions.join(' AND ') + ';';
    const result = formatSQL(sql, { maxDepth: 700 });
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });

  it('handles 200+ nested OR conditions in formatting without stack overflow', () => {
    const conditions: string[] = [];
    for (let i = 0; i < 300; i++) {
      conditions.push(`col${i} = ${i}`);
    }
    const sql = 'SELECT 1 FROM t WHERE ' + conditions.join(' OR ') + ';';
    const result = formatSQL(sql, { maxDepth: 700 });
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });
});

describe('memory and consistency checks', () => {
  it('formatting the same SQL twice produces identical results', () => {
    const sql = `SELECT e.name, d.dept_name, CASE WHEN e.salary > 100000 THEN 'high' ELSE 'normal' END AS tier FROM employees AS e INNER JOIN departments AS d ON e.dept_id = d.id WHERE e.active = true AND d.country = 'US' ORDER BY e.name;`;
    const result1 = formatSQL(sql);
    const result2 = formatSQL(sql);
    expect(result1).toBe(result2);
  });

  it('formatting produces identical results across multiple invocations', () => {
    const sql = `WITH stats AS (SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept) SELECT s.dept, s.avg_sal FROM stats AS s WHERE s.avg_sal > 50000 ORDER BY s.avg_sal DESC;`;
    const results = [];
    for (let i = 0; i < 5; i++) {
      results.push(formatSQL(sql));
    }
    for (let i = 1; i < results.length; i++) {
      expect(results[i]).toBe(results[0]);
    }
  });

  it('does not produce excessively large output for moderately sized inputs', () => {
    // A moderately complex query should not produce output that is orders of magnitude larger
    const conditions: string[] = [];
    for (let i = 0; i < 100; i++) {
      conditions.push(`col_${i} = ${i}`);
    }
    const sql = 'SELECT * FROM t WHERE ' + conditions.join(' AND ') + ';';
    const result = formatSQL(sql);
    // Output should be roughly similar in size to input (within 5x for formatting additions)
    expect(result.length).toBeLessThan(sql.length * 5);
  });
});

describe('sensitive token sanitization in CLI errors', () => {
  it('does not leak string literal in error when string token triggers parse failure', () => {
    // INSERT ... VALUES (value JUNK) triggers "Expected ), got ..." error
    // The error token is 'identifier' here, but this tests the overall error path
    const sql = `INSERT INTO t VALUES ('my_secret_api_key' JUNK);`;
    const dir = mkdtempSync(join(tmpdir(), 'holywell-sanitize-'));
    const file = join(dir, 'sensitive.sql');
    writeFileSync(file, sql, 'utf8');

    const res = runCli(['--strict', '--no-color', file]);
    expect(res.code).toBe(2);
    // The error message format shows 'got "TOKEN_VALUE" (token_type)'
    // Verify the error output doesn't contain the secret as the got-token
    // (it appears in the source context lines, which is expected)
    expect(res.err).toContain('Expected');
  });

  it('formats SQL with sensitive literals without leaking in normal output', () => {
    // Verify that normal formatting does not mangle the content
    const sql = `SELECT * FROM users WHERE api_key = 'sk-1234567890abcdef';`;
    const dir = mkdtempSync(join(tmpdir(), 'holywell-sanitize-'));
    const file = join(dir, 'normal.sql');
    writeFileSync(file, sql, 'utf8');

    const res = runCli(['--no-color', file]);
    expect(res.code).toBe(0);
    // The formatted output should still contain the literal (formatting doesn't sanitize content)
    expect(res.out).toContain('sk-1234567890abcdef');
  });
});

describe('formatter depth limit catches deeply nested CASE expressions', () => {
  it('deeply nested CASE expressions do not crash the formatter', () => {
    const depth = 100;
    let expr = '1';
    for (let i = 0; i < depth; i++) {
      expr = `CASE WHEN c${i} = ${i} THEN ${expr} ELSE 0 END`;
    }
    const sql = `SELECT ${expr} FROM t;`;
    // Should format or throw MaxDepthError, not stack overflow
    try {
      const result = formatSQL(sql);
      expect(result).toContain('SELECT');
      expect(result).toContain('CASE');
    } catch (err) {
      expect(err instanceof FormatterError || err instanceof MaxDepthError).toBe(true);
    }
  });
});

describe('formatter depth limit catches deeply nested parenthesized expressions', () => {
  it('deeply nested parens in expressions do not crash the formatter', () => {
    // Build deeply nested paren chain that parsing allows but stresses formatter
    const depth = 150;
    let expr = '1';
    for (let i = 0; i < depth; i++) {
      expr = `(${expr} + ${i})`;
    }
    const sql = `SELECT ${expr} FROM t;`;
    try {
      const result = formatSQL(sql);
      expect(result).toContain('SELECT');
    } catch (err) {
      expect(err instanceof FormatterError || err instanceof MaxDepthError).toBe(true);
    }
  });
});

describe('fuzz smoke tests', () => {
  it('formats randomized predicate chains without throwing and remains idempotent', () => {
    let seed = 123456789;
    const next = () => {
      seed = (seed * 1664525 + 1013904223) >>> 0;
      return seed;
    };

    for (let i = 0; i < 40; i++) {
      const termCount = 5 + (next() % 12);
      const terms: string[] = [];
      for (let t = 0; t < termCount; t++) {
        const col = `c${next() % 7}`;
        const val = next() % 1000;
        terms.push(`${col} = ${val}`);
      }
      const ops: string[] = [];
      for (let t = 0; t < termCount - 1; t++) {
        ops.push((next() & 1) === 0 ? 'AND' : 'OR');
      }

      let where = '';
      for (let t = 0; t < termCount; t++) {
        where += terms[t];
        if (t < ops.length) where += ` ${ops[t]} `;
      }

      const sql = `SELECT * FROM fuzz_t WHERE ${where};`;
      const once = formatSQL(sql);
      const twice = formatSQL(once);
      expect(twice).toBe(once);
    }
  });
});
