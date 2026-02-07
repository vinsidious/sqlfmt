import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse, MaxDepthError, ParseError } from '../src/parser';
import { tokenize, TokenizeError } from '../src/tokenizer';
import { mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

const root = join(import.meta.dir, '..');
const cliPath = join(root, 'src', 'cli.ts');
const bunPath = process.execPath;
const decoder = new TextDecoder();

function runCli(args: string[]) {
  const proc = Bun.spawnSync({
    cmd: [bunPath, cliPath, ...args],
    cwd: root,
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
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });

  it('handles deeply nested OR conditions without crashing', () => {
    const conditions: string[] = [];
    for (let i = 0; i < 250; i++) {
      conditions.push('b = 2');
    }
    const sql = 'SELECT 1 FROM t WHERE ' + conditions.join(' OR ') + ';';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('WHERE');
  });

  it('handles mixed AND/OR deeply nested conditions', () => {
    const conditions: string[] = [];
    for (let i = 0; i < 200; i++) {
      conditions.push(`c${i} = ${i}`);
    }
    const sql = 'SELECT 1 FROM t WHERE ' + conditions.join(i => i % 2 === 0 ? ' AND ' : ' OR ') + ';';
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
});

describe('CLI path validation for --write', () => {
  it('writes files in temp directories with absolute paths', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-pathval-'));
    const file = join(dir, 'test.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--write', file]);
    expect(res.code).toBe(0);
    expect(readFileSync(file, 'utf8')).toBe('SELECT 1;\n');
  });

  it('creates and cleans up temp file during atomic write', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-atomic-'));
    const file = join(dir, 'atomic.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--write', file]);
    expect(res.code).toBe(0);
    // No .tmp files should remain in the directory
    const remaining = readdirSync(dir).filter(f => f.endsWith('.tmp'));
    expect(remaining).toEqual([]);
    // Original file should have formatted content
    expect(readFileSync(file, 'utf8')).toBe('SELECT 1;\n');
  });
});

describe('glob filtering', () => {
  it('excludes .git directory from glob expansion', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-globfilter-'));
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
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-globfilter-'));
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
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-globfilter-'));
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
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-ignore-safe-'));
    const file = join(dir, 'query.sql');
    writeFileSync(file, 'select 1;', 'utf8');
    const pattern = 'unlikely-prefix-' + '*'.repeat(300) + '?.sql';

    const res = runCli(['--write', '--ignore', pattern, file]);
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
