import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'fs';
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

describe('cli flags and UX', () => {
  it('supports --version', () => {
    const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')) as { version: string };
    const res = runCli(['--version']);
    expect(res.code).toBe(0);
    expect(res.out.trim()).toBe(pkg.version);
  });

  it('rejects unknown flags', () => {
    const res = runCli(['--definitely-not-a-real-flag']);
    expect(res.code).toBe(1);
    expect(res.err).toContain('Unknown option');
  });

  it('returns parse exit code 2 with a clean message', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-cli-'));
    const file = join(dir, 'broken.sql');
    writeFileSync(file, "SELECT 'broken", 'utf8');
    const res = runCli([file]);
    expect(res.code).toBe(2);
    expect(res.err).toContain('Parse error');
  });

  it('supports --check with normalized edge whitespace', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-cli-'));
    const file = join(dir, 'normalized.sql');
    writeFileSync(file, '\n\nSELECT 1;\n', 'utf8');
    const res = runCli(['--check', file]);
    expect(res.code).toBe(0);
  });

  it('supports --diff output when --check fails', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-cli-'));
    const file = join(dir, 'diff.sql');
    writeFileSync(file, 'select 1;', 'utf8');
    const res = runCli(['--check', '--diff', file]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--- input');
    expect(res.err).toContain('+++ formatted');
    expect(res.err).toContain('@@');
  });

  it('supports --write for in-place formatting', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-cli-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--write', file]);
    expect(res.code).toBe(0);

    const after = readFileSync(file, 'utf8');
    expect(after).toBe('SELECT 1;\n');
  });

  it('handles multiple file arguments', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-cli-'));
    const fileA = join(dir, 'a.sql');
    const fileB = join(dir, 'b.sql');
    writeFileSync(fileA, 'select 1;', 'utf8');
    writeFileSync(fileB, 'select 2;', 'utf8');

    const checkRes = runCli(['--check', fileA, fileB]);
    expect(checkRes.code).toBe(1);

    const writeRes = runCli(['--write', fileA, fileB]);
    expect(writeRes.code).toBe(0);
    expect(readFileSync(fileA, 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(fileB, 'utf8')).toBe('SELECT 2;\n');
  });
});

describe('glob pattern expansion', () => {
  it('expands *.sql glob patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-glob-'));
    writeFileSync(join(dir, 'a.sql'), 'select 1;', 'utf8');
    writeFileSync(join(dir, 'b.sql'), 'select 2;', 'utf8');
    writeFileSync(join(dir, 'c.txt'), 'not sql', 'utf8');

    const res = runCli(['--write', join(dir, '*.sql')]);
    expect(res.code).toBe(0);

    expect(readFileSync(join(dir, 'a.sql'), 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(join(dir, 'b.sql'), 'utf8')).toBe('SELECT 2;\n');
    // .txt file should be untouched
    expect(readFileSync(join(dir, 'c.txt'), 'utf8')).toBe('not sql');
  });

  it('expands **/*.sql recursive glob patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-glob-'));
    const sub = join(dir, 'sub');
    mkdirSync(sub);
    writeFileSync(join(dir, 'top.sql'), 'select 1;', 'utf8');
    writeFileSync(join(sub, 'nested.sql'), 'select 2;', 'utf8');

    const res = runCli(['--check', join(dir, '**/*.sql')]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('not formatted');
  });

  it('treats non-matching glob as literal path (ENOENT)', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-glob-'));
    const res = runCli([join(dir, '*.sql')]);
    // No matches; treated as literal, should error on I/O
    expect(res.code).toBe(1);
    expect(res.err).toContain('I/O error');
  });
});

describe('--list-different flag', () => {
  it('prints only filenames that need formatting', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-ld-'));
    const clean = join(dir, 'clean.sql');
    const dirty = join(dir, 'dirty.sql');
    writeFileSync(clean, 'SELECT 1;\n', 'utf8');
    writeFileSync(dirty, 'select 1;', 'utf8');

    const res = runCli(['--list-different', clean, dirty]);
    expect(res.code).toBe(1);
    expect(res.out.trim()).toBe(dirty);
  });

  it('exits 0 when all files are already formatted', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-ld-'));
    const file = join(dir, 'ok.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--list-different', file]);
    expect(res.code).toBe(0);
    expect(res.out.trim()).toBe('');
  });

  it('supports -l shorthand', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-ld-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['-l', file]);
    expect(res.code).toBe(1);
    expect(res.out.trim()).toBe(file);
  });

  it('requires file arguments', () => {
    const res = runCli(['--list-different']);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--list-different requires');
  });
});

describe('--no-color flag', () => {
  it('accepts --no-color without error', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-nc-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--no-color', '--check', file]);
    expect(res.code).toBe(0);
  });

  it('does not emit ANSI codes with --no-color', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-nc-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--no-color', '--check', file]);
    expect(res.code).toBe(1);
    // Ensure no ANSI escape sequences
    expect(res.err).not.toContain('\x1b[');
    expect(res.err).toContain('not formatted');
  });
});
