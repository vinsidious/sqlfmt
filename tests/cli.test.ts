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

function runCliWithStdin(args: string[], stdinContent: string) {
  const proc = Bun.spawnSync({
    cmd: [bunPath, cliPath, ...args],
    cwd: root,
    stdin: new TextEncoder().encode(stdinContent),
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

  it('errors on non-matching glob pattern with clear message', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-glob-'));
    const res = runCli([join(dir, '*.sql')]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('No files matched pattern');
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

describe('--verbose flag', () => {
  it('prints file progress to stderr', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-verbose-'));
    const fileA = join(dir, 'a.sql');
    const fileB = join(dir, 'b.sql');
    writeFileSync(fileA, 'select 1;', 'utf8');
    writeFileSync(fileB, 'SELECT 2;\n', 'utf8');

    const res = runCli(['--verbose', '--write', fileA, fileB]);
    expect(res.code).toBe(0);
    // Should print "Formatting N files..." header
    expect(res.err).toContain('Formatting 2 files...');
    // Should print each filename
    expect(res.err).toContain(fileA);
    expect(res.err).toContain(fileB);
    // Should print summary with changed count
    expect(res.err).toContain('Formatted 2 files (1 changed)');
  });

  it('prints singular form for 1 file', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-verbose-'));
    const file = join(dir, 'a.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--verbose', '--write', file]);
    expect(res.code).toBe(0);
    expect(res.err).toContain('Formatting 1 file...');
    expect(res.err).toContain('Formatted 1 file (0 changed)');
  });
});

describe('--quiet flag', () => {
  it('suppresses normal output', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-quiet-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--quiet', '--check', file]);
    expect(res.code).toBe(1);
    // stderr should have no "not formatted" message
    expect(res.err).toBe('');
  });

  it('still uses proper exit codes', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-quiet-'));
    const file = join(dir, 'ok.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--quiet', '--check', file]);
    expect(res.code).toBe(0);
    expect(res.err).toBe('');
  });

  it('is mutually exclusive with --verbose', () => {
    const res = runCli(['--verbose', '--quiet']);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--verbose and --quiet cannot be used together');
  });
});

describe('--ignore flag', () => {
  it('excludes files matching ignore pattern', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-ignore-'));
    const migrations = join(dir, 'migrations');
    mkdirSync(migrations);
    writeFileSync(join(dir, 'query.sql'), 'select 1;', 'utf8');
    writeFileSync(join(migrations, 'v1.sql'), 'select 2;', 'utf8');

    const res = runCli([
      '--write',
      '--ignore', 'migrations/**',
      join(dir, '**/*.sql'),
    ]);
    expect(res.code).toBe(0);
    // query.sql should be formatted
    expect(readFileSync(join(dir, 'query.sql'), 'utf8')).toBe('SELECT 1;\n');
    // migrations/v1.sql should be untouched
    expect(readFileSync(join(migrations, 'v1.sql'), 'utf8')).toBe('select 2;');
  });

  it('supports multiple --ignore patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlfmt-ignore-'));
    const vendor = join(dir, 'vendor');
    const migrations = join(dir, 'migrations');
    mkdirSync(vendor);
    mkdirSync(migrations);
    writeFileSync(join(dir, 'app.sql'), 'select 1;', 'utf8');
    writeFileSync(join(vendor, 'lib.sql'), 'select 2;', 'utf8');
    writeFileSync(join(migrations, 'v1.sql'), 'select 3;', 'utf8');

    const res = runCli([
      '--write',
      '--ignore', 'vendor/**',
      '--ignore', 'migrations/**',
      join(dir, '**/*.sql'),
    ]);
    expect(res.code).toBe(0);
    expect(readFileSync(join(dir, 'app.sql'), 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(join(vendor, 'lib.sql'), 'utf8')).toBe('select 2;');
    expect(readFileSync(join(migrations, 'v1.sql'), 'utf8')).toBe('select 3;');
  });

  it('errors when --ignore has no argument', () => {
    const res = runCli(['--ignore']);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--ignore requires');
  });
});

describe('--stdin-filepath flag', () => {
  it('shows filepath in error messages when reading from stdin', () => {
    const res = runCliWithStdin(
      ['--stdin-filepath', 'query.sql'],
      "SELECT 'unterminated",
    );
    expect(res.code).toBe(2);
    expect(res.err).toContain('query.sql');
  });

  it('formats stdin normally when no error', () => {
    const res = runCliWithStdin(
      ['--stdin-filepath', 'query.sql'],
      'select 1;',
    );
    expect(res.code).toBe(0);
    expect(res.out).toContain('SELECT 1;');
  });

  it('errors when --stdin-filepath has no argument', () => {
    const res = runCli(['--stdin-filepath']);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--stdin-filepath requires');
  });
});

describe('--help flag', () => {
  it('shows all new flags in help output', () => {
    const res = runCli(['--help']);
    expect(res.code).toBe(0);
    expect(res.out).toContain('--verbose');
    expect(res.out).toContain('--quiet');
    expect(res.out).toContain('--ignore');
    expect(res.out).toContain('--stdin-filepath');
  });
});
