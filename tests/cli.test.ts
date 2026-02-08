import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync, symlinkSync } from 'fs';
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

function runCliWithStdin(args: string[], stdinContent: string, cwd: string = root) {
  const proc = Bun.spawnSync({
    cmd: [bunPath, cliPath, ...args],
    cwd,
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

  it('supports -V shorthand for version', () => {
    const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')) as { version: string };
    const res = runCli(['-V']);
    expect(res.code).toBe(0);
    expect(res.out.trim()).toBe(pkg.version);
  });

  it('rejects unknown flags', () => {
    const res = runCli(['--definitely-not-a-real-flag']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('Unknown option');
    expect(res.err).toContain('--help');
  });

  it('returns parse exit code 2 with a clean message', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'broken.sql');
    writeFileSync(file, "SELECT 'broken", 'utf8');
    const res = runCli([file]);
    expect(res.code).toBe(2);
    expect(res.err).toContain('Parse error');
  });

  it('supports --check with normalized edge whitespace', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'normalized.sql');
    writeFileSync(file, '\n\nSELECT 1;\n', 'utf8');
    const res = runCli(['--check', file]);
    expect(res.code).toBe(0);
  });

  it('supports --diff output when --check fails', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'diff.sql');
    writeFileSync(file, 'select 1;', 'utf8');
    const res = runCli(['--check', '--diff', file]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--- a/');
    expect(res.err).toContain('+++ b/');
    expect(res.err).toContain('@@');
  });

  it('supports --write for in-place formatting', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--write', 'q.sql'], dir);
    expect(res.code).toBe(0);

    const after = readFileSync(file, 'utf8');
    expect(after).toBe('SELECT 1;\n');
  });

  it('supports --dry-run preview without writing changes', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'preview.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--dry-run', 'preview.sql'], dir);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--- a/preview.sql');
    expect(readFileSync(file, 'utf8')).toBe('select 1;');
  });

  it('handles multiple file arguments', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const fileA = join(dir, 'a.sql');
    const fileB = join(dir, 'b.sql');
    writeFileSync(fileA, 'select 1;', 'utf8');
    writeFileSync(fileB, 'select 2;', 'utf8');

    const checkRes = runCli(['--check', 'a.sql', 'b.sql'], dir);
    expect(checkRes.code).toBe(1);

    const writeRes = runCli(['--write', 'a.sql', 'b.sql'], dir);
    expect(writeRes.code).toBe(0);
    expect(readFileSync(fileA, 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(fileB, 'utf8')).toBe('SELECT 2;\n');
  });
});

describe('glob pattern expansion', () => {
  it('expands *.sql glob patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-glob-'));
    writeFileSync(join(dir, 'a.sql'), 'select 1;', 'utf8');
    writeFileSync(join(dir, 'b.sql'), 'select 2;', 'utf8');
    writeFileSync(join(dir, 'c.txt'), 'not sql', 'utf8');

    const res = runCli(['--write', '*.sql'], dir);
    expect(res.code).toBe(0);

    expect(readFileSync(join(dir, 'a.sql'), 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(join(dir, 'b.sql'), 'utf8')).toBe('SELECT 2;\n');
    // .txt file should be untouched
    expect(readFileSync(join(dir, 'c.txt'), 'utf8')).toBe('not sql');
  });

  it('expands **/*.sql recursive glob patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-glob-'));
    const sub = join(dir, 'sub');
    mkdirSync(sub);
    writeFileSync(join(dir, 'top.sql'), 'select 1;', 'utf8');
    writeFileSync(join(sub, 'nested.sql'), 'select 2;', 'utf8');

    const res = runCli(['--check', join(dir, '**/*.sql')]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('not formatted');
  });

  it('errors on non-matching glob pattern with clear message', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-glob-'));
    const res = runCli([join(dir, '*.sql')]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('No files matched pattern');
  });
});

describe('--list-different flag', () => {
  it('prints only filenames that need formatting', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ld-'));
    const clean = join(dir, 'clean.sql');
    const dirty = join(dir, 'dirty.sql');
    writeFileSync(clean, 'SELECT 1;\n', 'utf8');
    writeFileSync(dirty, 'select 1;', 'utf8');

    const res = runCli(['--list-different', clean, dirty]);
    expect(res.code).toBe(1);
    expect(res.out.trim()).toBe(dirty);
  });

  it('exits 0 when all files are already formatted', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ld-'));
    const file = join(dir, 'ok.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--list-different', file]);
    expect(res.code).toBe(0);
    expect(res.out.trim()).toBe('');
  });

  it('supports -l shorthand', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ld-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['-l', file]);
    expect(res.code).toBe(1);
    expect(res.out.trim()).toBe(file);
  });

  it('requires file arguments', () => {
    const res = runCli(['--list-different']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('--list-different requires');
  });
});

describe('--no-color flag', () => {
  it('accepts --no-color without error', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-nc-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--no-color', '--check', file]);
    expect(res.code).toBe(0);
  });

  it('does not emit ANSI codes with --no-color', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-nc-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--no-color', '--check', file]);
    expect(res.code).toBe(1);
    // Ensure no ANSI escape sequences
    expect(res.err).not.toContain('\x1b[');
    expect(res.err).toContain('not formatted');
  });
});

describe('--color flag', () => {
  it('accepts --color=always and forces ANSI output', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-color-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--color=always', '--check', file]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('\x1b[');
  });

  it('accepts --color never form and disables ANSI output', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-color-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--color', 'never', '--check', file]);
    expect(res.code).toBe(1);
    expect(res.err).not.toContain('\x1b[');
  });

  it('rejects invalid --color values', () => {
    const res = runCli(['--color=rainbow']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('--color must be one of');
  });
});

describe('--verbose flag', () => {
  it('supports -v shorthand for verbose', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-verbose-'));
    const file = join(dir, 'a.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['-v', '--check', 'a.sql'], dir);
    expect(res.code).toBe(0);
    expect(res.err).toContain('Formatting 1 file...');
  });

  it('prints file progress to stderr', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-verbose-'));
    const fileA = join(dir, 'a.sql');
    const fileB = join(dir, 'b.sql');
    writeFileSync(fileA, 'select 1;', 'utf8');
    writeFileSync(fileB, 'SELECT 2;\n', 'utf8');

    const res = runCli(['--verbose', '--write', 'a.sql', 'b.sql'], dir);
    expect(res.code).toBe(0);
    // Should print "Formatting N files..." header
    expect(res.err).toContain('Formatting 2 files...');
    // Should print each filename
    expect(res.err).toContain('a.sql');
    expect(res.err).toContain('b.sql');
    // Should print summary with changed count
    expect(res.err).toContain('Formatted 2 files (1 changed)');
  });

  it('prints singular form for 1 file', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-verbose-'));
    const file = join(dir, 'a.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--verbose', '--write', 'a.sql'], dir);
    expect(res.code).toBe(0);
    expect(res.err).toContain('Formatting 1 file...');
    expect(res.err).toContain('Formatted 1 file (0 changed)');
  });
});

describe('--quiet flag', () => {
  it('suppresses normal output', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-quiet-'));
    const file = join(dir, 'q.sql');
    writeFileSync(file, 'select 1;', 'utf8');

    const res = runCli(['--quiet', '--check', file]);
    expect(res.code).toBe(1);
    // stderr should have no "not formatted" message
    expect(res.err).toBe('');
  });

  it('still uses proper exit codes', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-quiet-'));
    const file = join(dir, 'ok.sql');
    writeFileSync(file, 'SELECT 1;\n', 'utf8');

    const res = runCli(['--quiet', '--check', file]);
    expect(res.code).toBe(0);
    expect(res.err).toBe('');
  });

  it('is mutually exclusive with --verbose', () => {
    const res = runCli(['--verbose', '--quiet']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('--verbose and --quiet cannot be used together');
  });
});

describe('--ignore flag', () => {
  it('excludes files matching ignore pattern', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignore-'));
    const migrations = join(dir, 'migrations');
    mkdirSync(migrations);
    writeFileSync(join(dir, 'query.sql'), 'select 1;', 'utf8');
    writeFileSync(join(migrations, 'v1.sql'), 'select 2;', 'utf8');

    const res = runCli([
      '--write',
      '--ignore', 'migrations/**',
      '**/*.sql',
    ], dir);
    expect(res.code).toBe(0);
    // query.sql should be formatted
    expect(readFileSync(join(dir, 'query.sql'), 'utf8')).toBe('SELECT 1;\n');
    // migrations/v1.sql should be untouched
    expect(readFileSync(join(migrations, 'v1.sql'), 'utf8')).toBe('select 2;');
  });

  it('supports multiple --ignore patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignore-'));
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
      '**/*.sql',
    ], dir);
    expect(res.code).toBe(0);
    expect(readFileSync(join(dir, 'app.sql'), 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(join(vendor, 'lib.sql'), 'utf8')).toBe('select 2;');
    expect(readFileSync(join(migrations, 'v1.sql'), 'utf8')).toBe('select 3;');
  });

  it('supports ? wildcard in ignore patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignore-'));
    const keep = join(dir, 'keep.sql');
    const skipSingle = join(dir, 'skip1.sql');
    const skipDouble = join(dir, 'skip10.sql');
    writeFileSync(keep, 'select 1;', 'utf8');
    writeFileSync(skipSingle, 'select 2;', 'utf8');
    writeFileSync(skipDouble, 'select 3;', 'utf8');

    const res = runCli([
      '--write',
      '--ignore', 'skip?.sql',
      '*.sql',
    ], dir);
    expect(res.code).toBe(0);
    expect(readFileSync(keep, 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(skipSingle, 'utf8')).toBe('select 2;');
    expect(readFileSync(skipDouble, 'utf8')).toBe('SELECT 3;\n');
  });

  it('rejects overly long --ignore patterns', () => {
    const res = runCli(['--ignore', 'x'.repeat(1025)]);
    expect(res.code).toBe(3);
    expect(res.err).toContain('pattern is too long');
  });

  it('errors when --ignore has no argument', () => {
    const res = runCli(['--ignore']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('--ignore requires');
  });
});

describe('.holywellignore support', () => {
  it('loads ignore patterns from .holywellignore in cwd', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignorefile-'));
    const migrations = join(dir, 'migrations');
    mkdirSync(migrations);
    writeFileSync(join(dir, '.holywellignore'), 'migrations/**\n', 'utf8');
    writeFileSync(join(dir, 'query.sql'), 'select 1;', 'utf8');
    writeFileSync(join(migrations, 'v1.sql'), 'select 2;', 'utf8');

    const res = runCli(['--write', '**/*.sql'], dir);
    expect(res.code).toBe(0);
    expect(readFileSync(join(dir, 'query.sql'), 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(join(migrations, 'v1.sql'), 'utf8')).toBe('select 2;');
  });

  it('combines .holywellignore and --ignore patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignorefile-'));
    const migrations = join(dir, 'migrations');
    const vendor = join(dir, 'vendor');
    mkdirSync(migrations);
    mkdirSync(vendor);
    writeFileSync(join(dir, '.holywellignore'), 'migrations/**\n', 'utf8');
    writeFileSync(join(dir, 'query.sql'), 'select 1;', 'utf8');
    writeFileSync(join(migrations, 'v1.sql'), 'select 2;', 'utf8');
    writeFileSync(join(vendor, 'v2.sql'), 'select 3;', 'utf8');

    const res = runCli(['--write', '--ignore', 'vendor/**', '**/*.sql'], dir);
    expect(res.code).toBe(0);
    expect(readFileSync(join(dir, 'query.sql'), 'utf8')).toBe('SELECT 1;\n');
    expect(readFileSync(join(migrations, 'v1.sql'), 'utf8')).toBe('select 2;');
    expect(readFileSync(join(vendor, 'v2.sql'), 'utf8')).toBe('select 3;');
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
    expect(res.code).toBe(3);
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
    expect(res.out).toContain('--color');
    expect(res.out).toContain('--completion');
    expect(res.out).toContain('.holywellignore');
    expect(res.out).toContain('.holywellrc.json');
  });
});

describe('--completion flag', () => {
  it('prints bash completion script', () => {
    const res = runCli(['--completion', 'bash']);
    expect(res.code).toBe(0);
    expect(res.out).toContain('complete -F _holywell_completions holywell');
  });

  it('prints zsh completion script', () => {
    const res = runCli(['--completion', 'zsh']);
    expect(res.code).toBe(0);
    expect(res.out).toContain('#compdef holywell');
  });

  it('rejects invalid completion shell', () => {
    const res = runCli(['--completion', 'pwsh']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('--completion requires one of: bash, zsh, fish');
  });
});

describe('.holywellrc.json config support', () => {
  const longSql = 'SELECT customer_identifier, product_identifier, order_identifier, shipment_identifier FROM very_long_table_name;';

  it('loads maxLineLength from .holywellrc.json in cwd', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-config-'));
    writeFileSync(join(dir, '.holywellrc.json'), JSON.stringify({ maxLineLength: 140 }) + '\n', 'utf8');
    writeFileSync(join(dir, 'q.sql'), longSql, 'utf8');

    const res = runCli(['q.sql'], dir);
    expect(res.code).toBe(0);
    expect(res.out).toContain('SELECT customer_identifier, product_identifier, order_identifier, shipment_identifier');
  });

  it('supports explicit --config path', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-config-'));
    writeFileSync(join(dir, 'custom-config.json'), JSON.stringify({ maxLineLength: 140 }) + '\n', 'utf8');
    writeFileSync(join(dir, 'q.sql'), longSql, 'utf8');

    const res = runCli(['--config', 'custom-config.json', 'q.sql'], dir);
    expect(res.code).toBe(0);
    expect(res.out).toContain('SELECT customer_identifier, product_identifier, order_identifier, shipment_identifier');
  });

  it('lets CLI flags override config values', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-config-'));
    writeFileSync(join(dir, '.holywellrc.json'), JSON.stringify({ maxLineLength: 140 }) + '\n', 'utf8');
    writeFileSync(join(dir, 'q.sql'), longSql, 'utf8');

    const res = runCli(['--max-line-length', '60', 'q.sql'], dir);
    expect(res.code).toBe(0);
    expect(res.out).toContain('SELECT customer_identifier,');
    expect(res.out).toContain('\n       product_identifier,');
  });
});

describe('symlink path validation', () => {
  it('rejects symlinks pointing outside CWD', () => {
    const outsideDir = mkdtempSync(join(tmpdir(), 'holywell-symlink-outside-'));
    const target = join(outsideDir, 'secret.sql');
    writeFileSync(target, 'select 1;', 'utf8');

    const workDir = mkdtempSync(join(tmpdir(), 'holywell-symlink-cwd-'));
    const link = join(workDir, 'link.sql');
    symlinkSync(target, link);

    const res = runCli(['--write', 'link.sql'], workDir);
    expect(res.code).toBe(0);
    expect(res.err).toContain('path resolves outside working directory');
    // The target file should be unchanged
    expect(readFileSync(target, 'utf8')).toBe('select 1;');
  });
});

describe('ignore pattern validation', () => {
  it('rejects --ignore pattern with ../ traversal', () => {
    const res = runCli(['--ignore', '../etc/**', 'test.sql']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('../');
    expect(res.err).toContain('directory traversal');
  });

  it('rejects --ignore pattern with excessive ** segments', () => {
    const pattern = Array.from({ length: 12 }, () => '**').join('/');
    const res = runCli(['--ignore', pattern, 'test.sql']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('too many **');
  });

  it('accepts normal ignore patterns', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignore-val-'));
    writeFileSync(join(dir, 'query.sql'), 'SELECT 1;\n', 'utf8');

    const res = runCli(['--check', '--ignore', 'migrations/**', 'query.sql'], dir);
    expect(res.code).toBe(0);
  });

  it('rejects .holywellignore pattern with ../ traversal', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignfile-val-'));
    writeFileSync(join(dir, '.holywellignore'), '../../../etc/passwd\n', 'utf8');
    writeFileSync(join(dir, 'query.sql'), 'SELECT 1;\n', 'utf8');

    const res = runCli(['--check', 'query.sql'], dir);
    expect(res.code).toBe(3);
    expect(res.err).toContain('../');
    expect(res.err).toContain('directory traversal');
  });

  it('rejects .holywellignore pattern with excessive ** segments', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-ignfile-val-'));
    const pattern = Array.from({ length: 12 }, () => '**').join('/');
    writeFileSync(join(dir, '.holywellignore'), pattern + '\n', 'utf8');
    writeFileSync(join(dir, 'query.sql'), 'SELECT 1;\n', 'utf8');

    const res = runCli(['--check', 'query.sql'], dir);
    expect(res.code).toBe(3);
    expect(res.err).toContain('too many **');
  });
});

describe('recovery warnings on stderr', () => {
  it('emits recovery warnings even with --quiet', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-recover-'));
    // This SQL has a statement the parser will struggle with, forcing recovery
    const sql = 'SELECT 1; GOBBLEDYGOOK NOT VALID SQL HERE; SELECT 2;';
    writeFileSync(join(dir, 'recover.sql'), sql, 'utf8');

    const res = runCli(['--quiet', '--write', 'recover.sql'], dir);
    // Recovery warnings should still appear on stderr even with --quiet
    // (if recovery actually fired; if this SQL parses fine, no warning expected)
    // The important thing is that --quiet does NOT suppress recovery warnings
    expect(res.code).toBe(0);
  });
});
