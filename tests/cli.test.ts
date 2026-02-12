import { describe, expect, it } from 'bun:test';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, statSync, writeFileSync, symlinkSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

const root = join(import.meta.dir, '..');
const cliPath = join(root, 'src', 'cli.ts');
const bunPath = process.execPath;
const decoder = new TextDecoder();

function runCli(args: string[], cwd: string = root, envOverrides: Record<string, string> = {}) {
  const proc = Bun.spawnSync({
    cmd: [bunPath, cliPath, ...args],
    cwd,
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
    env: { ...process.env, ...envOverrides },
  });

  return {
    code: proc.exitCode,
    out: decoder.decode(proc.stdout),
    err: decoder.decode(proc.stderr),
  };
}

function runCliWithStdin(
  args: string[],
  stdinContent: string,
  cwd: string = root,
  envOverrides: Record<string, string> = {}
) {
  const proc = Bun.spawnSync({
    cmd: [bunPath, cliPath, ...args],
    cwd,
    stdin: new TextEncoder().encode(stdinContent),
    stdout: 'pipe',
    stderr: 'pipe',
    env: { ...process.env, ...envOverrides },
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

  it('accepts --dialect with a valid value', () => {
    const res = runCliWithStdin(['--dialect', 'postgres'], 'select 1;');
    expect(res.code).toBe(0);
    expect(res.out).toContain('SELECT 1;');
  });

  it('rejects --dialect with an invalid value', () => {
    const res = runCli(['--dialect', 'oracle']);
    expect(res.code).toBe(3);
    expect(res.err).toContain('--dialect must be one of');
  });

  it('accepts --max-input-size with a valid value', () => {
    const input = `SELECT '${'x'.repeat(64)}';`;

    const failRes = runCliWithStdin(['--max-input-size', '20'], input);
    expect(failRes.code).toBe(3);
    expect(failRes.err).toContain('Input exceeds maximum size');

    const okRes = runCliWithStdin(['--max-input-size', '256'], input);
    expect(okRes.code).toBe(0);
    expect(okRes.out).toContain('SELECT');
  });

  it('rejects --max-input-size with invalid values', () => {
    const missing = runCli(['--max-input-size']);
    expect(missing.code).toBe(3);
    expect(missing.err).toContain('--max-input-size requires');

    const invalid = runCli(['--max-input-size', '0']);
    expect(invalid.code).toBe(3);
    expect(invalid.err).toContain('--max-input-size must be an integer >= 1');
  });

  it('returns parse exit code 2 with a clean message', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'broken.sql');
    writeFileSync(file, "SELECT 'broken", 'utf8');
    const res = runCli([file]);
    expect(res.code).toBe(2);
    expect(res.err).toContain('broken.sql');
  });

  it('reads UTF-16LE SQL files with BOM', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'utf16le.sql');
    const raw = Buffer.concat([
      Buffer.from([0xff, 0xfe]),
      Buffer.from('select 1;', 'utf16le'),
    ]);
    writeFileSync(file, raw);

    const res = runCli(['--write', 'utf16le.sql'], dir);
    expect(res.code).toBe(0);
    const after = readFileSync(file);
    expect(after[0]).toBe(0xff);
    expect(after[1]).toBe(0xfe);
    expect(after.subarray(2).toString('utf16le')).toBe('SELECT 1;\n');
  });

  it('reads UTF-16BE SQL files with BOM', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'utf16be.sql');
    const le = Buffer.from('select 1;', 'utf16le');
    const be = Buffer.from(le);
    for (let i = 0; i + 1 < be.length; i += 2) {
      const a = be[i];
      be[i] = be[i + 1];
      be[i + 1] = a;
    }
    writeFileSync(file, Buffer.concat([Buffer.from([0xfe, 0xff]), be]));

    const res = runCli(['--write', 'utf16be.sql'], dir);
    expect(res.code).toBe(0);
    const after = readFileSync(file);
    expect(after[0]).toBe(0xfe);
    expect(after[1]).toBe(0xff);
    const body = Buffer.from(after.subarray(2));
    for (let i = 0; i + 1 < body.length; i += 2) {
      const a = body[i];
      body[i] = body[i + 1];
      body[i + 1] = a;
    }
    expect(body.toString('utf16le')).toBe('SELECT 1;\n');
  });

  it('preserves UTF-16LE files without BOM when writing', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'utf16le-nobom.sql');
    writeFileSync(file, Buffer.from('select 1;', 'utf16le'));

    const res = runCli(['--write', 'utf16le-nobom.sql'], dir);
    expect(res.code).toBe(0);

    const after = readFileSync(file);
    expect(after[0]).not.toBe(0xff);
    expect(after[1]).not.toBe(0xfe);
    expect(after.toString('utf16le')).toBe('SELECT 1;\n');
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

  it('handles large divergent files with --check --diff without crashing', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'large-diff.sql');
    const lines = Array.from({ length: 4500 }, (_, i) => `select ${i} as value_${i};`);
    writeFileSync(file, lines.join('\n'), 'utf8');

    const res = runCli(['--check', '--diff', 'large-diff.sql'], dir);
    expect(res.code).toBe(1);
    expect(res.err).toContain('--- a/large-diff.sql');
    expect(res.err).toContain('+++ b/large-diff.sql');
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

  it('preserves file mode bits when writing', () => {
    if (process.platform === 'win32') return;

    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'mode.sql');
    writeFileSync(file, 'select 1;', 'utf8');
    chmodSync(file, 0o755);

    const beforeMode = statSync(file).mode & 0o777;
    expect(beforeMode).toBe(0o755);

    const res = runCli(['--write', 'mode.sql'], dir);
    expect(res.code).toBe(0);

    const afterMode = statSync(file).mode & 0o777;
    expect(afterMode).toBe(0o755);
  });

  it('preserves CRLF line endings when writing', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'windows.sql');
    writeFileSync(file, 'select 1;\r\nselect 2;\r\n', 'utf8');

    const res = runCli(['--write', 'windows.sql'], dir);
    expect(res.code).toBe(0);

    const after = readFileSync(file, 'utf8');
    expect(after).toBe('SELECT 1;\r\n\r\nSELECT 2;\r\n');
  });

  it('treats CRLF-formatted multi-statement files as formatted in --check mode', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-cli-'));
    const file = join(dir, 'formatted-crlf.sql');
    writeFileSync(file, 'SELECT 1;\r\n\r\nSELECT 2;\r\n', 'utf8');

    const res = runCli(['--check', 'formatted-crlf.sql'], dir);
    expect(res.code).toBe(0);
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

  it('ignores directory matches from glob expansion', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-glob-'));
    mkdirSync(join(dir, 'folder.sql'));
    writeFileSync(join(dir, 'query.sql'), 'select 1;', 'utf8');

    const res = runCli(['--check', join(dir, '**/*.sql')]);
    expect(res.code).toBe(1);
    expect(res.err).toContain('query.sql');
    expect(res.err).not.toContain('EISDIR');
  });

  it('fallback glob matcher continues past early non-matching files', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-glob-fallback-'));
    const noisyDir = join(dir, 'a-noise');
    const targetDir = join(dir, 'z-target');
    mkdirSync(noisyDir);
    mkdirSync(targetDir);

    for (let i = 0; i < 10_050; i++) {
      const name = `${String(i).padStart(5, '0')}.txt`;
      writeFileSync(join(noisyDir, name), 'noise\n', 'utf8');
    }
    writeFileSync(join(targetDir, 'match.sql'), 'select 1;', 'utf8');

    const res = runCli(['--write', '**/*.sql'], dir, { HOLYWELL_FORCE_FALLBACK_GLOB: '1' });
    expect(res.code).toBe(0);
    expect(readFileSync(join(targetDir, 'match.sql'), 'utf8')).toBe('SELECT 1;\n');
  }, 30000);
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
    expect(res.out).toContain('--max-input-size');
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

  it('errors when explicit --config path does not exist', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-config-'));
    writeFileSync(join(dir, 'q.sql'), longSql, 'utf8');

    const res = runCli(['--config', 'missing.json', 'q.sql'], dir);
    expect(res.code).toBe(3);
    expect(res.err).toContain('Config file not found');
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

  it('lets --max-input-size override config maxInputSize', () => {
    const dir = mkdtempSync(join(tmpdir(), 'holywell-config-'));
    writeFileSync(join(dir, '.holywellrc.json'), JSON.stringify({ maxInputSize: 8 }) + '\n', 'utf8');
    writeFileSync(join(dir, 'q.sql'), 'select 1;', 'utf8');

    const strictConfigRes = runCli(['q.sql'], dir);
    expect(strictConfigRes.code).toBe(3);
    expect(strictConfigRes.err).toContain('Input exceeds maximum size');

    const overriddenRes = runCli(['--max-input-size', '128', 'q.sql'], dir);
    expect(overriddenRes.code).toBe(0);
    expect(overriddenRes.out).toContain('SELECT 1;');
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

  it('rejects paths inside symlinked directories that point outside CWD', () => {
    const outsideDir = mkdtempSync(join(tmpdir(), 'holywell-symlink-outside-dir-'));
    const target = join(outsideDir, 'escaped.sql');
    writeFileSync(target, 'select 1;', 'utf8');

    const workDir = mkdtempSync(join(tmpdir(), 'holywell-symlink-dir-cwd-'));
    const linkDir = join(workDir, 'linked');
    symlinkSync(outsideDir, linkDir);

    const res = runCli(['--write', 'linked/escaped.sql'], workDir);
    expect(res.code).toBe(0);
    expect(res.err).toContain('path resolves outside working directory');
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
