import { readFileSync, writeFileSync, globSync } from 'fs';
import { dirname, join } from 'path';
import { formatSQL } from './format';
import { ParseError } from './parser';
import { TokenizeError } from './tokenizer';

class CLIUsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CLIUsageError';
  }
}

interface CLIOptions {
  check: boolean;
  write: boolean;
  diff: boolean;
  help: boolean;
  version: boolean;
  listDifferent: boolean;
  noColor: boolean;
  files: string[];
}

// ANSI color helpers — disabled by NO_COLOR env, --no-color flag, or non-TTY stderr
const RESET = '\x1b[0m';
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const BOLD = '\x1b[1m';

let colorEnabled = true;

function initColor(opts: CLIOptions): void {
  if (opts.noColor || process.env.NO_COLOR !== undefined || !process.stderr.isTTY) {
    colorEnabled = false;
  }
}

function red(s: string): string {
  return colorEnabled ? `${RED}${s}${RESET}` : s;
}

function green(s: string): string {
  return colorEnabled ? `${GREEN}${s}${RESET}` : s;
}

function bold(s: string): string {
  return colorEnabled ? `${BOLD}${s}${RESET}` : s;
}

function readVersion(): string {
  const scriptPath = process.argv[1] || process.cwd();
  const pkgPath = join(dirname(scriptPath), '..', 'package.json');
  const pkg = JSON.parse(readFileSync(pkgPath, 'utf8')) as { version?: string };
  return pkg.version ?? '0.0.0';
}

function printHelp(): void {
  console.log(`sqlfmt - An opinionated SQL formatter

  sqlfmt formats SQL using river alignment, following the
  SQL style guide at https://www.sqlstyle.guide/. Keywords
  are right-aligned to form a "river" of whitespace, making
  queries easier to scan.

Usage: sqlfmt [options] [file ...]

  File arguments support glob patterns (e.g. **/*.sql).

Options:
  -h, --help            Show this help text
  -v, --version         Show version
  --check               Exit 1 when input is not formatted
  --diff                Show unified diff when --check fails
  -w, --write           Write formatted output back to input file(s)
  -l, --list-different  Print only filenames that need formatting
  --no-color            Disable colored output

Examples:
  sqlfmt query.sql
  sqlfmt --check --diff "db/**/*.sql"
  sqlfmt -w one.sql two.sql
  cat query.sql | sqlfmt
  echo "SELECT 1;" | sqlfmt

  # Pipe from another command
  pg_dump mydb --schema-only | sqlfmt

  # Pre-commit one-liner (exits 1 if any file needs formatting)
  sqlfmt --check $(git diff --cached --name-only -- '*.sql')

Exit codes:
  0  Success (or all files already formatted with --check)
  1  Check failure / usage error / I/O error
  2  Parse or tokenize error

Docs: https://github.com/vinsidious/sqlfmt`);
}

function parseArgs(args: string[]): CLIOptions {
  const opts: CLIOptions = {
    check: false,
    write: false,
    diff: false,
    help: false,
    version: false,
    listDifferent: false,
    noColor: false,
    files: [],
  };

  for (const arg of args) {
    if (arg === '--check') {
      opts.check = true;
      continue;
    }
    if (arg === '--diff') {
      opts.diff = true;
      continue;
    }
    if (arg === '--write' || arg === '-w') {
      opts.write = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      opts.help = true;
      continue;
    }
    if (arg === '--version' || arg === '-v') {
      opts.version = true;
      continue;
    }
    if (arg === '--list-different' || arg === '-l') {
      opts.listDifferent = true;
      continue;
    }
    if (arg === '--no-color') {
      opts.noColor = true;
      continue;
    }

    if (arg.startsWith('-')) {
      throw new CLIUsageError(`Unknown option: ${arg}`);
    }

    opts.files.push(arg);
  }

  if (opts.diff && !opts.check) {
    throw new CLIUsageError('--diff can only be used with --check');
  }

  if (opts.write && opts.check) {
    throw new CLIUsageError('--write and --check cannot be used together');
  }

  if (opts.write && opts.files.length === 0) {
    throw new CLIUsageError('--write requires at least one input file');
  }

  if (opts.listDifferent && opts.files.length === 0) {
    throw new CLIUsageError('--list-different requires at least one input file');
  }

  if (opts.listDifferent && opts.write) {
    throw new CLIUsageError('--list-different and --write cannot be used together');
  }

  return opts;
}

const GLOB_CHARS = /[*?{\[]/;

function isGlobPattern(arg: string): boolean {
  return GLOB_CHARS.test(arg);
}

function expandGlobs(files: string[]): string[] {
  const result: string[] = [];
  for (const f of files) {
    if (!isGlobPattern(f)) {
      result.push(f);
      continue;
    }
    try {
      const matches = globSync(f);
      if (matches.length === 0) {
        // No matches — treat as literal path (will error on read)
        result.push(f);
      } else {
        result.push(...matches.sort());
      }
    } catch {
      // globSync not available or failed — treat as literal path
      result.push(f);
    }
  }
  return result;
}

function normalizeForComparison(input: string): string {
  const trimmed = input.trim();
  if (!trimmed) return '';
  return trimmed + '\n';
}

function toLines(text: string): string[] {
  const lines = text.split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  return lines;
}

function unifiedDiff(aText: string, bText: string): string {
  const a = toLines(aText);
  const b = toLines(bText);
  const n = a.length;
  const m = b.length;

  const dp: number[][] = Array.from({ length: n + 1 }, () => Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }

  const body: string[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      body.push(` ${a[i]}`);
      i++;
      j++;
      continue;
    }
    if (dp[i + 1][j] >= dp[i][j + 1]) {
      body.push(red(`-${a[i]}`));
      i++;
    } else {
      body.push(green(`+${b[j]}`));
      j++;
    }
  }
  while (i < n) {
    body.push(red(`-${a[i]}`));
    i++;
  }
  while (j < m) {
    body.push(green(`+${b[j]}`));
    j++;
  }

  return [
    bold('--- input'),
    bold('+++ formatted'),
    `@@ -1,${n} +1,${m} @@`,
    ...body,
  ].join('\n');
}

function formatOneInput(input: string): string {
  return formatSQL(input);
}

function getSourceLine(input: string, line: number): string {
  const lines = input.split('\n');
  if (line >= 1 && line <= lines.length) return lines[line - 1];
  return '';
}

function formatErrorExcerpt(input: string, line: number, column: number, message: string): string {
  const sourceLine = getSourceLine(input, line);
  const caret = ' '.repeat(Math.max(0, column - 1)) + '^';
  return red(`Parse error at line ${line}, column ${column}:`) + `\n\n  ${sourceLine}\n  ${caret}\n  ${message}`;
}

function handleParseError(err: unknown, input?: string): never {
  if (err instanceof ParseError) {
    if (input) {
      console.error(formatErrorExcerpt(input, err.line, err.column, err.message));
    } else {
      console.error(red(`Parse error at line ${err.line}, column ${err.column}: ${err.message}`));
    }
    process.exit(2);
  }
  if (err instanceof TokenizeError) {
    if (input) {
      console.error(formatErrorExcerpt(input, err.line, err.column, err.message));
    } else {
      console.error(red(`Parse error at line ${err.line}, column ${err.column}: ${err.message}`));
    }
    process.exit(2);
  }
  throw err;
}

function main(): void {
  try {
    const opts = parseArgs(process.argv.slice(2));

    if (opts.version) {
      console.log(readVersion());
      process.exit(0);
    }

    if (opts.help) {
      printHelp();
      process.exit(0);
    }

    initColor(opts);

    const expandedFiles = expandGlobs(opts.files);
    let checkFailures = 0;

    if (expandedFiles.length === 0 && opts.files.length === 0) {
      const input = readFileSync(0, 'utf-8');
      let output: string;
      try {
        output = formatOneInput(input);
      } catch (err) {
        handleParseError(err, input);
      }

      if (opts.check) {
        const normalizedInput = normalizeForComparison(input);
        if (normalizedInput !== output) {
          checkFailures++;
          console.error(red('Input is not formatted.'));
          if (opts.diff) {
            console.error(unifiedDiff(normalizedInput, output));
          }
        }
      } else {
        process.stdout.write(output);
      }
    } else {
      for (const file of expandedFiles) {
        const input = readFileSync(file, 'utf-8');

        let output: string;
        try {
          output = formatOneInput(input);
        } catch (err) {
          handleParseError(err, input);
        }

        if (opts.write) {
          if (input !== output) writeFileSync(file, output, 'utf-8');
          continue;
        }

        if (opts.listDifferent) {
          const normalizedInput = normalizeForComparison(input);
          if (normalizedInput !== output) {
            checkFailures++;
            console.log(file);
          }
          continue;
        }

        if (opts.check) {
          const normalizedInput = normalizeForComparison(input);
          if (normalizedInput !== output) {
            checkFailures++;
            console.error(red(`${file}: not formatted.`));
            if (opts.diff) {
              console.error(unifiedDiff(normalizedInput, output));
            }
          }
          continue;
        }

        process.stdout.write(output);
      }
    }

    if (opts.check && checkFailures === 0 && expandedFiles.length > 0) {
      console.error(green('All files are formatted.'));
    }

    if (checkFailures > 0) {
      process.exit(1);
    }
  } catch (err) {
    if (err instanceof CLIUsageError) {
      console.error(red(err.message));
      process.exit(1);
    }

    const ioErr = err as NodeJS.ErrnoException;
    if (ioErr && typeof ioErr === 'object' && (ioErr.code === 'ENOENT' || ioErr.code === 'EISDIR')) {
      console.error(red(`I/O error: ${ioErr.message}`));
      process.exit(1);
    }

    handleParseError(err);
    throw err;
  }
}

main();
