import { readFileSync, writeFileSync } from 'fs';
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
  files: string[];
}

function readVersion(): string {
  const scriptPath = process.argv[1] || process.cwd();
  const pkgPath = join(dirname(scriptPath), '..', 'package.json');
  const pkg = JSON.parse(readFileSync(pkgPath, 'utf8')) as { version?: string };
  return pkg.version ?? '0.0.0';
}

function printHelp(): void {
  console.log('sqlfmt - SQL formatter');
  console.log('');
  console.log('Usage: sqlfmt [options] [file ...]');
  console.log('');
  console.log('Options:');
  console.log('  -h, --help      Show this help text');
  console.log('  -v, --version   Show version');
  console.log('  --check         Exit 1 when input is not formatted');
  console.log('  --diff          Show unified diff when --check fails');
  console.log('  -w, --write     Write formatted output back to input file(s)');
  console.log('');
  console.log('Examples:');
  console.log('  sqlfmt query.sql');
  console.log('  sqlfmt --check schema.sql');
  console.log('  sqlfmt --check --diff query.sql');
  console.log('  sqlfmt -w one.sql two.sql');
  console.log('  cat query.sql | sqlfmt');
  console.log('');
  console.log('Exit codes:');
  console.log('  0 success');
  console.log('  1 check failure / usage / I/O error');
  console.log('  2 parse/tokenize error');
  console.log('');
  console.log('Docs: https://github.com/vinsidious/sqlfmt');
}

function parseArgs(args: string[]): CLIOptions {
  const opts: CLIOptions = {
    check: false,
    write: false,
    diff: false,
    help: false,
    version: false,
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

  return opts;
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
      body.push(`-${a[i]}`);
      i++;
    } else {
      body.push(`+${b[j]}`);
      j++;
    }
  }
  while (i < n) {
    body.push(`-${a[i]}`);
    i++;
  }
  while (j < m) {
    body.push(`+${b[j]}`);
    j++;
  }

  return [
    '--- input',
    '+++ formatted',
    `@@ -1,${n} +1,${m} @@`,
    ...body,
  ].join('\n');
}

function formatOneInput(input: string): string {
  return formatSQL(input);
}

function handleParseError(err: unknown): never {
  if (err instanceof ParseError) {
    const pos = err.token.position >= 0 ? ` at position ${err.token.position}` : '';
    console.error(`Parse error${pos}: ${err.message}`);
    process.exit(2);
  }
  if (err instanceof TokenizeError) {
    console.error(`Parse error at position ${err.position}: ${err.message}`);
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

    let checkFailures = 0;

    if (opts.files.length === 0) {
      const input = readFileSync(0, 'utf-8');
      let output: string;
      try {
        output = formatOneInput(input);
      } catch (err) {
        handleParseError(err);
      }

      if (opts.check) {
        const normalizedInput = normalizeForComparison(input);
        if (normalizedInput !== output) {
          checkFailures++;
          console.error('Input is not formatted.');
          if (opts.diff) {
            console.error(unifiedDiff(normalizedInput, output));
          }
        }
      } else {
        process.stdout.write(output);
      }
    } else {
      for (const file of opts.files) {
        const input = readFileSync(file, 'utf-8');

        let output: string;
        try {
          output = formatOneInput(input);
        } catch (err) {
          handleParseError(err);
        }

        if (opts.write) {
          if (input !== output) writeFileSync(file, output, 'utf-8');
          continue;
        }

        if (opts.check) {
          const normalizedInput = normalizeForComparison(input);
          if (normalizedInput !== output) {
            checkFailures++;
            console.error(`${file}: not formatted.`);
            if (opts.diff) {
              console.error(unifiedDiff(normalizedInput, output));
            }
          }
          continue;
        }

        process.stdout.write(output);
      }
    }

    if (checkFailures > 0) {
      process.exit(1);
    }
  } catch (err) {
    if (err instanceof CLIUsageError) {
      console.error(err.message);
      process.exit(1);
    }

    const ioErr = err as NodeJS.ErrnoException;
    if (ioErr && typeof ioErr === 'object' && (ioErr.code === 'ENOENT' || ioErr.code === 'EISDIR')) {
      console.error(`I/O error: ${ioErr.message}`);
      process.exit(1);
    }

    handleParseError(err);
    throw err;
  }
}

main();
