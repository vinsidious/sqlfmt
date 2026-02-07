import { randomBytes } from 'crypto';
import { readFileSync, writeFileSync, renameSync, unlinkSync, globSync } from 'fs';
import { dirname, join, resolve, isAbsolute, relative } from 'path';
import { formatSQL } from './format';
import { ParseError } from './parser';
import { TokenizeError } from './tokenizer';

class CLIUsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CLIUsageError';
  }
}

type ColorMode = 'auto' | 'always' | 'never';

interface CLIOptions {
  check: boolean;
  write: boolean;
  diff: boolean;
  help: boolean;
  version: boolean;
  listDifferent: boolean;
  colorMode: ColorMode;
  verbose: boolean;
  quiet: boolean;
  ignore: string[];
  stdinFilepath: string | null;
  files: string[];
}

// ANSI color helpers — disabled by NO_COLOR env, --no-color flag, or non-TTY stderr
const RESET = '\x1b[0m';
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const BOLD = '\x1b[1m';

let colorEnabled = true;

function initColor(opts: CLIOptions): void {
  if (opts.colorMode === 'always') {
    colorEnabled = true;
    return;
  }
  if (opts.colorMode === 'never') {
    colorEnabled = false;
    return;
  }
  colorEnabled = process.env.NO_COLOR === undefined && !!process.stderr.isTTY;
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
  try {
    const scriptPath = process.argv[1] || process.cwd();
    const pkgPath = join(dirname(scriptPath), '..', 'package.json');
    const pkg = JSON.parse(readFileSync(pkgPath, 'utf8')) as { version?: string };
    return pkg.version ?? '0.0.0';
  } catch {
    return '0.0.0';
  }
}

function printHelp(): void {
  console.log(`sqlfmt - An opinionated SQL formatter

  sqlfmt formats SQL using river alignment, following the
  SQL style guide at https://www.sqlstyle.guide/. Keywords
  are right-aligned to form a "river" of whitespace, making
  queries easier to scan.
  Zero-config by design: no .sqlfmtrc, no --init, no style flags.

Usage: sqlfmt [options] [file ...]

  File arguments support glob patterns (e.g. **/*.sql).

Options:
  -h, --help            Show this help text
  -v, --version         Show version

Formatting:
  --check               Exit 1 when input is not formatted
  --diff                Show unified diff when --check fails
  -w, --write           Write formatted output back to input file(s)
  -l, --list-different  Print only filenames that need formatting

File selection:
  --ignore <pattern>    Exclude files matching glob pattern (repeatable)
                        Also reads patterns from .sqlfmtignore if present
  --stdin-filepath <p>  Path shown in error messages when reading stdin

Output:
  --verbose             Print progress details to stderr
  --quiet               Suppress all output except errors
  --no-color            Alias for --color=never
  --color <mode>        Colorize output: auto|always|never (default: auto)

Examples:
  sqlfmt query.sql
  sqlfmt --check --diff "db/**/*.sql"
  sqlfmt -w one.sql two.sql
  sqlfmt --write --ignore "migrations/**" "**/*.sql"
  cat query.sql | sqlfmt
  cat query.sql | sqlfmt --stdin-filepath query.sql
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

function parseColorModeArg(value: string): ColorMode {
  if (value === 'auto' || value === 'always' || value === 'never') {
    return value;
  }
  throw new CLIUsageError(`--color must be one of: auto, always, never (got '${value}')`);
}

function parseArgs(args: string[]): CLIOptions {
  const opts: CLIOptions = {
    check: false,
    write: false,
    diff: false,
    help: false,
    version: false,
    listDifferent: false,
    colorMode: 'auto',
    verbose: false,
    quiet: false,
    ignore: [],
    stdinFilepath: null,
    files: [],
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

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
    if (arg.startsWith('--color=')) {
      opts.colorMode = parseColorModeArg(arg.slice('--color='.length));
      continue;
    }
    if (arg === '--color') {
      const next = args[i + 1];
      if (next === undefined || next.startsWith('-')) {
        throw new CLIUsageError('--color requires one of: auto, always, never');
      }
      opts.colorMode = parseColorModeArg(next);
      i++;
      continue;
    }
    if (arg === '--no-color') {
      opts.colorMode = 'never';
      continue;
    }
    if (arg === '--verbose') {
      opts.verbose = true;
      continue;
    }
    if (arg === '--quiet') {
      opts.quiet = true;
      continue;
    }
    if (arg === '--ignore') {
      const next = args[i + 1];
      if (next === undefined || next.startsWith('-')) {
        throw new CLIUsageError('--ignore requires a glob pattern argument');
      }
      const normalized = normalizeGlobPath(next);
      if (normalized.length > MAX_IGNORE_PATTERN_LENGTH) {
        throw new CLIUsageError(
          `--ignore pattern is too long (${normalized.length} > ${MAX_IGNORE_PATTERN_LENGTH})`
        );
      }
      opts.ignore.push(next);
      i++;
      continue;
    }
    if (arg === '--stdin-filepath') {
      const next = args[i + 1];
      if (next === undefined || next.startsWith('-')) {
        throw new CLIUsageError('--stdin-filepath requires a path argument');
      }
      opts.stdinFilepath = next;
      i++;
      continue;
    }

    if (arg.startsWith('-')) {
      throw new CLIUsageError(`Unknown option: ${arg}`);
    }

    opts.files.push(arg);
  }

  if (opts.verbose && opts.quiet) {
    throw new CLIUsageError('--verbose and --quiet cannot be used together');
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

class NoFilesMatchedError extends Error {
  readonly pattern: string;
  constructor(pattern: string) {
    super(`No files matched pattern: '${pattern}'`);
    this.name = 'NoFilesMatchedError';
    this.pattern = pattern;
  }
}

// Filter out files in .git/, node_modules/, and dotfiles/dotdirs from glob results
const EXCLUDED_PATH_RE = /(?:^|[/\\])(?:\.git|node_modules)(?:[/\\]|$)/;
const DOTFILE_SEGMENT_RE = /(?:^|[/\\])\.[^./\\]/;

function shouldExcludeFromGlob(filepath: string): boolean {
  return EXCLUDED_PATH_RE.test(filepath) || DOTFILE_SEGMENT_RE.test(filepath);
}

// Cap total file count from glob expansion to prevent runaway matches
const MAX_GLOB_FILES = 10_000;

function expandGlobs(files: string[]): string[] {
  const result: string[] = [];
  for (const f of files) {
    if (!isGlobPattern(f)) {
      result.push(f);
      continue;
    }
    try {
      const matches = globSync(f).filter(m => !shouldExcludeFromGlob(m));
      if (matches.length === 0) {
        throw new NoFilesMatchedError(f);
      }
      result.push(...matches.sort());
      if (result.length > MAX_GLOB_FILES) {
        throw new CLIUsageError(
          `Too many files matched (${result.length} > ${MAX_GLOB_FILES}). Narrow your glob pattern.`
        );
      }
    } catch (err) {
      if (err instanceof NoFilesMatchedError) throw err;
      if (err instanceof CLIUsageError) throw err;
      // globSync not available or failed — treat as literal path
      result.push(f);
    }
  }
  return result;
}

type IgnoreGlobToken =
  | { type: 'literal'; value: string }
  | { type: 'star' }
  | { type: 'doubleStar' }
  | { type: 'question' };

const MAX_IGNORE_PATTERN_LENGTH = 1024;
const IGNORE_FILE_NAME = '.sqlfmtignore';

function normalizeGlobPath(input: string): string {
  return input
    .replace(/\\/g, '/')
    .replace(/^\.\//, '')
    .replace(/\/+/g, '/');
}

function tokenizeIgnorePattern(pattern: string): IgnoreGlobToken[] {
  const normalized = normalizeGlobPath(pattern);
  const tokens: IgnoreGlobToken[] = [];
  for (let i = 0; i < normalized.length; i++) {
    const ch = normalized[i];
    if (ch === '*') {
      if (normalized[i + 1] === '*') {
        tokens.push({ type: 'doubleStar' });
        i++;
        while (normalized[i + 1] === '*') i++;
      } else {
        tokens.push({ type: 'star' });
      }
      continue;
    }
    if (ch === '?') {
      tokens.push({ type: 'question' });
      continue;
    }
    tokens.push({ type: 'literal', value: ch });
  }
  return tokens;
}

function readIgnorePatternsFromFile(ignoreFilePath: string): string[] {
  const fileContent = readFileSync(ignoreFilePath, 'utf8');
  const patterns: string[] = [];
  for (const rawLine of fileContent.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const normalized = normalizeGlobPath(line);
    if (!normalized) continue;
    if (normalized.length > MAX_IGNORE_PATTERN_LENGTH) {
      throw new CLIUsageError(
        `${IGNORE_FILE_NAME} contains a pattern that is too long (${normalized.length} > ${MAX_IGNORE_PATTERN_LENGTH})`
      );
    }
    patterns.push(normalized);
  }
  return patterns;
}

function loadIgnorePatterns(cwd: string): string[] {
  const ignoreFilePath = join(cwd, IGNORE_FILE_NAME);
  try {
    return readIgnorePatternsFromFile(ignoreFilePath);
  } catch (err) {
    const ioErr = err as NodeJS.ErrnoException;
    if (ioErr?.code === 'ENOENT') return [];
    if (err instanceof CLIUsageError) throw err;
    throw new CLIUsageError(`Failed to read ${IGNORE_FILE_NAME}: ${ioErr?.message ?? String(err)}`);
  }
}

function matchGlobTokens(path: string, tokens: IgnoreGlobToken[]): boolean {
  const memo = new Map<string, boolean>();
  const normalizedPath = normalizeGlobPath(path);

  const dfs = (pathIndex: number, tokenIndex: number): boolean => {
    const key = `${pathIndex}:${tokenIndex}`;
    const cached = memo.get(key);
    if (cached !== undefined) return cached;

    if (tokenIndex >= tokens.length) {
      const done = pathIndex >= normalizedPath.length;
      memo.set(key, done);
      return done;
    }

    const token = tokens[tokenIndex];
    let matched = false;

    if (token.type === 'literal') {
      matched =
        pathIndex < normalizedPath.length
        && normalizedPath[pathIndex] === token.value
        && dfs(pathIndex + 1, tokenIndex + 1);
    } else if (token.type === 'question') {
      matched =
        pathIndex < normalizedPath.length
        && normalizedPath[pathIndex] !== '/'
        && dfs(pathIndex + 1, tokenIndex + 1);
    } else if (token.type === 'star') {
      matched = dfs(pathIndex, tokenIndex + 1);
      if (!matched && pathIndex < normalizedPath.length && normalizedPath[pathIndex] !== '/') {
        matched = dfs(pathIndex + 1, tokenIndex);
      }
    } else {
      matched = dfs(pathIndex, tokenIndex + 1);
      if (!matched && pathIndex < normalizedPath.length) {
        matched = dfs(pathIndex + 1, tokenIndex);
      }
    }

    memo.set(key, matched);
    return matched;
  };

  return dfs(0, 0);
}

function splitPathSuffixes(filepath: string): string[] {
  const normalized = normalizeGlobPath(filepath);
  const segments = normalized.split('/');
  if (segments.length === 0) return [''];

  const suffixes: string[] = [];
  for (let i = 0; i < segments.length; i++) {
    const subpath = segments.slice(i).join('/');
    if (subpath) suffixes.push(subpath);
  }
  return suffixes.length > 0 ? suffixes : [''];
}

function matchesAnyIgnorePattern(filepath: string, patterns: string[]): boolean {
  const suffixes = splitPathSuffixes(filepath);
  for (const pattern of patterns) {
    const normalizedPattern = normalizeGlobPath(pattern);
    if (!normalizedPattern) continue;
    if (normalizedPattern.length > MAX_IGNORE_PATTERN_LENGTH) {
      throw new CLIUsageError(
        `Ignore pattern is too long (${normalizedPattern.length} > ${MAX_IGNORE_PATTERN_LENGTH})`
      );
    }
    const tokens = tokenizeIgnorePattern(normalizedPattern);
    for (const subpath of suffixes) {
      if (matchGlobTokens(subpath, tokens)) return true;
    }
  }
  return false;
}

function isInsideDirectory(baseDir: string, targetPath: string): boolean {
  const relPath = relative(baseDir, targetPath);
  return relPath === '' || (!relPath.startsWith('..') && !isAbsolute(relPath));
}

// Validate that a relative file path doesn't escape the current working directory via traversal.
// Returns the resolved absolute path, or null if a relative path resolves outside CWD.
function validateWritePath(file: string): string | null {
  const resolved = resolve(file);
  // Only enforce CWD containment for relative paths, where ".." traversal is a concern.
  // Absolute paths (from glob expansion or explicit user input) are trusted as-is.
  if (!isAbsolute(file)) {
    const cwd = process.cwd();
    if (!isInsideDirectory(cwd, resolved)) {
      return null;
    }
  }
  return resolved;
}

// Write a file atomically: write to a temp file first, then rename.
// This prevents partial writes from corrupting the original file.
function atomicWriteFileSync(file: string, content: string): void {
  const suffix = randomBytes(8).toString('hex');
  const tmpFile = `${file}.sqlfmt.${suffix}.tmp`;
  try {
    writeFileSync(tmpFile, content, 'utf-8');
    renameSync(tmpFile, file);
  } catch (err) {
    try { unlinkSync(tmpFile); } catch { /* ignore cleanup errors */ }
    throw err;
  }
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

function formatErrorExcerpt(
  input: string,
  line: number,
  column: number,
  message: string,
  filepath?: string | null,
): string {
  const sourceLine = getSourceLine(input, line);
  const caret = ' '.repeat(Math.max(0, column - 1)) + '^';
  const location = filepath
    ? `${filepath}:${line}:${column}:`
    : `Parse error at line ${line}, column ${column}:`;
  return red(location) + `\n\n  ${sourceLine}\n  ${caret}\n  ${message}`;
}

function handleParseError(err: unknown, input?: string, filepath?: string | null): never {
  if (err instanceof ParseError) {
    if (input) {
      console.error(formatErrorExcerpt(input, err.line, err.column, err.message, filepath));
    } else {
      console.error(red(`Parse error at line ${err.line}, column ${err.column}: ${err.message}`));
    }
    process.exit(2);
  }
  if (err instanceof TokenizeError) {
    if (input) {
      console.error(formatErrorExcerpt(input, err.line, err.column, err.message, filepath));
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
    const cwd = process.cwd();
    const fileIgnorePatterns = loadIgnorePatterns(cwd);

    let expandedFiles = expandGlobs(opts.files);
    const allIgnorePatterns = [...fileIgnorePatterns, ...opts.ignore];

    if (opts.verbose && fileIgnorePatterns.length > 0) {
      console.error(`Loaded ${fileIgnorePatterns.length} pattern(s) from ${IGNORE_FILE_NAME}`);
    }

    // Apply .sqlfmtignore and --ignore patterns
    if (allIgnorePatterns.length > 0 && expandedFiles.length > 0) {
      expandedFiles = expandedFiles.filter(f => !matchesAnyIgnorePattern(f, allIgnorePatterns));
    }

    let checkFailures = 0;
    let changedCount = 0;

    if (expandedFiles.length === 0 && opts.files.length === 0) {
      // stdin mode
      const input = readFileSync(0, 'utf-8');
      let output: string;
      try {
        output = formatOneInput(input);
      } catch (err) {
        handleParseError(err, input, opts.stdinFilepath);
      }

      if (opts.check) {
        const normalizedInput = normalizeForComparison(input);
        if (normalizedInput !== output) {
          checkFailures++;
          if (!opts.quiet) {
            console.error(red('Input is not formatted.'));
          }
          if (opts.diff && !opts.quiet) {
            console.error(unifiedDiff(normalizedInput, output));
          }
        }
      } else if (!opts.quiet) {
        process.stdout.write(output);
      }
    } else {
      if (opts.verbose) {
        console.error(`Formatting ${expandedFiles.length} file${expandedFiles.length === 1 ? '' : 's'}...`);
      }

      for (const file of expandedFiles) {
        if (opts.verbose) {
          console.error(file);
        }

        const input = readFileSync(file, 'utf-8');

        let output: string;
        try {
          output = formatOneInput(input);
        } catch (err) {
          handleParseError(err, input);
        }

        if (opts.write) {
          if (input !== output) {
            const validPath = validateWritePath(file);
            if (validPath === null) {
              console.error(red(`Warning: skipping '${file}' — path resolves outside working directory`));
            } else {
              atomicWriteFileSync(validPath, output);
              changedCount++;
            }
          }
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
            if (!opts.quiet) {
              console.error(red(`${file}: not formatted.`));
            }
            if (opts.diff && !opts.quiet) {
              console.error(unifiedDiff(normalizedInput, output));
            }
          }
          continue;
        }

        if (!opts.quiet) {
          process.stdout.write(output);
        }
      }

      if (opts.verbose) {
        console.error(`Formatted ${expandedFiles.length} file${expandedFiles.length === 1 ? '' : 's'} (${changedCount} changed)`);
      }
    }

    if (opts.check && checkFailures === 0 && expandedFiles.length > 0) {
      if (!opts.quiet) {
        console.error(green('All files are formatted.'));
      }
    }

    if (checkFailures > 0) {
      process.exit(1);
    }
  } catch (err) {
    if (err instanceof CLIUsageError || err instanceof NoFilesMatchedError) {
      console.error(red(err.message));
      process.exit(1);
    }

    if (err instanceof ParseError || err instanceof TokenizeError) {
      handleParseError(err);
    }

    const ioErr = err as NodeJS.ErrnoException | undefined;
    if (ioErr?.code === 'ENOENT' || ioErr?.code === 'EISDIR') {
      console.error(red(`I/O error: ${ioErr.message}`));
      process.exit(1);
    }

    console.error(red(`Unexpected error: ${err instanceof Error ? err.message : String(err)}`));
    process.exit(1);
  }
}

main();
