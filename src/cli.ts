import { randomBytes } from 'crypto';
import { readFileSync, writeFileSync, renameSync, unlinkSync, globSync, lstatSync, realpathSync } from 'fs';
import { dirname, join, resolve, isAbsolute, relative } from 'path';
import { formatSQL } from './format';
import { ParseError } from './parser';
import { TokenizeError } from './tokenizer';
import type { RawExpression } from './ast';

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
  dryRun: boolean;
  help: boolean;
  version: boolean;
  listDifferent: boolean;
  colorMode: ColorMode;
  verbose: boolean;
  quiet: boolean;
  strict: boolean;
  ignore: string[];
  stdinFilepath: string | null;
  configPath: string | null;
  completionShell: 'bash' | 'zsh' | 'fish' | null;
  maxLineLength?: number;
  files: string[];
}

interface RuntimeFormatOptions {
  recover: boolean;
  maxLineLength?: number;
  maxDepth?: number;
  maxInputSize?: number;
}

interface CLIConfigFile {
  maxLineLength?: number;
  maxDepth?: number;
  maxInputSize?: number;
  strict?: boolean;
  recover?: boolean;
}

// Recovery event collected during formatting
interface RecoveryEvent {
  line: number;
  message: string;
  dropped?: boolean;
  statementIndex?: number;
  totalStatements?: number;
}

// ANSI color helpers — disabled by NO_COLOR env, --no-color flag, or non-TTY stderr
const RESET = '\x1b[0m';
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const BOLD = '\x1b[1m';
const DIM = '\x1b[2m';

let colorEnabled = true;

const EXIT_SUCCESS = 0;
const EXIT_CHECK_FAILURE = 1;
const EXIT_PARSE_ERROR = 2;
const EXIT_USAGE_OR_IO_ERROR = 3;

function initColor(opts: CLIOptions): void {
  if (opts.colorMode === 'always') {
    colorEnabled = true;
    return;
  }
  if (opts.colorMode === 'never') {
    colorEnabled = false;
    return;
  }
  const runningInCI = process.env.CI !== undefined || process.env.GITHUB_ACTIONS !== undefined;
  colorEnabled = process.env.NO_COLOR === undefined && !!process.stderr.isTTY && !runningInCI;
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

function dim(s: string): string {
  return colorEnabled ? `${DIM}${s}${RESET}` : s;
}

// Injected at build time by tsup's `define` option from package.json.
declare const __HOLYWELL_VERSION__: string | undefined;

function readVersion(): string {
  if (typeof __HOLYWELL_VERSION__ !== 'undefined') {
    return __HOLYWELL_VERSION__;
  }
  // Fallback for development (running via bun/ts-node without build)
  try {
    const scriptDir = dirname(new URL(import.meta.url).pathname);
    const pkgPath = join(scriptDir, '..', 'package.json');
    const pkg = JSON.parse(readFileSync(pkgPath, 'utf8')) as { version?: string };
    return pkg.version ?? '0.0.0';
  } catch {
    return '0.0.0';
  }
}

function printHelp(): void {
  console.log(`holywell - An opinionated SQL formatter

  holywell formats SQL using river alignment, following the
  SQL style guide at https://www.sqlstyle.guide/. Keywords
  are right-aligned to form a "river" of whitespace, making
  queries easier to scan.
  Deterministic by design: defaults are opinionated, with optional project config.

Usage: holywell [options] [file ...]

  File arguments support glob patterns (e.g. **/*.sql).

Options:
  -h, --help            Show this help text
  -V, --version         Show version

Formatting:
  --check               Exit 1 when input is not formatted
  --diff                Show unified diff when --check fails
  --dry-run             Preview changes without writing (implies --check --diff)
  --preview             Alias for --dry-run
  -w, --write           Write formatted output back to input file(s)
  -l, --list-different  Print only filenames that need formatting
  --max-line-length <n> Preferred output line width (default: 80)
  --strict              Disable parser recovery; exit 2 on parse errors
                        (recommended for CI)

File selection:
  --ignore <pattern>    Exclude files matching glob pattern (repeatable)
                        Also reads patterns from .holywellignore if present
  --config <path>       Use an explicit config file (default: .holywellrc.json)
  --stdin-filepath <p>  Path shown in error messages when reading stdin

Output:
  -v, --verbose         Print progress details to stderr
  --quiet               Suppress all output except errors
  --no-color            Alias for --color=never
  --color <mode>        Colorize output: auto|always|never (default: auto)
  --completion <shell>  Print shell completion script (bash|zsh|fish)

Examples:
  holywell query.sql
  holywell --check --diff "db/**/*.sql"
  holywell -w one.sql two.sql
  holywell --write --ignore "migrations/**" "**/*.sql"
  holywell --strict --check "**/*.sql"
  cat query.sql | holywell
  cat query.sql | holywell --stdin-filepath query.sql
  echo "SELECT 1;" | holywell

  # Pipe from another command
  pg_dump mydb --schema-only | holywell

  # Pre-commit one-liner (exits 1 if any file needs formatting)
  holywell --check $(git diff --cached --name-only -- '*.sql')

Exit codes:
  0  Success (or all files already formatted with --check)
  1  Check failure
  2  Parse or tokenize error
  3  Usage or I/O error

Docs: https://github.com/vinsidious/holywell`);
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
    dryRun: false,
    help: false,
    version: false,
    listDifferent: false,
    colorMode: 'auto',
    verbose: false,
    quiet: false,
    strict: false,
    ignore: [],
    stdinFilepath: null,
    configPath: null,
    completionShell: null,
    maxLineLength: undefined,
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
    if (arg === '--dry-run' || arg === '--preview') {
      opts.dryRun = true;
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
    if (arg === '--version' || arg === '-V') {
      opts.version = true;
      continue;
    }
    if (arg === '--list-different' || arg === '-l') {
      opts.listDifferent = true;
      continue;
    }
    if (arg === '--strict') {
      opts.strict = true;
      continue;
    }
    if (arg === '--max-line-length') {
      const next = args[i + 1];
      if (next === undefined || next.startsWith('-')) {
        throw new CLIUsageError('--max-line-length requires a numeric argument');
      }
      const parsed = Number(next);
      if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 40) {
        throw new CLIUsageError('--max-line-length must be an integer >= 40');
      }
      opts.maxLineLength = parsed;
      i++;
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
    if (arg === '--verbose' || arg === '-v') {
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
      validateIgnorePattern(normalized, '--ignore');
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
    if (arg === '--config') {
      const next = args[i + 1];
      if (next === undefined || next.startsWith('-')) {
        throw new CLIUsageError('--config requires a path argument');
      }
      opts.configPath = next;
      i++;
      continue;
    }
    if (arg === '--completion') {
      const next = args[i + 1];
      if (next !== 'bash' && next !== 'zsh' && next !== 'fish') {
        throw new CLIUsageError('--completion requires one of: bash, zsh, fish');
      }
      opts.completionShell = next;
      i++;
      continue;
    }

    if (arg.startsWith('-')) {
      throw new CLIUsageError(`Unknown option: ${arg}. Run --help for usage.`);
    }

    opts.files.push(arg);
  }

  if (opts.verbose && opts.quiet) {
    throw new CLIUsageError('--verbose and --quiet cannot be used together');
  }

  if (opts.dryRun) {
    opts.check = true;
    opts.diff = true;
  }

  if (opts.diff && !opts.check) {
    throw new CLIUsageError('--diff can only be used with --check');
  }

  if (opts.dryRun && opts.write) {
    throw new CLIUsageError('--dry-run and --write cannot be used together');
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

function renderCompletionScript(shell: 'bash' | 'zsh' | 'fish'): string {
  const options = [
    '--help', '--version', '--check', '--diff', '--dry-run', '--preview',
    '--write', '--list-different', '--max-line-length', '--strict', '--ignore',
    '--config', '--stdin-filepath', '--verbose', '--quiet', '--no-color',
    '--color', '--completion',
    '-h', '-V', '-w', '-l', '-v',
  ];
  const joined = options.join(' ');

  if (shell === 'bash') {
    return `# bash completion for holywell
_holywell_completions() {
  local cur="\${COMP_WORDS[COMP_CWORD]}"
  COMPREPLY=( $(compgen -W "${joined}" -- "$cur") )
}
complete -F _holywell_completions holywell`;
  }

  if (shell === 'zsh') {
    const quoted = options.map(opt => `'${opt}'`).join(' ');
    return `#compdef holywell
_holywell_completions() {
  local -a opts
  opts=(${quoted})
  _describe 'holywell option' opts
}
compdef _holywell_completions holywell`;
  }

  return `# fish completion for holywell
${options.map(opt => `complete -c holywell -f -a '${opt}'`).join('\n')}`;
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
const MAX_DOUBLE_STAR_SEGMENTS = 10;
const IGNORE_FILE_NAME = '.holywellignore';
const CONFIG_FILE_NAME = '.holywellrc.json';

function validateIgnorePattern(pattern: string, source: string): void {
  if (pattern.includes('../')) {
    throw new CLIUsageError(
      `${source} pattern must not contain '../' (directory traversal): '${pattern}'`
    );
  }
  const segments = pattern.split('/');
  let doubleStarCount = 0;
  for (const seg of segments) {
    if (seg === '**') doubleStarCount++;
  }
  if (doubleStarCount > MAX_DOUBLE_STAR_SEGMENTS) {
    throw new CLIUsageError(
      `${source} pattern has too many ** segments (${doubleStarCount} > ${MAX_DOUBLE_STAR_SEGMENTS}): '${pattern}'`
    );
  }
}

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
    validateIgnorePattern(normalized, IGNORE_FILE_NAME);
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

function validateConfigShape(raw: unknown, sourcePath: string): CLIConfigFile {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new CLIUsageError(`${sourcePath} must contain a JSON object`);
  }
  const obj = raw as Record<string, unknown>;
  const cfg: CLIConfigFile = {};

  if (obj.maxLineLength !== undefined) {
    const value = obj.maxLineLength;
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 40) {
      throw new CLIUsageError(`${sourcePath}: maxLineLength must be an integer >= 40`);
    }
    cfg.maxLineLength = value;
  }
  if (obj.maxDepth !== undefined) {
    const value = obj.maxDepth;
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 1) {
      throw new CLIUsageError(`${sourcePath}: maxDepth must be an integer >= 1`);
    }
    cfg.maxDepth = value;
  }
  if (obj.maxInputSize !== undefined) {
    const value = obj.maxInputSize;
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 1) {
      throw new CLIUsageError(`${sourcePath}: maxInputSize must be an integer >= 1`);
    }
    cfg.maxInputSize = value;
  }
  if (obj.strict !== undefined) {
    if (typeof obj.strict !== 'boolean') {
      throw new CLIUsageError(`${sourcePath}: strict must be a boolean`);
    }
    cfg.strict = obj.strict;
  }
  if (obj.recover !== undefined) {
    if (typeof obj.recover !== 'boolean') {
      throw new CLIUsageError(`${sourcePath}: recover must be a boolean`);
    }
    cfg.recover = obj.recover;
  }
  return cfg;
}

function loadConfig(cwd: string, explicitPath: string | null): CLIConfigFile {
  const configPath = explicitPath ? resolve(cwd, explicitPath) : join(cwd, CONFIG_FILE_NAME);
  try {
    const content = readFileSync(configPath, 'utf8');
    const parsed = JSON.parse(content) as unknown;
    return validateConfigShape(parsed, explicitPath ? configPath : CONFIG_FILE_NAME);
  } catch (err) {
    const ioErr = err as NodeJS.ErrnoException;
    if (ioErr?.code === 'ENOENT') return {};
    if (err instanceof CLIUsageError) throw err;
    if (err instanceof SyntaxError) {
      throw new CLIUsageError(`Invalid JSON in ${explicitPath ? configPath : CONFIG_FILE_NAME}: ${err.message}`);
    }
    throw new CLIUsageError(`Failed to read ${explicitPath ? configPath : CONFIG_FILE_NAME}: ${ioErr?.message ?? String(err)}`);
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

// Validate that a file path doesn't escape the current working directory via traversal.
// Returns the resolved absolute path, or null if the path resolves outside CWD.
// Both relative and absolute paths are checked for CWD containment.
// Symlinks inside CWD that point outside CWD are also rejected.
function validateWritePath(file: string): string | null {
  const resolved = resolve(file);
  const cwd = process.cwd();
  // Enforce CWD containment for both relative and absolute paths.
  if (!isInsideDirectory(cwd, resolved)) {
    return null;
  }
  // Detect symlinks that escape CWD
  try {
    const stats = lstatSync(resolved);
    if (stats.isSymbolicLink()) {
      const realTarget = realpathSync(resolved);
      if (!isInsideDirectory(cwd, realTarget)) {
        return null; // Symlink points outside CWD
      }
    }
  } catch {
    // File doesn't exist yet; safe to write
  }
  return resolved;
}

// Write a file atomically: write to a temp file first, then rename.
// This prevents partial writes from corrupting the original file.
function atomicWriteFileSync(file: string, content: string): void {
  const suffix = randomBytes(8).toString('hex');
  const tmpFile = `${file}.holywell.${suffix}.tmp`;
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

function unifiedDiff(aText: string, bText: string, aLabel: string = 'a/input.sql', bLabel: string = 'b/formatted.sql'): string {
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
    bold(`--- ${aLabel}`),
    bold(`+++ ${bLabel}`),
    `@@ -1,${n} +1,${m} @@`,
    ...body,
  ].join('\n');
}

function formatOneInput(input: string, options: RuntimeFormatOptions): { output: string; recoveries: RecoveryEvent[] } {
  const recoveries: RecoveryEvent[] = [];
  const output = formatSQL(input, {
    recover: options.recover,
    maxLineLength: options.maxLineLength,
    maxDepth: options.maxDepth,
    maxInputSize: options.maxInputSize,
    onRecover: (error: ParseError, raw: RawExpression | null, context) => {
      if (!raw) return;
      recoveries.push({
        line: error.line,
        message: error.message,
        statementIndex: context.statementIndex,
        totalStatements: context.totalStatements,
      });
    },
    onDropStatement: (error: ParseError, context) => {
      recoveries.push({
        line: error.line,
        message: error.message,
        dropped: true,
        statementIndex: context.statementIndex,
        totalStatements: context.totalStatements,
      });
    },
  });
  return { output, recoveries };
}

function getSourceLines(input: string): string[] {
  return input.split('\n');
}

function formatErrorExcerpt(
  input: string,
  line: number,
  column: number,
  message: string,
  filepath?: string | null,
): string {
  const allLines = getSourceLines(input);
  const location = filepath
    ? `${filepath}:${line}:${column}:`
    : `Parse error at line ${line}, column ${column}:`;

  // Show +/-2 lines of context around the error line
  const startLine = Math.max(1, line - 2);
  const endLine = Math.min(allLines.length, line + 2);
  const gutterWidth = String(endLine).length;

  const contextLines: string[] = [];
  for (let i = startLine; i <= endLine; i++) {
    const lineContent = i >= 1 && i <= allLines.length ? allLines[i - 1] : '';
    const lineNum = String(i).padStart(gutterWidth, ' ');
    const prefix = i === line ? '>' : ' ';
    contextLines.push(`  ${prefix} ${dim(lineNum)} | ${lineContent}`);
    if (i === line) {
      const padding = ' '.repeat(gutterWidth);
      const caret = ' '.repeat(Math.max(0, column - 1)) + '^';
      contextLines.push(`    ${padding} | ${caret}`);
    }
  }

  return red(location) + '\n\n' + contextLines.join('\n') + '\n  ' + message;
}

// Sanitize token values in error messages to avoid leaking sensitive data (passwords, API keys).
// Replaces string literal and parameter token values with placeholders.
function sanitizeErrorMessage(err: ParseError | TokenizeError): string {
  if (err instanceof ParseError && (err.token.type === 'string' || err.token.type === 'parameter')) {
    const placeholder = `<${err.token.type}>`;
    const got = `"${placeholder}" (${err.token.type})`;
    return `Expected ${err.expected}, got ${got}`;
  }
  return err.message;
}

// Check if an error message already contains position info like "at line X, column Y"
const POSITION_INFO_RE = /at line \d+,? column \d+/i;

function handleParseError(err: unknown, input?: string, filepath?: string | null): never {
  if (err instanceof ParseError) {
    const safeMessage = sanitizeErrorMessage(err);
    if (input) {
      console.error(formatErrorExcerpt(input, err.line, err.column, safeMessage, filepath));
    } else {
      console.error(red(`Parse error at line ${err.line}, column ${err.column}: ${safeMessage}`));
    }
    process.exit(EXIT_PARSE_ERROR);
  }
  if (err instanceof TokenizeError) {
    const safeMessage = sanitizeErrorMessage(err);
    if (input) {
      // TokenizeError message may already include position info; use the message as-is
      // but show the excerpt with context at the correct location
      const displayMessage = POSITION_INFO_RE.test(safeMessage)
        ? safeMessage
        : `${safeMessage} at line ${err.line}, column ${err.column}`;
      console.error(formatErrorExcerpt(input, err.line, err.column, displayMessage, filepath));
    } else {
      console.error(red(`Parse error at line ${err.line}, column ${err.column}: ${safeMessage}`));
    }
    process.exit(EXIT_PARSE_ERROR);
  }
  throw err;
}

// Recovery warnings always go to stderr, even with --quiet, because they indicate
// that the output may not faithfully represent the input SQL.
function printRecoveryWarnings(recoveries: RecoveryEvent[]): void {
  if (recoveries.length === 0) return;

  for (const r of recoveries) {
    const stmtLabel = r.statementIndex && r.totalStatements
      ? `statement ${r.statementIndex}/${r.totalStatements}`
      : `statement at line ${r.line}`;
    if (r.dropped) {
      console.error(`Warning: ${stmtLabel} was dropped (could not recover)`);
    } else {
      console.error(`Warning: ${stmtLabel} could not be parsed and was passed through as-is`);
    }
  }
  const droppedCount = recoveries.filter(r => r.dropped).length;
  const passthroughCount = recoveries.length - droppedCount;
  const parts: string[] = [];
  if (passthroughCount > 0) parts.push(`${passthroughCount} passed through`);
  if (droppedCount > 0) parts.push(`${droppedCount} dropped`);
  console.error(`Warning: ${recoveries.length} statement(s) could not be parsed (${parts.join(', ')})`);
}

function main(): void {
  try {
    const opts = parseArgs(process.argv.slice(2));

    if (opts.version) {
      console.log(readVersion());
      process.exit(EXIT_SUCCESS);
    }

    if (opts.help) {
      printHelp();
      process.exit(EXIT_SUCCESS);
    }

    if (opts.completionShell) {
      console.log(renderCompletionScript(opts.completionShell));
      process.exit(EXIT_SUCCESS);
    }

    initColor(opts);
    const cwd = process.cwd();
    const config = loadConfig(cwd, opts.configPath);
    const fileIgnorePatterns = loadIgnorePatterns(cwd);
    const runtimeFormatOptions: RuntimeFormatOptions = {
      recover: opts.strict ? false : (config.recover ?? !(config.strict ?? false)),
      maxLineLength: opts.maxLineLength ?? config.maxLineLength,
      maxDepth: config.maxDepth,
      maxInputSize: config.maxInputSize,
    };

    let expandedFiles = expandGlobs(opts.files);
    const allIgnorePatterns = [...fileIgnorePatterns, ...opts.ignore];

    if (opts.verbose && fileIgnorePatterns.length > 0) {
      console.error(`Loaded ${fileIgnorePatterns.length} pattern(s) from ${IGNORE_FILE_NAME}`);
    }

    // Apply .holywellignore and --ignore patterns
    if (allIgnorePatterns.length > 0 && expandedFiles.length > 0) {
      expandedFiles = expandedFiles.filter(f => !matchesAnyIgnorePattern(f, allIgnorePatterns));
    }

    let checkFailures = 0;
    let changedCount = 0;

    if (expandedFiles.length === 0 && opts.files.length === 0) {
      // stdin mode
      const input = readFileSync(0, 'utf-8');
      let output: string;
      let recoveries: RecoveryEvent[];
      try {
        ({ output, recoveries } = formatOneInput(input, runtimeFormatOptions));
      } catch (err) {
        handleParseError(err, input, opts.stdinFilepath);
      }

      printRecoveryWarnings(recoveries);

      if (opts.check) {
        const normalizedInput = normalizeForComparison(input);
        if (normalizedInput !== output) {
          checkFailures++;
          if (!opts.quiet) {
            console.error(red('Input is not formatted.'));
          }
          if (opts.diff && !opts.quiet) {
            console.error(unifiedDiff(normalizedInput, output, 'a/stdin.sql', 'b/stdin.sql'));
          }
        }
      } else if (!opts.quiet) {
        process.stdout.write(output);
      }
    } else {
      if (opts.verbose) {
        console.error(`Formatting ${expandedFiles.length} file${expandedFiles.length === 1 ? '' : 's'}...`);
      }

      for (let fileIndex = 0; fileIndex < expandedFiles.length; fileIndex++) {
        const file = expandedFiles[fileIndex];
        if (opts.verbose) {
          if (expandedFiles.length >= 20) {
            console.error(`[${fileIndex + 1}/${expandedFiles.length}] ${file}`);
          } else {
            console.error(file);
          }
        }

        const input = readFileSync(file, 'utf-8');

        let output: string;
        let recoveries: RecoveryEvent[];
        try {
          ({ output, recoveries } = formatOneInput(input, runtimeFormatOptions));
        } catch (err) {
          handleParseError(err, input);
        }

        printRecoveryWarnings(recoveries);

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
              console.error(unifiedDiff(normalizedInput, output, `a/${file}`, `b/${file}`));
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
      process.exit(EXIT_CHECK_FAILURE);
    }
  } catch (err) {
    if (err instanceof NoFilesMatchedError) {
      console.error(red(err.message));
      process.exit(EXIT_CHECK_FAILURE);
    }
    if (err instanceof CLIUsageError) {
      console.error(red(err.message));
      process.exit(EXIT_USAGE_OR_IO_ERROR);
    }

    if (err instanceof ParseError || err instanceof TokenizeError) {
      handleParseError(err);
    }

    const ioErr = err as NodeJS.ErrnoException | undefined;
    if (ioErr?.code === 'ENOENT' || ioErr?.code === 'EISDIR') {
      console.error(red(`I/O error: ${ioErr.message}`));
      process.exit(EXIT_USAGE_OR_IO_ERROR);
    }

    console.error(red(`Unexpected error: ${err instanceof Error ? err.message : String(err)}`));
    process.exit(EXIT_USAGE_OR_IO_ERROR);
  }
}

main();
