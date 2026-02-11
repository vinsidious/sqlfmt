import * as AST from './ast';
import { FUNCTION_KEYWORDS } from './keywords';
import { DEFAULT_MAX_DEPTH, TERMINAL_WIDTH } from './constants';
import { parse } from './parser';
import { tokenize } from './tokenizer';

function isInExprSubquery(expr: AST.InExpr): expr is AST.InExprSubquery {
  return expr.kind === 'subquery';
}

function getInExprSubquery(expr: AST.InExprSubquery): AST.SubqueryExpr {
  return expr.subquery;
}

function getInExprList(expr: AST.InExprList): AST.Expression[] {
  return expr.values as AST.Expression[];
}

// The formatter walks the AST and produces formatted SQL per the Holywell style guide.
// Key concept: "The River" — top-level clause keywords are right-aligned so content
// starts at a consistent column position.

const DEFAULT_RIVER = 6; // length of SELECT keyword
const MAX_FORMATTER_DEPTH = DEFAULT_MAX_DEPTH;

// Approximate monospace display width with East Asian wide/full-width support.
// Used for line-length heuristics so CJK-heavy SQL wraps more predictably.
function stringDisplayWidth(text: string): number {
  let width = 0;
  for (const ch of text) {
    const cp = ch.codePointAt(0) ?? 0;
    width += isWideCodePoint(cp) ? 2 : 1;
  }
  return width;
}

function isWideCodePoint(cp: number): boolean {
  return (
    (cp >= 0x1100 && cp <= 0x115f) ||
    cp === 0x2329 ||
    cp === 0x232a ||
    (cp >= 0x2e80 && cp <= 0xa4cf && cp !== 0x303f) ||
    (cp >= 0xac00 && cp <= 0xd7a3) ||
    (cp >= 0xf900 && cp <= 0xfaff) ||
    (cp >= 0xfe10 && cp <= 0xfe19) ||
    (cp >= 0xfe30 && cp <= 0xfe6f) ||
    (cp >= 0xff00 && cp <= 0xff60) ||
    (cp >= 0xffe0 && cp <= 0xffe6) ||
    (cp >= 0x1f300 && cp <= 0x1faff) ||
    (cp >= 0x20000 && cp <= 0x3fffd)
  );
}

// Layout thresholds that control when the formatter breaks lines or switches
// from inline to multi-line output. Values are in character columns.
//
//   topLevelInlineColumnMax (66)
//     Maximum columns for a top-level SELECT column list on one line.
//     66 = 80 (standard terminal) - 6 (river) - 8 (typical indent headroom).
//
//   nestedInlineColumnMax (80)
//     Maximum columns for inline expressions inside subqueries/CTEs.
//     Matches the standard 80-column terminal width.
//
//   nestedInlineWithShortAliasesMax (66)
//     Same as topLevelInlineColumnMax — used for nested contexts where
//     short aliases keep things compact enough to warrant a tighter limit.
//
//   topLevelAliasBreakMin (50)
//     When a column alias pushes a line past this threshold, the alias is
//     moved to the next line. ~63% of 80 columns, keeps aliases readable.
//
//   nestedConcatTailBreakMin (66)
//     Threshold for breaking concatenation operator (||) tails in nested
//     contexts. Matches the 66-column river-aware limit.
//
//   groupPackColumnMax (66)
//     Maximum columns before GROUP BY / ORDER BY items wrap to new lines.
//     Same river-aware 66-column limit as top-level SELECT lists.
//
//   nestedGroupPackColumnMax (80)
//     Same as groupPackColumnMax but for nested contexts — full terminal width.
//
//   expressionWrapColumnMax (80)
//     Maximum columns for general expression wrapping. Standard terminal width.
//
//   createTableTypeAlignMax (13)
//     Maximum data-type-name length (e.g. "VARCHAR(255)") for column-type
//     alignment in CREATE TABLE. 13 covers common types like TIMESTAMP(6).
interface LayoutPolicy {
  topLevelInlineColumnMax: number;
  nestedInlineColumnMax: number;
  nestedInlineWithShortAliasesMax: number;
  topLevelAliasBreakMin: number;
  nestedConcatTailBreakMin: number;
  groupPackColumnMax: number;
  nestedGroupPackColumnMax: number;
  expressionWrapColumnMax: number;
  createTableTypeAlignMax: number;
}

function buildLayoutPolicy(maxLineLength: number): LayoutPolicy {
  return {
    // maxWidth - river keyword - indentation headroom
    topLevelInlineColumnMax: maxLineLength - DEFAULT_RIVER - 8,
    nestedInlineColumnMax: maxLineLength,
    nestedInlineWithShortAliasesMax: maxLineLength - DEFAULT_RIVER - 8,
    topLevelAliasBreakMin: Math.floor(maxLineLength * 0.625),
    nestedConcatTailBreakMin: maxLineLength - DEFAULT_RIVER - 8,
    groupPackColumnMax: maxLineLength - DEFAULT_RIVER - 8,
    nestedGroupPackColumnMax: maxLineLength,
    expressionWrapColumnMax: maxLineLength,
    createTableTypeAlignMax: 13,
  };
}

interface FormatterRuntime {
  maxLineLength: number;
  layoutPolicy: LayoutPolicy;
}

interface FormatContext {
  indentOffset: number;  // extra left-margin offset for nested contexts
  riverWidth: number;
  isSubquery: boolean;
  outerColumnOffset?: number;  // additional columns in the final output (for threshold calc in subqueries)
  depth: number;  // current recursion depth for stack overflow prevention
  runtime: FormatterRuntime;
}

export interface FormatterOptions {
  maxLineLength?: number;
}

export class FormatterError extends Error {
  readonly nodeType?: string;

  constructor(message: string, nodeType?: string) {
    super(message);
    this.name = 'FormatterError';
    this.nodeType = nodeType;
  }
}

export function formatStatements(nodes: AST.Node[], options: FormatterOptions = {}): string {
  const maxLineLength = Math.max(40, options.maxLineLength ?? TERMINAL_WIDTH);
  const runtime: FormatterRuntime = {
    maxLineLength,
    layoutPolicy: buildLayoutPolicy(maxLineLength),
  };

  const parts: string[] = [];
  for (const node of nodes) {
    if (node.type === 'raw' && (node.reason === 'trailing_semicolon_comment' || node.reason === 'slash_terminator')) {
      if (parts.length === 0) {
        parts.push(node.text);
      } else if (node.text.trim() === '/') {
        const previous = parts[parts.length - 1].trimEnd();
        const withoutSyntheticSemicolon = node.reason === 'slash_terminator'
          ? previous.replace(/;$/, '')
          : previous;
        parts[parts.length - 1] = withoutSyntheticSemicolon + '\n/';
      } else {
        const previous = parts[parts.length - 1].trimEnd();
        parts[parts.length - 1] = previous + ' ' + node.text;
      }
      continue;
    }
    try {
      parts.push(formatNode(node, {
        indentOffset: 0,
        riverWidth: deriveRiverWidth(node),
        isSubquery: false,
        depth: 0,
        runtime,
      }));
    } catch (err) {
      if (err instanceof FormatterError) {
        parts.push(formatFormatterFallback(node));
        continue;
      }
      throw err;
    }
  }
  if (parts.length === 0) return '\n';

  // Top-level statement separation already inserts a blank line. If the next
  // statement itself starts with blank lines (typically from leading comment
  // metadata), remove one leading newline to prevent pass-to-pass growth.
  let out = parts[0];
  let previous = parts[0];
  for (let i = 1; i < parts.length; i++) {
    let next = parts[i];
    if (next.startsWith('\n')) next = next.slice(1);
    const separator = shouldUseSingleNewlineBetween(previous, next) ? '\n' : '\n\n';
    out += separator + next;
    previous = next;
  }
  return out + '\n';
}

function shouldUseSingleNewlineBetween(previous: string, next: string): boolean {
  const previousTrimmed = previous.trimEnd();
  if (!/^IF\b/i.test(previousTrimmed)) return false;
  if (previousTrimmed.endsWith(';')) return false;

  const nextTrimmed = next.trimStart();
  return /^(BEGIN|INSERT|UPDATE|DELETE|MERGE|SET|SELECT|WITH|EXEC|EXECUTE|PRINT)\b/i.test(nextTrimmed);
}

function formatFormatterFallback(node: AST.Node): string {
  if (node.type === 'raw') return node.text;
  if (node.type === 'comment') return node.text;
  return '/* formatter fallback: unsupported AST node */';
}

function deriveRiverWidth(node: AST.Node): number {
  switch (node.type) {
    case 'select':
      return deriveSelectRiverWidth(node);
    case 'explain':
      return Math.max('EXPLAIN'.length, deriveRiverWidth(node.statement));
    case 'insert': {
      let width = 'INSERT'.length;
      if (node.values) width = Math.max(width, 'VALUES'.length);
      if (node.defaultValues) width = Math.max(width, 'DEFAULT'.length);
      if (node.onConflict) {
        width = Math.max(width, 'ON'.length, 'DO'.length);
        if (node.onConflict.setItems && node.onConflict.setItems.length > 0) {
          width = Math.max(width, 'SET'.length);
        }
        if (node.onConflict.where) {
          width = Math.max(width, 'WHERE'.length);
        }
      }
      if (node.returning && node.returning.length > 0) {
        width = Math.max(width, 'RETURNING'.length);
      }
      return width;
    }
    case 'update': {
      let width = Math.max('UPDATE'.length, 'SET'.length);
      if (node.joinSources && node.joinSources.length > 0) width = Math.max(width, 'JOIN'.length);
      if (node.from && node.from.length > 0) width = Math.max(width, 'FROM'.length);
      if (node.where) width = Math.max(width, 'WHERE'.length);
      if (node.returning && node.returning.length > 0) {
        width = Math.max(width, 'RETURNING'.length);
      }
      return width;
    }
    case 'delete': {
      let width = Math.max('DELETE'.length, 'FROM'.length);
      if (node.using && node.using.length > 0) width = Math.max(width, 'USING'.length);
      if (node.where || node.currentOf) width = Math.max(width, 'WHERE'.length);
      if (node.returning && node.returning.length > 0) {
        width = Math.max(width, 'RETURNING'.length);
      }
      return width;
    }
    case 'union': {
      let width = DEFAULT_RIVER;
      for (const member of node.members) {
        width = Math.max(width, deriveRiverWidth(member.statement as AST.Node));
      }
      for (const op of node.operators) {
        width = Math.max(width, op.split(' ')[0].length);
      }
      if (node.orderBy) width = Math.max(width, 'ORDER'.length);
      if (node.limit) width = Math.max(width, 'LIMIT'.length);
      if (node.offset) width = Math.max(width, 'OFFSET'.length);
      if (node.fetch) width = Math.max(width, 'FETCH'.length);
      if (node.lockingClause) width = Math.max(width, 'FOR'.length);
      return width;
    }
    case 'cte': {
      let width = 'WITH'.length;
      for (const cte of node.ctes) {
        width = Math.max(width, deriveRiverWidth(cte.query as AST.Node));
      }
      width = Math.max(width, deriveRiverWidth(node.mainQuery));
      return width;
    }
    case 'merge': {
      let width = Math.max('MERGE'.length, 'USING'.length);
      // ON is short but always present
      width = Math.max(width, 'ON'.length);
      for (const wc of node.whenClauses) {
        width = Math.max(width, 'WHEN'.length);
        if (wc.action === 'update' && wc.setItems && wc.setItems.length > 0) {
          width = Math.max(width, 'SET'.length);
        }
        if (wc.action === 'insert') {
          width = Math.max(width, 'VALUES'.length);
        }
      }
      return width;
    }
    case 'create_index': {
      let width = 'ON'.length;
      if (node.using) width = Math.max(width, 'USING'.length);
      if (node.include && node.include.length > 0) width = Math.max(width, 'INCLUDE'.length);
      if (node.where) width = Math.max(width, 'WHERE'.length);
      return Math.max(width, DEFAULT_RIVER);
    }
    case 'create_view':
      return deriveRiverWidth(node.query as AST.Node);
    case 'create_policy': {
      let width = 'ON'.length;
      if (node.permissive) width = Math.max(width, 'AS'.length);
      if (node.command) width = Math.max(width, 'FOR'.length);
      if (node.roles) width = Math.max(width, 'TO'.length);
      if (node.using) width = Math.max(width, 'USING'.length);
      if (node.withCheck) width = Math.max(width, 'WITH'.length);
      return Math.max(width, DEFAULT_RIVER);
    }
    default:
      return DEFAULT_RIVER;
  }
}

function deriveSelectRiverWidth(node: AST.SelectStatement): number {
  let width = 'SELECT'.length;
  if (node.into) width = Math.max(width, 'INTO'.length);
  if (node.from) width = Math.max(width, 'FROM'.length);
  if (node.joins.some(j => j.joinType === 'JOIN')) width = Math.max(width, 'JOIN'.length);
  if (node.where) width = Math.max(width, 'WHERE'.length);
  if (node.startWith) width = Math.max(width, 'START'.length);
  if (node.connectBy) width = Math.max(width, 'CONNECT'.length);
  if (node.groupBy) width = Math.max(width, 'GROUP'.length);
  if (node.having) width = Math.max(width, 'HAVING'.length);
  if (node.windowClause && node.windowClause.length > 0) width = Math.max(width, 'WINDOW'.length);
  if (node.orderBy) width = Math.max(width, 'ORDER'.length);
  if (node.limit) width = Math.max(width, 'LIMIT'.length);
  if (node.offset) width = Math.max(width, 'OFFSET'.length);
  if (node.fetch) width = Math.max(width, 'FETCH'.length);
  if (node.lockingClause) width = Math.max(width, 'FOR'.length);
  return width;
}

function formatNode(node: AST.Node, ctx: FormatContext): string {
  switch (node.type) {
    case 'select': return formatSelect(node, ctx);
    case 'insert': return formatInsert(node, ctx);
    case 'update': return formatUpdate(node, ctx);
    case 'delete': return formatDelete(node, ctx);
    case 'create_table': return formatCreateTable(node, ctx);
    case 'alter_table': return formatAlterTable(node, ctx);
    case 'drop_table': return formatDropTable(node, ctx);
    case 'union': return formatUnion(node, ctx);
    case 'cte': return formatCTE(node, ctx);
    case 'values': return formatValuesClause(node, ctx);
    case 'merge': return formatMerge(node, ctx);
    case 'create_index': return formatCreateIndex(node, ctx);
    case 'create_view': return formatCreateView(node, ctx);
    case 'create_policy': return formatCreatePolicy(node, ctx);
    case 'grant': return formatGrant(node, ctx);
    case 'truncate': return formatTruncate(node, ctx);
    case 'standalone_values': return formatStandaloneValues(node, ctx);
    case 'explain': return formatExplain(node, ctx);
    case 'raw': return formatRawTopLevelNode(node.text, ctx);
    case 'comment': return node.text;
    default: {
      const _exhaustive: never = node;
      throw new FormatterError(
        `Unknown node type: ${(_exhaustive as { type?: string }).type ?? 'unknown'}`,
        (_exhaustive as { type?: string }).type
      );
    }
  }
}

function formatRawTopLevelNode(text: string, ctx: FormatContext): string {
  const routine = tryFormatRoutineBlock(text, ctx.runtime);
  if (routine) return routine;
  const createPipe = tryFormatCreatePipe(text);
  if (createPipe) return createPipe;
  const beginEnd = tryFormatBeginEndBlock(text, ctx.runtime);
  if (beginEnd) return beginEnd;
  const alterView = tryFormatAlterViewAsQuery(text, ctx.runtime);
  if (alterView) return alterView;
  const returnSelect = tryFormatReturnParenthesizedSelect(text, ctx.runtime);
  return returnSelect ?? text;
}

function tryFormatCreatePipe(text: string): string | null {
  const trimmed = text.trim();
  if (!/^CREATE\s+(?:OR\s+REPLACE\s+)?PIPE\b/i.test(trimmed)) return null;

  const lines = trimmed
    .split('\n')
    .map(line => line.trim())
    .filter(line => line.length > 0);
  if (lines.length === 0) return null;

  const first = lines[0];
  const firstMatch = /^CREATE\s+(OR\s+REPLACE\s+)?PIPE\b(.*)$/i.exec(first);
  if (!firstMatch) return null;
  const header = firstMatch[1] ? 'CREATE OR REPLACE PIPE' : 'CREATE PIPE';
  const objectName = firstMatch[2].trim();

  const out: string[] = [objectName ? `${header} ${objectName}` : header];
  for (let i = 1; i < lines.length; i++) {
    let line = lines[i];
    line = line.replace(/^AUTO_INGEST\b/i, 'AUTO_INGEST');
    line = line.replace(/^AS\b/i, 'AS');
    line = line.replace(/^COPY\s+INTO\b/i, 'COPY INTO');
    line = line.replace(/^FROM\b/i, 'FROM');
    line = line.replace(/\s+;$/, ';');
    out.push(line);
  }

  return out.join('\n');
}

function tryFormatAlterViewAsQuery(text: string, runtime: FormatterRuntime): string | null {
  const trimmed = text.trim();
  if (!/^ALTER\s+VIEW\b/i.test(trimmed)) return null;

  const tokens = tokenize(trimmed)
    .filter(token => token.type !== 'whitespace' && token.type !== 'line_comment' && token.type !== 'block_comment' && token.type !== 'eof');
  if (tokens.length < 4 || tokens[0].upper !== 'ALTER' || tokens[1].upper !== 'VIEW') return null;

  let groupDepth = 0;
  let asTokenIndex = -1;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token.value === '(' || token.value === '[' || token.value === '{') {
      groupDepth++;
      continue;
    }
    if (token.value === ')' || token.value === ']' || token.value === '}') {
      groupDepth = Math.max(0, groupDepth - 1);
      continue;
    }
    if (groupDepth === 0 && token.upper === 'AS') {
      asTokenIndex = i;
      break;
    }
  }
  if (asTokenIndex < 0 || asTokenIndex + 1 >= tokens.length) return null;

  const queryStartToken = tokens[asTokenIndex + 1];
  if (queryStartToken.upper !== 'SELECT' && queryStartToken.upper !== 'WITH' && queryStartToken.upper !== 'VALUES') {
    return null;
  }

  const asToken = tokens[asTokenIndex];
  const header = trimmed.slice(0, asToken.position + asToken.value.length).trim().replace(/\s+/g, ' ');
  const querySql = trimmed.slice(queryStartToken.position).trim().replace(/;$/, '') + ';';

  try {
    const nodes = parse(querySql, {
      recover: false,
      maxDepth: DEFAULT_MAX_DEPTH,
    });
    if (nodes.length !== 1) return null;
    const node = nodes[0];
    if (node.type !== 'select' && node.type !== 'cte' && node.type !== 'union' && node.type !== 'standalone_values') {
      return null;
    }
    const formattedQuery = formatStatements(nodes, {
      maxLineLength: runtime.maxLineLength,
    }).trim();
    return `${header}\n${formattedQuery}`;
  } catch {
    return null;
  }
}

function tryFormatRoutineBlock(text: string, runtime: FormatterRuntime): string | null {
  const trimmed = text.trim();
  if (
    !/^(CREATE|ALTER)\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION|TRIGGER|EVENT)\b/i.test(trimmed)
  ) {
    return null;
  }

  const beginMatch = /\bBEGIN\b/i.exec(trimmed);
  const endMatch = /\bEND\b\s*;?\s*$/i.exec(trimmed);
  if (!beginMatch || !endMatch || endMatch.index <= beginMatch.index) return null;

  const headerEnd = beginMatch.index + beginMatch[0].length;
  const header = trimmed.slice(0, headerEnd).trimEnd();
  const body = trimmed.slice(headerEnd, endMatch.index).trim();
  const footer = trimmed.slice(endMatch.index).trim();

  if (!body) {
    return `${header}\n${footer}`;
  }

  const formattedBody = formatBlockBody(body, runtime);
  if (!formattedBody) {
    return null;
  }

  const indentedBody = formattedBody
    .split('\n')
    .map(line => (line ? '    ' + line : line))
    .join('\n');

  return `${header}\n${indentedBody}\n${footer}`;
}

function tryFormatBeginEndBlock(text: string, runtime: FormatterRuntime): string | null {
  const trimmed = text.trim();
  if (!/^BEGIN\b/i.test(trimmed)) return null;

  const beginMatch = /^BEGIN\b/i.exec(trimmed);
  const endMatch = /\bEND\b\s*;?\s*$/i.exec(trimmed);
  if (!beginMatch || !endMatch || endMatch.index <= beginMatch.index) return null;

  const body = trimmed.slice(beginMatch[0].length, endMatch.index).trim();
  const footer = trimmed.slice(endMatch.index).trim().replace(/^end\b/i, 'END');
  if (!body) return `BEGIN\n${footer}`;

  const formattedBody = formatBlockBody(body, runtime);
  if (!formattedBody) return null;

  return `BEGIN\n${formattedBody}\n${footer}`;
}

function formatBlockBody(body: string, runtime: FormatterRuntime): string | null {
  try {
    const bodyNodes = parse(body, {
      recover: true,
      maxDepth: DEFAULT_MAX_DEPTH,
    });
    if (bodyNodes.length === 0) return body.trim();

    const parts: string[] = [];
    for (const node of bodyNodes) {
      if (node.type === 'raw') {
        const returnSelect = tryFormatReturnParenthesizedSelect(node.text, runtime);
        if (returnSelect) {
          parts.push(returnSelect.trim());
          continue;
        }
      }
      parts.push(formatStatements([node], {
        maxLineLength: runtime.maxLineLength,
      }).trim());
    }
    return parts.join('\n\n');
  } catch {
    return null;
  }
}

function tryFormatReturnParenthesizedSelect(text: string, runtime: FormatterRuntime): string | null {
  const trimmed = text.trim();
  const match = /^RETURN\s*\(\s*([\s\S]+)\)\s*;?$/i.exec(trimmed);
  if (!match) return null;

  const inner = match[1].trim().replace(/;$/, '') + ';';
  let formattedInner: string;
  try {
    const innerNodes = parse(inner, {
      recover: false,
      maxDepth: DEFAULT_MAX_DEPTH,
    });
    if (
      innerNodes.length !== 1
      || (
        innerNodes[0].type !== 'select'
        && innerNodes[0].type !== 'cte'
        && innerNodes[0].type !== 'union'
      )
    ) {
      return null;
    }
    formattedInner = formatStatements(innerNodes, {
      maxLineLength: runtime.maxLineLength,
    }).trim().replace(/;$/, '');
  } catch {
    return null;
  }

  const indentedInner = formattedInner
    .split('\n')
    .map(line => (line ? '    ' + line : line))
    .join('\n');
  const suffix = trimmed.endsWith(';') ? ';' : '';
  return `RETURN (\n${indentedInner}\n)${suffix}`;
}

// Right-align keyword so its last char is at column (indentOffset + riverWidth - 1)
function rightAlign(keyword: string, ctx: FormatContext): string {
  const totalWidth = ctx.indentOffset + ctx.riverWidth;
  const padding = totalWidth - keyword.length;
  if (padding <= 0) return keyword;
  return ' '.repeat(padding) + keyword;
}

// Column where content starts (after the keyword + space)
function contentCol(ctx: FormatContext): number {
  return ctx.indentOffset + ctx.riverWidth + 1;
}

function contentPad(ctx: FormatContext): string {
  return ' '.repeat(contentCol(ctx));
}

function appendStatementSemicolon(sql: string): string {
  if (!sql) return ';';
  if (endsWithLineComment(sql)) return sql;
  return sql.endsWith(';') ? sql : sql + ';';
}

function endsWithLineComment(sql: string): boolean {
  const trimmed = sql.trimEnd();
  if (!trimmed) return false;

  const lastNl = Math.max(trimmed.lastIndexOf('\n'), trimmed.lastIndexOf('\r'));
  const line = trimmed.slice(lastNl + 1);

  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    const next = i + 1 < line.length ? line[i + 1] : '';

    if (inSingle) {
      if (ch === "'" && next === "'") {
        i++;
        continue;
      }
      if (ch === "'") inSingle = false;
      continue;
    }
    if (inDouble) {
      if (ch === '"' && next === '"') {
        i++;
        continue;
      }
      if (ch === '"') inDouble = false;
      continue;
    }
    if (inBacktick) {
      if (ch === '`' && next === '`') {
        i++;
        continue;
      }
      if (ch === '`') inBacktick = false;
      continue;
    }
    if (inBracket) {
      if (ch === ']' && next === ']') {
        i++;
        continue;
      }
      if (ch === ']') inBracket = false;
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      continue;
    }
    if (ch === '`') {
      inBacktick = true;
      continue;
    }
    if (ch === '[') {
      inBracket = true;
      continue;
    }

    if (ch === '-' && next === '-') return true;
    if (ch === '/' && next === '/') return true;
    if (ch === '#') {
      const prev = i > 0 ? line[i - 1] : '';
      const nextCh = next;
      const prevOk = !prev || prev === ' ' || prev === '\t';
      const nextOk = !nextCh || nextCh === ' ' || nextCh === '\t';
      if (prevOk && nextOk) return true;
    }
  }

  return false;
}

function formatExplain(node: AST.ExplainStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  const options: string[] = [];
  if (node.analyze) options.push('ANALYZE');
  if (node.verbose) options.push('VERBOSE');
  if (node.costs !== undefined) options.push(node.costs ? 'COSTS' : 'COSTS OFF');
  if (node.buffers !== undefined) options.push(node.buffers ? 'BUFFERS' : 'BUFFERS OFF');
  if (node.timing !== undefined) options.push(node.timing ? 'TIMING' : 'TIMING OFF');
  if (node.summary !== undefined) options.push(node.summary ? 'SUMMARY' : 'SUMMARY OFF');
  if (node.settings !== undefined) options.push(node.settings ? 'SETTINGS' : 'SETTINGS OFF');
  if (node.wal !== undefined) options.push(node.wal ? 'WAL' : 'WAL OFF');
  if (node.format) options.push(`FORMAT ${node.format}`);

  let header = 'EXPLAIN';
  if (node.planFor) {
    header += ' PLAN FOR';
  } else if (options.length > 0) {
    header += ' (' + options.join(', ') + ')';
  }
  lines.push(header);

  const inner = formatNode(node.statement, {
    ...ctx,
    riverWidth: deriveRiverWidth(node.statement),
    isSubquery: true,
    depth: ctx.depth + 1,
  });
  lines.push(inner);

  let result = lines.join('\n');
  if (!ctx.isSubquery) result = appendStatementSemicolon(result);
  return result;
}

// ─── SELECT ──────────────────────────────────────────────────────────

function formatSelect(node: AST.SelectStatement, ctx: FormatContext): string {
  const lines: string[] = [];

  emitComments(node.leadingComments, lines);

  // SELECT [DISTINCT] columns
  const selectKw = rightAlign('SELECT', ctx);
  const distinctStr = node.distinctOn
    ? ` DISTINCT ON (${node.distinctOn.map(e => formatExpr(e)).join(', ')})`
    : node.distinct
      ? ' DISTINCT'
      : '';
  const topStr = node.top ? ` ${node.top}` : '';
  const colStartCol = contentCol(ctx) + stringDisplayWidth(distinctStr + topStr);
  const colStr = formatColumnList(node.columns, colStartCol, ctx);
  const firstColumnHasLeadingComments = !!(node.columns[0]?.leadingComments && node.columns[0].leadingComments.length > 0);
  if (firstColumnHasLeadingComments) {
    lines.push(selectKw + distinctStr + topStr);
    lines.push(colStr);
  } else if (colStr) {
    lines.push(selectKw + distinctStr + topStr + ' ' + colStr);
  } else {
    lines.push(selectKw + distinctStr + topStr);
  }

  if (node.into) {
    lines.push(rightAlign('INTO', ctx) + ' ' + node.into);
  }

  // FROM
  if (node.from) {
    const fromKw = rightAlign('FROM', ctx);
    const hasAdditional = !!(node.additionalFromItems && node.additionalFromItems.length > 0);
    lines.push(fromKw + ' ' + formatFromClause(node.from, ctx) + (hasAdditional ? ',' : ''));
    if (node.additionalFromItems && node.additionalFromItems.length > 0) {
      for (let i = 0; i < node.additionalFromItems.length; i++) {
        const item = node.additionalFromItems[i];
        const comma = i < node.additionalFromItems.length - 1 ? ',' : '';
        lines.push(contentPad(ctx) + formatFromClause(item, ctx) + comma);
      }
    }
  }

  // JOINs
  const hasSubqueryJoins = node.joins.some(j => j.table.type === 'subquery');
  const fromHasSubquery = node.from?.table.type === 'subquery';
  for (let i = 0; i < node.joins.length; i++) {
    const prev = i > 0 ? node.joins[i - 1] : undefined;
    const current = node.joins[i];
    const joinHasClause = !!(current.on || current.usingClause);
    const prevHasClause = !!(prev && (prev.on || prev.usingClause));
    const currentIsJoinClause = /\bJOIN$/i.test(current.joinType);
    const bothPlain = !!prev
      && prev.joinType === 'JOIN'
      && (current.joinType === 'JOIN' || currentIsJoinClause);
    const needsBlank = fromHasSubquery || (i > 0 && (joinHasClause || prevHasClause) && !bothPlain);
    const joinLines = formatJoin(node.joins[i], ctx, needsBlank);
    lines.push(joinLines);
  }

  // WHERE
  if (node.where) {
    if (hasSubqueryJoins && node.joins.length > 1) {
      lines.push('');
    }
    const whereKw = rightAlign('WHERE', ctx);
    const cond = formatCondition(node.where.condition, ctx);
    const trailing = node.where.trailingComment ? '  ' + node.where.trailingComment.text : '';
    lines.push(whereKw + ' ' + cond + trailing);
  }

  if (node.startWith) {
    const kw = rightAlign('START', ctx);
    lines.push(kw + ' WITH ' + formatCondition(node.startWith, ctx));
  }

  if (node.connectBy) {
    const kw = rightAlign('CONNECT', ctx);
    const noCycle = node.connectBy.noCycle ? 'NOCYCLE ' : '';
    lines.push(kw + ' BY ' + noCycle + formatCondition(node.connectBy.condition, ctx));
  }

  // GROUP BY
  if (node.groupBy) {
    const kw = rightAlign('GROUP', ctx);
    lines.push(kw + ' BY ' + formatGroupByClause(node.groupBy, ctx));
  }

  // HAVING
  if (node.having) {
    const kw = rightAlign('HAVING', ctx);
    lines.push(kw + ' ' + formatCondition(node.having.condition, ctx));
  }

  // WINDOW
  if (node.windowClause && node.windowClause.length > 0) {
    const kw = rightAlign('WINDOW', ctx);
    const first = node.windowClause[0];
    const firstComma = node.windowClause.length > 1 ? ',' : '';
    lines.push(kw + ' ' + first.name + ' AS (' + formatWindowSpec(first.spec) + ')' + firstComma);
    for (let i = 1; i < node.windowClause.length; i++) {
      const def = node.windowClause[i];
      const comma = i < node.windowClause.length - 1 ? ',' : '';
      lines.push(contentPad(ctx) + def.name + ' AS (' + formatWindowSpec(def.spec) + ')' + comma);
    }
  }

  // ORDER BY
  if (node.orderBy) {
    const kw = rightAlign('ORDER', ctx);
    lines.push(...formatSelectOrderByLines(node.orderBy.items, kw, contentPad(ctx)));
  }

  // LIMIT
  if (node.limit) {
    lines.push(rightAlign('LIMIT', ctx) + ' ' + formatExpr(node.limit.count));
  }

  // OFFSET
  if (node.offset) {
    const rows = node.offset.rowsKeyword ? ' ROWS' : '';
    lines.push(rightAlign('OFFSET', ctx) + ' ' + formatExpr(node.offset.count) + rows);
  }

  // FETCH
  if (node.fetch) {
    const suffix = node.fetch.withTies ? ' WITH TIES' : ' ONLY';
    lines.push(rightAlign('FETCH', ctx) + ' FIRST ' + formatExpr(node.fetch.count) + ' ROWS' + suffix);
  }

  if (node.lockingClause) {
    lines.push(rightAlign('FOR', ctx) + ' ' + node.lockingClause);
  }

  let result = lines.join('\n');
  if (!ctx.isSubquery) result = appendStatementSemicolon(result);
  return result;
}

// ─── Column List ─────────────────────────────────────────────────────

interface FormattedColumnPart {
  text: string;
  leadingComments?: readonly AST.CommentNode[];
  comment?: AST.CommentNode;
}

function formatColumnList(columns: readonly AST.ColumnExpr[], firstColStartCol: number, ctx: FormatContext): string {
  if (columns.length === 0) return '';

  const parts = buildFormattedColumnParts(columns, ctx);
  const inlineResult = tryFormatInlineColumnList(parts, columns, firstColStartCol, ctx);
  if (inlineResult) return inlineResult;

  const hasMultiLine = parts.some(p => p.text.includes('\n'));
  const hasLeadingComments = parts.some(p => !!(p.leadingComments && p.leadingComments.length > 0));
  const cCol = contentCol(ctx);
  const indent = ' '.repeat(cCol);

  // If any multi-line expression, one-per-line
  if (hasMultiLine || hasLeadingComments) {
    return formatColumnsOnePerLine(parts, indent);
  }

  if (shouldBreakNestedConcatTail(columns, parts, firstColStartCol, ctx)) {
    return formatColumnListWithConcatTailBreak(parts, indent);
  }

  return formatColumnListWithGroups(parts, indent, cCol, ctx);
}

function buildFormattedColumnParts(columns: readonly AST.ColumnExpr[], ctx: FormatContext): FormattedColumnPart[] {
  return columns.map(col => {
    let text = formatExprInSelect(col.expr, contentCol(ctx), ctx, ctx.outerColumnOffset || 0, ctx.depth);
    if (col.alias) {
      text += ' AS ' + formatProjectionAlias(col.alias);
    }
    return {
      text,
      leadingComments: col.leadingComments,
      comment: col.trailingComment,
    };
  });
}

function hasEffectiveAlias(column: AST.ColumnExpr): boolean {
  return !!column.alias;
}

function getMaxInlineColumnLength(columns: readonly AST.ColumnExpr[], ctx: FormatContext): number {
  if (ctx.indentOffset === 0) return ctx.runtime.layoutPolicy.topLevelInlineColumnMax;
  const hasAliases = columns.some(hasEffectiveAlias);
  if (columns.length <= 2 && hasAliases) return ctx.runtime.layoutPolicy.nestedInlineWithShortAliasesMax;
  return ctx.runtime.layoutPolicy.nestedInlineColumnMax;
}

function hasTopLevelAliasBreak(columns: readonly AST.ColumnExpr[], effectiveLen: number, ctx: FormatContext): boolean {
  if (ctx.indentOffset !== 0) return false;
  const aliasCount = columns.filter(hasEffectiveAlias).length;
  return aliasCount >= 2
    && columns.length >= 3
    && effectiveLen > ctx.runtime.layoutPolicy.topLevelAliasBreakMin;
}

function shouldBreakNestedConcatTail(
  columns: readonly AST.ColumnExpr[],
  parts: FormattedColumnPart[],
  firstColStartCol: number,
  ctx: FormatContext
): boolean {
  if (ctx.indentOffset === 0 || columns.length < 4) return false;
  if (parts.some(p => p.comment)) return false;
  if (!parts.slice(3).some(p => p.text.includes('||'))) return false;

  const singleLine = parts.map(p => p.text).join(', ');
  const totalLen = firstColStartCol + stringDisplayWidth(singleLine);
  const effectiveLen = totalLen + (ctx.outerColumnOffset || 0);
  return effectiveLen > ctx.runtime.layoutPolicy.nestedConcatTailBreakMin;
}

function tryFormatInlineColumnList(
  parts: FormattedColumnPart[],
  columns: readonly AST.ColumnExpr[],
  firstColStartCol: number,
  ctx: FormatContext
): string | null {
  if (parts.some(p => !!(p.leadingComments && p.leadingComments.length > 0))) return null;
  if (parts.some(p => p.comment)) return null;
  if (parts.some(p => p.text.includes('\n'))) return null;

  const singleLine = parts.map(p => p.text).join(', ');
  const totalLen = firstColStartCol + stringDisplayWidth(singleLine);
  const effectiveLen = totalLen + (ctx.outerColumnOffset || 0);
  const maxInlineLen = getMaxInlineColumnLength(columns, ctx);

  if (effectiveLen > maxInlineLen) return null;
  if (hasTopLevelAliasBreak(columns, effectiveLen, ctx)) return null;
  if (shouldBreakNestedConcatTail(columns, parts, firstColStartCol, ctx)) return null;
  return singleLine;
}

function formatColumnListWithConcatTailBreak(parts: FormattedColumnPart[], indent: string): string {
  const lines: string[] = [];
  const head = parts.slice(0, 3).map(p => p.text).join(', ');
  lines.push(head + ',');
  for (let i = 3; i < parts.length; i++) {
    const isLast = i === parts.length - 1;
    const comma = isLast ? '' : ',';
    lines.push(indent + parts[i].text + comma);
  }
  return lines.join('\n');
}

function formatColumnListWithGroups(
  parts: FormattedColumnPart[],
  indent: string,
  cCol: number,
  ctx: FormatContext
): string {
  // Multi-line with grouped continuation:
  // First column always on its own line (the SELECT line)
  const firstComment = parts[0].comment ? '  ' + parts[0].comment.text : '';
  const firstComma = parts.length > 1 ? ',' : '';
  const lines: string[] = [parts[0].text + firstComma + firstComment];

  if (parts.length === 1) return lines[0];

  // Group remaining columns by comment boundaries
  const remaining = parts.slice(1);
  const lineGroups: FormattedColumnPart[][] = [];
  let currentGroup: FormattedColumnPart[] = [];

  for (const col of remaining) {
    currentGroup.push(col);
    if (col.comment) {
      lineGroups.push(currentGroup);
      currentGroup = [];
    }
  }
  if (currentGroup.length > 0) {
    lineGroups.push(currentGroup);
  }

  // Format each group
  for (let g = 0; g < lineGroups.length; g++) {
    const group = lineGroups[g];
    const isLastGroup = g === lineGroups.length - 1;

    // Calculate group line length (columns only, without comments)
    const groupTexts = group.map(p => p.text);
    const groupLine = groupTexts.join(', ');
    const groupLen = cCol + stringDisplayWidth(groupLine);

    // Get trailing comment from last column in group (if any)
    const lastCol = group[group.length - 1];
    const groupComment = lastCol.comment ? ' ' + lastCol.comment.text : '';
    const groupComma = isLastGroup ? '' : ',';

    const effectiveGroupLen = groupLen + (ctx.outerColumnOffset || 0);
    if (shouldPackColumnGroup(group.length, groupLen, effectiveGroupLen, ctx)) {
      // Pack onto one continuation line (3+ similar columns that fit)
      lines.push(indent + groupLine + groupComma + groupComment);
    } else {
      // One-per-line within this group
      for (let i = 0; i < group.length; i++) {
        const col = group[i];
        const isLastOverall = isLastGroup && i === group.length - 1;
        const comma = isLastOverall ? '' : ',';
        const comment = col.comment ? ' ' + col.comment.text : '';
        lines.push(indent + col.text + comma + comment);
      }
    }
  }

  return lines.join('\n');
}

function shouldPackColumnGroup(
  groupLength: number,
  groupLen: number,
  effectiveGroupLen: number,
  ctx: FormatContext
): boolean {
  if (groupLength >= 3 && groupLen <= ctx.runtime.layoutPolicy.groupPackColumnMax) {
    return true;
  }
  return groupLength >= 2
    && (ctx.outerColumnOffset || 0) > 0
    && effectiveGroupLen <= ctx.runtime.layoutPolicy.nestedGroupPackColumnMax;
}

function formatColumnsOnePerLine(parts: FormattedColumnPart[], indent: string): string {
  const result: string[] = [];
  const firstHasLeadingComments = !!(parts[0]?.leadingComments && parts[0].leadingComments.length > 0);
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    const baseIndent = i === 0 ? (firstHasLeadingComments ? indent : '') : indent;
    const commentIndent = indent;
    if (p.leadingComments && p.leadingComments.length > 0) {
      for (const comment of p.leadingComments) {
        result.push(commentIndent + comment.text);
      }
    }
    const isLast = i === parts.length - 1;
    const comma = isLast ? '' : ',';
    const comment = p.comment ? '  ' + p.comment.text : '';
    const text = p.text + comma + comment;
    result.push(baseIndent + text);
  }
  return result.join('\n');
}

// Format an expression that appears in a SELECT column list
// This needs context-awareness for CASE and subqueries
function formatExprInSelect(
  expr: AST.Expression,
  colStart: number,
  ctx: FormatContext,
  outerOffset: number = 0,
  depth: number = 0
): string {
  if (expr.type === 'case') {
    return formatCaseAtColumn(expr, colStart, ctx.runtime, depth + 1);
  }
  if (expr.type === 'subquery') {
    return formatSubqueryAtColumn(expr, colStart, ctx.runtime, depth + 1);
  }
  if (expr.type === 'window_function') {
    return formatWindowFunctionAtColumn(expr, colStart, ctx.runtime);
  }

  if (expr.type === 'binary' && expr.right.type === 'subquery') {
    const left = formatExpr(expr.left);
    const op = ' ' + expr.operator + ' ';
    const subq = formatSubqueryAtColumn(
      expr.right,
      colStart + stringDisplayWidth(left) + stringDisplayWidth(op) + 1,
      ctx.runtime,
      depth + 1
    );
    return left + op + subq;
  }

  // Check if single-line would be too long
  const simple = formatExpr(expr);
  const effectiveLen = colStart + outerOffset + stringDisplayWidth(simple);

  // Function call with CASE argument that's too long — wrap compactly
  const runtime = ctx.runtime;
  if (expr.type === 'function_call' && effectiveLen > runtime.layoutPolicy.expressionWrapColumnMax) {
    const wrapped = formatFunctionCallWrapped(expr, colStart, outerOffset, runtime);
    if (wrapped !== null) return wrapped;
    const multiline = formatFunctionCallMultiline(expr, colStart, runtime);
    if (multiline !== null) return multiline;
  }

  // Binary expression that's too long — wrap at outermost operator
  if (expr.type === 'binary' && effectiveLen > runtime.layoutPolicy.expressionWrapColumnMax) {
    return formatBinaryWrapped(expr, colStart);
  }

  // Array constructor that's too long — wrap elements
  if (expr.type === 'array_constructor' && effectiveLen > runtime.layoutPolicy.expressionWrapColumnMax) {
    return formatArrayConstructorWrapped(expr, colStart, runtime);
  }

  return simple;
}

// Wrap a function call when its argument is a CASE expression
function formatFunctionCallWrapped(
  expr: AST.FunctionCallExpr,
  colStart: number,
  outerOffset: number,
  runtime: FormatterRuntime
): string | null {
  if (expr.args.length === 1 && expr.args[0].type === 'case') {
    const name = expr.name.toUpperCase();
    const distinct = expr.distinct ? 'DISTINCT ' : '';
    const prefix = name + '(' + distinct;
    const caseCol = colStart + prefix.length;
    const caseFmt = formatCaseCompact(expr.args[0] as AST.CaseExpr, caseCol, outerOffset, runtime);
    return prefix + caseFmt + ')';
  }
  return null;
}

function formatFunctionCallMultiline(
  expr: AST.FunctionCallExpr,
  colStart: number,
  runtime: FormatterRuntime
): string | null {
  const name = expr.name.toUpperCase();
  const innerCol = colStart + 4;
  const innerPad = ' '.repeat(innerCol);
  const closePad = ' '.repeat(colStart);

  // Generic key/value pair layout (e.g., object builder functions).
  if (hasKeyValueArgShape(expr)) {
    const lines: string[] = [];
    lines.push(name + '(');
    for (let i = 0; i < expr.args.length; i += 2) {
      const key = formatExpr(expr.args[i]);
      const valueCol = innerCol;
      const value = formatExprAtColumn(expr.args[i + 1], valueCol, runtime);
      const comma = i + 2 < expr.args.length ? ',' : '';
      lines.push(innerPad + key + ', ' + value + comma);
    }
    lines.push(closePad + ')');
    return lines.join('\n');
  }

  // Generic 3-arg range/series shape where the step argument is interval-like.
  if (hasSeriesArgShape(expr)) {
    const lines: string[] = [];
    lines.push(name + '(');
    const inlineArgs = expr.args.map(formatExpr).join(', ');
    const preferMultiline = expr.args.every(isLiteralLike);
    if (!preferMultiline) {
      lines.push(innerPad + inlineArgs);
    } else {
      for (let i = 0; i < expr.args.length; i++) {
        const arg = formatExprAtColumn(expr.args[i], innerCol, runtime);
        const comma = i < expr.args.length - 1 ? ',' : '';
        lines.push(innerPad + arg + comma);
      }
    }
    lines.push(closePad + ')');
    return lines.join('\n');
  }

  if (expr.orderBy && expr.args.length === 1) {
    const lines: string[] = [];
    lines.push(name + '(');
    lines.push(innerPad + formatExprAtColumn(expr.args[0], innerCol, runtime));
    lines.push(innerPad + 'ORDER BY ' + expr.orderBy.map(formatOrderByItem).join(', '));
    if (expr.separator) {
      lines.push(innerPad + 'SEPARATOR ' + formatExpr(expr.separator));
    }
    lines.push(closePad + ')');
    return lines.join('\n');
  }

  return null;
}

function hasKeyValueArgShape(expr: AST.FunctionCallExpr): boolean {
  if (expr.args.length < 6 || expr.args.length % 2 !== 0) return false;
  for (let i = 0; i < expr.args.length; i += 2) {
    const key = expr.args[i];
    if (key.type === 'literal') continue;
    if (key.type === 'identifier') continue;
    if (key.type === 'raw') continue;
    return false;
  }
  return true;
}

function hasSeriesArgShape(expr: AST.FunctionCallExpr): boolean {
  if (expr.args.length !== 3) return false;
  const step = expr.args[2];
  if (step.type === 'raw') return /^INTERVAL\s+/i.test(step.text);
  if (step.type === 'cast') return /INTERVAL/i.test(step.targetType);
  if (step.type === 'pg_cast') return /INTERVAL/i.test(step.targetType);
  return false;
}

// Compact CASE formatting for inside function calls:
// Single WHEN with multi-line condition:
//   CASE WHEN <multi-line cond>
//        THEN result ELSE else_result END
// Multiple WHENs:
//   CASE WHEN <cond> THEN <result>
//        WHEN <cond2> THEN <result2>
//        ELSE <else_result> END
function formatCaseCompact(
  expr: AST.CaseExpr,
  col: number,
  outerOffset: number,
  runtime: FormatterRuntime
): string {
  const whenCol = col + 'CASE '.length;
  const pad = ' '.repeat(whenCol);
  const singleWhen = expr.whenClauses.length === 1;

  let result = 'CASE';
  if (expr.operand) result += ' ' + formatExpr(expr.operand);

  for (let i = 0; i < expr.whenClauses.length; i++) {
    const wc = expr.whenClauses[i];
    const condCol = whenCol + 'WHEN '.length;
    const condStr = formatExprColumnAware(wc.condition, condCol, outerOffset, runtime);
    const thenComment = wc.trailingComment ? ' ' + wc.trailingComment : '';
    const thenStr = formatExpr(wc.result) + thenComment;
    const isMultiLine = condStr.includes('\n');

    if (i === 0) {
      result += ' WHEN ' + condStr;
      if (isMultiLine) {
        // For single WHEN: put THEN + ELSE + END all on one continuation line
        let line = 'THEN ' + thenStr;
        if (singleWhen && expr.elseResult) {
          line += ' ELSE ' + formatExpr(expr.elseResult) + ' END';
          result += '\n' + pad + line;
          return result;
        }
        result += '\n' + pad + line;
      } else {
        result += ' THEN ' + thenStr;
      }
    } else {
      result += '\n' + pad + 'WHEN ' + condStr + ' THEN ' + thenStr;
    }
  }

  if (expr.elseResult) {
    result += '\n' + pad + 'ELSE ' + formatExpr(expr.elseResult);
  }
  result += ' END';
  return result;
}

// Format an expression with column-awareness (for wrapping IN lists etc.)
function formatExprColumnAware(
  expr: AST.Expression,
  col: number,
  outerOffset: number,
  runtime: FormatterRuntime
): string {
  if (expr.type === 'in') {
    const simple = formatExpr(expr);
    if (col + outerOffset + stringDisplayWidth(simple) > runtime.layoutPolicy.expressionWrapColumnMax) {
      return formatInExprWrapped(expr, col, runtime);
    }
  }
  return formatExpr(expr);
}

// Wrap IN list values across lines when too long (only called for non-subquery IN).
// Packs as many items as fit on each line before wrapping.
function formatInExprWrapped(expr: AST.InExpr, col: number, runtime: FormatterRuntime): string {
  const neg = expr.negated ? 'NOT ' : '';
  const prefix = formatExpr(expr.expr) + ' ' + neg + 'IN (';
  if (isInExprSubquery(expr)) {
    const left = formatExpr(expr.expr);
    const inPrefix = `${left} ${neg}IN `;
    const subquery = getInExprSubquery(expr);
    const inner = formatQueryExpressionForSubquery(subquery.query, runtime);
    const lineCount = inner.split('\n').length;
    if (lineCount <= 2) {
      const wrapped = wrapSubqueryLines(inner, col + stringDisplayWidth(inPrefix));
      const inline = inPrefix + wrapped;
      if (col + stringDisplayWidth(inline) <= runtime.maxLineLength) {
        return inline;
      }
    }
    return inPrefix.trimEnd() + '\n' + ' '.repeat(col) + wrapSubqueryLines(inner, col);
  }
  const vals = getInExprList(expr).map(v => formatExpr(v));
  const valStartCol = col + stringDisplayWidth(prefix);
  const valPad = ' '.repeat(valStartCol);
  const maxWidth = runtime.maxLineLength;

  // Pack items onto lines, wrapping when exceeding maxWidth
  const lines: string[] = [];
  let currentLine = prefix;
  let currentCol = col + prefix.length;

  for (let i = 0; i < vals.length; i++) {
    const isLast = i === vals.length - 1;
    const suffix = isLast ? ')' : ', ';
    const itemText = vals[i] + suffix;
    const itemWidth = stringDisplayWidth(itemText);

    if (i > 0 && currentCol + itemWidth > maxWidth) {
      // Wrap: trim trailing comma-space if present, add it back on next line
      lines.push(currentLine.trimEnd());
      currentLine = valPad + itemText;
      currentCol = valStartCol + itemWidth;
    } else {
      currentLine += itemText;
      currentCol += itemWidth;
    }
  }
  lines.push(currentLine);
  return lines.join('\n');
}

// Wrap ARRAY[...] constructors across lines when too long.
// Packs items per line, aligning continuation under first element after '['.
function formatArrayConstructorWrapped(expr: AST.ArrayConstructorExpr, col: number, runtime: FormatterRuntime): string {
  const prefix = 'ARRAY[';
  const vals = expr.elements.map(e => formatExpr(e));
  const elemStartCol = col + stringDisplayWidth(prefix);
  const elemPad = ' '.repeat(elemStartCol);
  const maxWidth = runtime.maxLineLength;

  const lines: string[] = [];
  let currentLine = prefix;
  let currentCol = col + prefix.length;

  for (let i = 0; i < vals.length; i++) {
    const isLast = i === vals.length - 1;
    const suffix = isLast ? ']' : ', ';
    const itemText = vals[i] + suffix;
    const itemWidth = stringDisplayWidth(itemText);

    if (i > 0 && currentCol + itemWidth > maxWidth) {
      lines.push(currentLine.trimEnd());
      currentLine = elemPad + itemText;
      currentCol = elemStartCol + itemWidth;
    } else {
      currentLine += itemText;
      currentCol += itemWidth;
    }
  }
  lines.push(currentLine);
  return lines.join('\n');
}

// Wrap a binary expression at the outermost operator
function formatBinaryWrapped(expr: AST.BinaryExpr, colStart: number): string {
  if (expr.operator === ':') {
    return formatExpr(expr.left) + ':' + formatExpr(expr.right);
  }

  if (expr.operator === '||' || expr.operator === '+') {
    const op = expr.operator;
    const parts = flattenBinaryChain(expr, op).map(part => formatExpr(part));
    const wrapPad = ' '.repeat(colStart + 2);
    const lines = [parts[0]];
    for (let i = 1; i < parts.length; i++) {
      lines.push(wrapPad + op + ' ' + parts[i]);
    }
    return lines.join('\n');
  }

  const left = formatExpr(expr.left);
  const right = formatExpr(expr.right);
  const wrapPad = ' '.repeat(colStart + 4);
  return left + '\n' + wrapPad + expr.operator + ' ' + right;
}

function flattenBinaryChain(expr: AST.Expression, operator: string): AST.Expression[] {
  if (expr.type === 'binary' && expr.operator === operator) {
    return [
      ...flattenBinaryChain(expr.left, operator),
      ...flattenBinaryChain(expr.right, operator),
    ];
  }
  return [expr];
}

function formatExprAtColumn(expr: AST.Expression, colStart: number, runtime: FormatterRuntime): string {
  if (expr.type === 'case') return formatCaseAtColumn(expr, colStart, runtime);
  if (expr.type === 'subquery') return formatSubqueryAtColumn(expr, colStart, runtime);
  if (expr.type === 'window_function') return formatWindowFunctionAtColumn(expr, colStart, runtime);
  if (expr.type === 'paren') return '(' + formatExprAtColumn(expr.expr, colStart + 1, runtime) + ')';
  if (expr.type === 'binary' && expr.right.type === 'subquery') {
    const left = formatExpr(expr.left);
    const operator = ' ' + expr.operator + ' ';
    const subquery = formatSubqueryAtColumn(
      expr.right,
      colStart + stringDisplayWidth(left) + stringDisplayWidth(operator),
      runtime,
    );
    return left + operator + subquery;
  }
  if (expr.type === 'function_call') {
    const wrapped = formatFunctionCallMultiline(expr, colStart, runtime);
    if (wrapped) return wrapped;
  }
  return formatExpr(expr);
}

function isLiteralLike(expr: AST.Expression): boolean {
  if (expr.type === 'literal') return true;
  if (expr.type === 'interval' || expr.type === 'null' || expr.type === 'typed_string') return true;
  if (expr.type === 'raw') return /^INTERVAL\s+/.test(expr.text) || /^NULL$/i.test(expr.text);
  if (expr.type === 'pg_cast') return isLiteralLike(expr.expr);
  if (expr.type === 'paren') return isLiteralLike(expr.expr);
  return false;
}

// ─── FROM ────────────────────────────────────────────────────────────

function formatFromClause(from: AST.FromClause, ctx: FormatContext): string {
  const baseCol = contentCol(ctx);
  const lateralOffset = from.lateral && from.table.type !== 'function_call' ? 'LATERAL '.length : 0;
  let result = formatExprAtColumn(from.table, baseCol + lateralOffset, ctx.runtime);
  if (from.table.type === 'raw') {
    result = wrapJoinLikeRawSource(result, baseCol + lateralOffset, ctx.runtime);
  }
  if (from.lateral) result = 'LATERAL ' + result;
  if (from.tablesample) {
    result += ' TABLESAMPLE ' + from.tablesample.method + '(' + from.tablesample.args.map(formatExpr).join(', ') + ')';
    if (from.tablesample.repeatable) {
      result += ' REPEATABLE(' + formatExpr(from.tablesample.repeatable) + ')';
    }
  }
  if (from.ordinality) {
    result += ' WITH ORDINALITY';
  }
  if (from.alias) {
    result += ' AS ' + formatAlias(from.alias);
    if (from.aliasColumns && from.aliasColumns.length > 0) {
      result += '(' + from.aliasColumns.join(', ') + ')';
    }
  }
  if (from.pivotClause) {
    result += '\n' + ' '.repeat(baseCol) + normalizePivotClauseText(from.pivotClause);
  }
  if (from.trailingComments && from.trailingComments.length > 0) {
    result = appendFromTrailingComments(result, from.trailingComments, baseCol);
  }
  return result;
}

function appendFromTrailingComments(
  base: string,
  comments: readonly AST.CommentNode[],
  baseCol: number,
): string {
  if (comments.length === 0) return base;

  const inlineEligible = comments.length === 1
    && !(comments[0].startsOnOwnLine ?? false)
    && (comments[0].blankLinesBefore ?? 0) === 0
    && (comments[0].blankLinesAfter ?? 0) === 0;
  if (inlineEligible) return base + ' ' + comments[0].text;

  const lines: string[] = [base];
  const indent = ' '.repeat(baseCol);
  for (const comment of comments) {
    const before = comment.blankLinesBefore || 0;
    for (let i = 0; i < before; i++) lines.push('');
    lines.push(indent + comment.text);

    const after = comment.blankLinesAfter || 0;
    for (let i = 0; i < after; i++) lines.push('');
  }
  return lines.join('\n');
}

function wrapJoinLikeRawSource(text: string, startCol: number, runtime: FormatterRuntime): string {
  const normalized = text.replace(/\bON\(/gi, 'ON (');
  if (!/\bJOIN\b/i.test(normalized)) return normalized;
  if (startCol + stringDisplayWidth(normalized) <= runtime.maxLineLength) return normalized;

  const joinPad = ' '.repeat(Math.max(0, startCol));
  const onPad = ' '.repeat(Math.max(0, startCol + 3));

  let wrapped = normalized
    .replace(
      /\s+((?:INNER|LEFT|RIGHT|FULL|CROSS|NATURAL)(?:\s+OUTER)?\s+JOIN|JOIN)\s+/gi,
      '\n' + joinPad + '$1 ',
    )
    .replace(/\s+ON\s+/gi, '\n' + onPad + 'ON ')
    .replace(/\s+USING\s*\(/gi, '\n' + onPad + 'USING (');

  if (wrapped.startsWith('\n')) wrapped = wrapped.slice(1);
  return wrapped;
}

function normalizePivotClauseText(text: string): string {
  return text
    .replace(/\b(PIVOT|UNPIVOT)\s*\(/gi, '$1 (')
    .replace(/\bIN\s*\(/gi, 'IN (');
}

// ─── JOIN ────────────────────────────────────────────────────────────

function formatJoin(join: AST.JoinClause, ctx: FormatContext, needsBlank: boolean): string {
  const lines: string[] = [];
  const cCol = contentCol(ctx);
  const isPlain = join.joinType === 'JOIN';

  if (needsBlank) lines.push('');

  if (isPlain) {
    // Plain JOIN: keyword right-aligned like FROM
    const kw = rightAlign('JOIN', ctx);
    let tableStr = formatJoinTable(join, cCol, ctx.runtime);
    lines.push(kw + ' ' + tableStr);

    if (join.on) {
      // "    ON" — ON is indented 4 spaces from the river start
      const onPad = ' '.repeat(ctx.indentOffset + 4);
      const cond = formatJoinOn(join.on, ctx.indentOffset + 4 + 3, ctx.runtime);
      lines.push(onPad + 'ON ' + cond);
    } else if (join.usingClause && join.usingClause.length > 0) {
      const usingPad = ' '.repeat(ctx.indentOffset + 4);
      lines.push(usingPad + formatJoinUsingClause(join));
    }
  } else {
    const isFullOuter = /^FULL(?:\s+OUTER)?\s+JOIN$/i.test(join.joinType);
    if (isFullOuter) {
      const joinTail = join.joinType.replace(/^FULL\s*/i, '').trim();
      const joinPrefix = rightAlign('FULL', ctx) + (joinTail ? ' ' + joinTail : '');
      const tableStartCol = stringDisplayWidth(joinPrefix) + 1;
      const tableStr = formatJoinTable(join, tableStartCol, ctx.runtime);
      lines.push(joinPrefix + ' ' + tableStr);
      if (join.on) {
        const indent = ' '.repeat(cCol);
        const cond = formatJoinOn(join.on, cCol + 3, ctx.runtime);
        lines.push(indent + 'ON ' + cond);
      } else if (join.usingClause && join.usingClause.length > 0) {
        const indent = ' '.repeat(cCol);
        lines.push(indent + formatJoinUsingClause(join));
      }
    } else {
      // Qualified JOIN: indented at content column
      const indent = ' '.repeat(cCol);
      const tableStr = formatJoinTable(join, cCol + join.joinType.length + 1, ctx.runtime);
      lines.push(indent + join.joinType + ' ' + tableStr);

      if (join.on) {
        const cond = formatJoinOn(join.on, cCol + 3, ctx.runtime); // 3 for "ON "
        lines.push(indent + 'ON ' + cond);
      } else if (join.usingClause && join.usingClause.length > 0) {
        lines.push(indent + formatJoinUsingClause(join));
      }
    }
  }

  if (join.trailingComment && lines.length > 0) {
    lines[lines.length - 1] += '  ' + join.trailingComment.text;
  }

  return lines.join('\n');
}

function formatJoinUsingClause(join: AST.JoinClause): string {
  const using = join.usingClause ?? [];
  let text = 'USING (' + using.join(', ') + ')';
  if (join.usingAlias) {
    text += ' AS ' + formatAlias(join.usingAlias);
    if (join.usingAliasColumns && join.usingAliasColumns.length > 0) {
      text += '(' + join.usingAliasColumns.join(', ') + ')';
    }
  }
  return text;
}

function formatSelectOrderByLines(items: readonly AST.OrderByItem[], orderKeyword: string, continuationPad: string): string[] {
  if (items.length === 0) return [`${orderKeyword} BY`];

  const hasTrailingComments = items.some(item => !!item.trailingComment);
  if (!hasTrailingComments) {
    return [`${orderKeyword} BY ${items.map(formatOrderByItem).join(', ')}`];
  }

  const lines: string[] = [];
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const isLast = i === items.length - 1;
    const comma = isLast ? '' : ',';
    const comment = item.trailingComment ? '  ' + item.trailingComment.text : '';
    const line = formatOrderByItem(item) + comma + comment;
    if (i === 0) {
      lines.push(`${orderKeyword} BY ${line}`);
    } else {
      lines.push(continuationPad + line);
    }
  }

  return lines;
}

function formatJoinTable(join: AST.JoinClause, tableStartCol: number, runtime: FormatterRuntime): string {
  const lateralOffset = join.lateral && join.table.type !== 'function_call' ? 'LATERAL '.length : 0;
  let result = formatExprAtColumn(join.table, tableStartCol + lateralOffset, runtime);
  if (join.table.type === 'raw') {
    result = wrapJoinLikeRawSource(result, tableStartCol + lateralOffset, runtime);
  }
  if (join.lateral) result = 'LATERAL ' + result;
  if (join.ordinality) result += ' WITH ORDINALITY';
  if (join.alias) {
    result += ' AS ' + formatAlias(join.alias);
    if (join.aliasColumns && join.aliasColumns.length > 0) {
      result += '(' + join.aliasColumns.join(', ') + ')';
    }
  }
  if (join.pivotClause) result += ' ' + join.pivotClause;
  return result;
}

function formatJoinOn(
  expr: AST.Expression,
  baseCol: number,
  runtime: FormatterRuntime,
  depth: number = 0
): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return formatExpr(expr);
  }
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    const left = formatJoinOn(expr.left, baseCol, runtime, depth + 1);
    const indent = ' '.repeat(baseCol);
    const right = formatJoinOn(expr.right, baseCol, runtime, depth + 1);
    const leadingComment = splitLeadingLineComment(right, baseCol);
    if (leadingComment) {
      return left + ' ' + leadingComment.comment + '\n' + indent + expr.operator + ' ' + leadingComment.remainder;
    }
    return left + '\n' + indent + expr.operator + ' ' + right;
  }
  const rawInline = formatExpr(expr);
  const inline = expr.type === 'raw'
    ? normalizeMultilineLogicalOperand(rawInline, baseCol)
    : rawInline;
  if (baseCol + stringDisplayWidth(inline) <= runtime.maxLineLength) return inline;
  if (expr.type === 'binary') {
    const left = formatJoinOn(expr.left, baseCol, runtime, depth + 1);
    const indent = ' '.repeat(baseCol);
    return left + '\n' + indent + expr.operator + ' ' + formatJoinOn(expr.right, baseCol, runtime, depth + 1);
  }
  return inline;
}

// ─── WHERE/HAVING condition ──────────────────────────────────────────

function formatCondition(expr: AST.Expression, ctx: FormatContext): string {
  return formatConditionInner(expr, ctx);
}

function formatConditionInner(expr: AST.Expression, ctx: FormatContext): string {
  if (ctx.depth >= MAX_FORMATTER_DEPTH) {
    return formatExpr(expr);
  }
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    const deeper = { ...ctx, depth: ctx.depth + 1 };
    const left = formatConditionInner(expr.left, deeper);
    const opKw = rightAlign(expr.operator, ctx);
    const right = formatConditionRight(expr.right, deeper);
    const leadingComment = splitLeadingLineComment(right, contentCol(ctx));
    if (leadingComment) {
      return left + ' ' + leadingComment.comment + '\n' + opKw + ' ' + leadingComment.remainder;
    }
    return left + '\n' + opKw + ' ' + right;
  }
  return formatExprInCondition(expr, ctx);
}

function formatConditionRight(expr: AST.Expression, ctx: FormatContext): string {
  if (ctx.depth >= MAX_FORMATTER_DEPTH) {
    return formatExpr(expr);
  }
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    return formatConditionInner(expr, ctx);
  }
  const formatted = formatExprInCondition(expr, ctx);
  if (expr.type !== 'raw') return formatted;
  return normalizeMultilineLogicalOperand(formatted, contentCol(ctx));
}

function normalizeMultilineLogicalOperand(text: string, continuationCol: number): string {
  if (!text.includes('\n')) return text;
  const lines = text.split('\n');
  const first = lines[0].trimEnd();
  const firstTrimmed = first.trimStart();
  const continuationPad = ' '.repeat(Math.max(0, continuationCol));

  const out = [first];
  for (let i = 1; i < lines.length; i++) {
    const rawLine = lines[i].trim();
    if (!rawLine) {
      out.push('');
      continue;
    }
    let line = rawLine;
    if (
      (/^--/.test(firstTrimmed) || /^\/\*/.test(firstTrimmed))
      && /^(AND|OR)\b/i.test(line)
    ) {
      line = line.replace(/^(AND|OR)\s+/i, '');
    }
    out.push(continuationPad + line);
  }
  return out.join('\n');
}

function splitLeadingLineComment(
  text: string,
  continuationCol: number,
): { comment: string; remainder: string } | null {
  const match = text.match(/^\s*(--[^\n]*)(?:\n([\s\S]*))$/);
  if (!match) return null;

  const comment = match[1];
  let remainder = (match[2] || '').trimStart();
  if (!remainder) return null;
  if (/^(AND|OR)\b/i.test(remainder)) {
    remainder = remainder.replace(/^(AND|OR)\s+/i, '');
  }
  return {
    comment,
    remainder: normalizeMultilineLogicalOperand(remainder, continuationCol),
  };
}

function formatGroupByClause(groupBy: AST.GroupByClause, ctx: FormatContext): string {
  const plainItems = groupBy.items.map(e => formatExpr(e));
  const quantifier = groupBy.setQuantifier;
  const withRollup = groupBy.withRollup ? ' WITH ROLLUP' : '';
  if (!groupBy.groupingSets || groupBy.groupingSets.length === 0) {
    const body = plainItems.join(', ');
    if (quantifier && body) return quantifier + ' ' + body + withRollup;
    if (quantifier) return quantifier + withRollup;
    return body + withRollup;
  }

  const specs = groupBy.groupingSets.map(spec => formatGroupingSpec(spec, ctx));
  const all = [...plainItems, ...specs];
  const body = all.join(', ');
  if (quantifier && body) return quantifier + ' ' + body + withRollup;
  if (quantifier) return quantifier + withRollup;
  return body + withRollup;
}

function formatGroupingSpec(
  spec: { readonly type: 'grouping_sets' | 'rollup' | 'cube'; readonly sets: readonly (readonly AST.Expression[])[] },
  ctx: FormatContext
): string {
  const kind = spec.type === 'grouping_sets' ? 'GROUPING SETS' : spec.type.toUpperCase();
  if (spec.type !== 'grouping_sets') {
    const flat = spec.sets.map(set => {
      const exprs = set.map(e => formatExpr(e));
      return exprs.length === 1 ? exprs[0] : '(' + exprs.join(', ') + ')';
    });
    const flatText = flat.join(', ');
    if (flatText.length <= 32) {
      return kind + ' (' + flatText + ')';
    }
  }

  const itemIndent = ' '.repeat(contentCol(ctx) + 7);
  const closeIndent = ' '.repeat(contentCol(ctx) + 3);

  const lines: string[] = [kind + ' ('];
  for (let i = 0; i < spec.sets.length; i++) {
    const set = spec.sets[i];
    const isLast = i === spec.sets.length - 1;
    const comma = isLast ? '' : ',';
    let text: string;

    if (spec.type === 'grouping_sets') {
      text = set.length === 0 ? '()' : '(' + set.map(e => formatExpr(e)).join(', ') + ')';
    } else {
      if (set.length === 1) text = formatExpr(set[0]);
      else text = '(' + set.map(e => formatExpr(e)).join(', ') + ')';
    }
    lines.push(itemIndent + text + comma);
  }
  lines.push(closeIndent + ')');
  return lines.join('\n');
}

// Format expression in WHERE/HAVING context — handles IN subquery, EXISTS, comparisons with subqueries
function formatExprInCondition(expr: AST.Expression, ctx: FormatContext): string {
  if (expr.type === 'paren' && expr.expr.type === 'binary' && (expr.expr.operator === 'AND' || expr.expr.operator === 'OR')) {
    return '(' + formatParenLogical(expr.expr, contentCol(ctx) + 1, ctx.runtime) + ')';
  }
  if (expr.type === 'paren' && expr.expr.type === 'binary' && expr.expr.right.type === 'subquery') {
    return '(' + formatExprAtColumn(expr.expr, contentCol(ctx) + 1, ctx.runtime) + ')';
  }

  if (expr.type === 'unary' && expr.operator === 'NOT' && expr.operand.type === 'in' && isInExprSubquery(expr.operand)) {
    const inExpr = expr.operand;
    const e = formatExpr(inExpr.expr);
    const subqExpr = getInExprSubquery(inExpr);
    const inner = formatQueryExpressionForSubquery(subqExpr.query, ctx.runtime);
    const lineCount = inner.split('\n').length;

    if (lineCount <= 2) {
      const prefix = 'NOT ' + e + ' IN ';
      const parenCol = contentCol(ctx) + prefix.length;
      return prefix + wrapSubqueryLines(inner, parenCol);
    }

    const subq = wrapSubqueryLines(inner, contentCol(ctx));
    return 'NOT ' + e + ' IN\n' + contentPad(ctx) + subq;
  }

  // IN with subquery
  if (expr.type === 'in' && isInExprSubquery(expr)) {
    const e = formatExpr(expr.expr);
    const subqExpr = getInExprSubquery(expr);

    // NOT IN: always on new line
    if (expr.negated) {
      const subq = formatSubqueryAtColumn(subqExpr, contentCol(ctx), ctx.runtime);
      return e + ' NOT IN\n' + contentPad(ctx) + subq;
    }

    // IN: inline if subquery is short (≤ 2 lines), new line otherwise
    const inner = formatQueryExpressionForSubquery(subqExpr.query, ctx.runtime);
    const lineCount = inner.split('\n').length;

    if (lineCount <= 2) {
      const prefix = e + ' IN ';
      const parenCol = contentCol(ctx) + prefix.length;
      return prefix + wrapSubqueryLines(inner, parenCol);
    }

    const subq = wrapSubqueryLines(inner, contentCol(ctx));
    return e + ' IN\n' + contentPad(ctx) + subq;
  }

  // IN with value list: wrap if too long
  if (expr.type === 'in' && !isInExprSubquery(expr)) {
    const simple = formatExpr(expr);
    const cCol = contentCol(ctx);
    if (cCol + stringDisplayWidth(simple) > ctx.runtime.maxLineLength) {
      return formatInExprWrapped(expr, cCol, ctx.runtime);
    }
    return simple;
  }

  // EXISTS: always on new line
  if (expr.type === 'exists') {
    const subq = formatSubqueryAtColumn(expr.subquery, contentCol(ctx), ctx.runtime);
    return 'EXISTS\n' + contentPad(ctx) + subq;
  }

  // NOT EXISTS: format like EXISTS but with NOT prefix
  if (expr.type === 'unary' && expr.operator === 'NOT' && expr.operand.type === 'exists') {
    const subq = formatSubqueryAtColumn(expr.operand.subquery, contentCol(ctx), ctx.runtime);
    return 'NOT EXISTS\n' + contentPad(ctx) + subq;
  }

  if (expr.type === 'quantified_comparison' && expr.kind === 'subquery') {
    const left = formatExpr(expr.left);
    const prefix = `${left} ${expr.operator} ${expr.quantifier} `;
    const parenCol = contentCol(ctx) + prefix.length;
    const inner = formatQueryExpressionForSubquery(expr.subquery.query, ctx.runtime);
    return prefix + wrapSubqueryLines(inner, parenCol);
  }

  // Comparison where right side is a subquery
  if (expr.type === 'binary' && ['=', '<>', '!=', '<', '>', '<=', '>='].includes(expr.operator)) {
    if (expr.right.type === 'subquery') {
      const left = formatExpr(expr.left);
      const inner = formatQueryExpressionForSubquery(expr.right.query, ctx.runtime);
      const lineCount = inner.split('\n').length;

      if (lineCount <= 2) {
        const prefix = left + ' ' + expr.operator + ' ';
        const parenCol = contentCol(ctx) + prefix.length;
        return prefix + wrapSubqueryLines(inner, parenCol);
      }

      const subq = wrapSubqueryLines(inner, contentCol(ctx));
      return left + ' ' + expr.operator + '\n' + contentPad(ctx) + subq;
    }
  }

  if (expr.type === 'between') {
    const simple = formatExpr(expr);
    const cCol = contentCol(ctx);
    if (cCol + stringDisplayWidth(simple) <= ctx.runtime.maxLineLength) {
      return simple;
    }
    const left = formatExpr(expr.expr);
    const low = formatExpr(expr.low);
    const high = formatExpr(expr.high);
    const neg = expr.negated ? 'NOT ' : '';
    const head = `${left} ${neg}BETWEEN ${low}`;
    const betweenPrefix = `${left} ${neg}BETWEEN `;
    const andPad = ' '.repeat(cCol + stringDisplayWidth(betweenPrefix) - stringDisplayWidth('AND '));
    return head + '\n' + andPad + 'AND ' + high;
  }

  return formatExpr(expr);
}

function formatParenLogical(
  expr: AST.BinaryExpr,
  opCol: number,
  runtime: FormatterRuntime,
  depth: number = 0
): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return formatExpr(expr);
  }
  const left = formatParenOperand(expr.left, opCol, expr.operator, runtime, depth + 1);
  const right = formatParenOperand(expr.right, opCol, expr.operator, runtime, depth + 1);
  if (expr.right.type === 'raw') {
    const continuationCol = opCol + expr.operator.length + 1;
    const normalizedRight = normalizeMultilineLogicalOperand(right, continuationCol);
    const leadingComment = splitLeadingLineComment(normalizedRight, continuationCol);
    if (leadingComment) {
      return left + ' ' + leadingComment.comment + '\n'
        + ' '.repeat(opCol) + expr.operator + ' ' + leadingComment.remainder;
    }
    return left + '\n' + ' '.repeat(opCol) + expr.operator + ' ' + normalizedRight;
  }
  return left + '\n' + ' '.repeat(opCol) + expr.operator + ' ' + right;
}

function formatParenOperand(
  expr: AST.Expression,
  opCol: number,
  parentOp: string,
  runtime: FormatterRuntime,
  depth: number = 0
): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return formatExpr(expr);
  }
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    return formatParenLogical(expr, opCol, runtime, depth);
  }
  if (expr.type === 'paren' && expr.expr.type === 'binary' && (expr.expr.operator === 'AND' || expr.expr.operator === 'OR')) {
    return '(' + formatParenLogical(expr.expr, opCol + parentOp.length + 2, runtime, depth) + ')';
  }
  if (expr.type === 'exists') {
    return 'EXISTS ' + formatSubqueryAtColumn(expr.subquery, opCol + 'EXISTS '.length, runtime, depth + 1);
  }
  if (expr.type === 'unary' && expr.operator === 'NOT' && expr.operand.type === 'exists') {
    return 'NOT EXISTS ' + formatSubqueryAtColumn(expr.operand.subquery, opCol + 'NOT EXISTS '.length, runtime, depth + 1);
  }
  return formatExprAtColumn(expr, opCol + parentOp.length + 1, runtime);
}

// ─── Subquery formatting ─────────────────────────────────────────────

function formatQueryExpressionForSubquery(
  query: AST.QueryExpression,
  runtime: FormatterRuntime,
  outerColumnOffset?: number,
  depth: number = 0
): string {
  return formatNode(query, {
    indentOffset: 0,
    riverWidth: deriveRiverWidth(query),
    isSubquery: true,
    outerColumnOffset,
    depth,
    runtime,
  });
}

// Format subquery at a given column offset.
// The ( sits at `col` in the final output. SELECT starts at col+1.
// We format the inner query with indentOffset=0 (its own coordinate system),
// then shift subsequent lines by (col+1) to align under SELECT.
function formatSubqueryAtColumn(
  expr: AST.SubqueryExpr,
  col: number,
  runtime: FormatterRuntime,
  depth: number = 0
): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return '(/* depth exceeded */)';
  }
  const inner = formatQueryExpressionForSubquery(expr.query, runtime, col + 1, depth + 1);
  return wrapSubqueryLines(inner, col);
}

function wrapSubqueryLines(innerFormatted: string, col: number): string {
  const lines = innerFormatted.split('\n');
  const pad = ' '.repeat(col + 1);
  let result = '(' + lines[0];
  for (let i = 1; i < lines.length; i++) {
    result += lines[i] ? '\n' + pad + lines[i] : '\n';
  }
  result += ')';
  return result;
}

// ─── CASE formatting ─────────────────────────────────────────────────

function formatCaseAtColumn(
  expr: AST.CaseExpr,
  col: number,
  runtime: FormatterRuntime,
  depth: number = 0
): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return formatCaseSimple(expr, depth);
  }
  const pad = ' '.repeat(col);
  let result = 'CASE';
  if (expr.operand) result += ' ' + formatExpr(expr.operand);
  result += '\n';

  for (const wc of expr.whenClauses) {
    const thenExpr = formatCaseThenResult(
      wc.result,
      col + 'WHEN '.length + stringDisplayWidth(formatExpr(wc.condition)) + ' THEN '.length,
      runtime,
      depth + 1
    );
    const thenComment = wc.trailingComment ? ' ' + wc.trailingComment : '';
    result += pad + 'WHEN ' + formatExpr(wc.condition) + ' THEN ' + thenExpr + thenComment + '\n';
  }

  if (expr.elseResult) {
    result += pad + 'ELSE ' + formatExpr(expr.elseResult) + '\n';
  }

  result += pad + 'END';
  return result;
}

function formatCaseThenResult(
  expr: AST.Expression,
  col: number,
  runtime: FormatterRuntime,
  depth: number = 0
): string {
  if (expr.type === 'case') {
    return formatCaseAtColumn(expr, col, runtime, depth);
  }
  return formatExprAtColumn(expr, col, runtime);
}

// ─── Window function formatting ──────────────────────────────────────

function formatWindowFunctionAtColumn(
  expr: AST.WindowFunctionExpr,
  col: number,
  runtime: FormatterRuntime
): string {
  const func = formatFunctionCall(expr.func);
  const funcWithNullTreatment = expr.nullTreatment ? `${func} ${expr.nullTreatment}` : func;
  const hasWindowSpecParts = !!(expr.partitionBy || expr.orderBy || expr.frame || expr.exclude);
  if (expr.windowName && !hasWindowSpecParts) {
    return funcWithNullTreatment + ' OVER ' + expr.windowName;
  }
  const overStart = funcWithNullTreatment + ' OVER (';
  const overContentCol = col + overStart.length;

  if (expr.windowName && !expr.partitionBy && !expr.orderBy && expr.frame) {
    const frame = formatFrameClause(
      expr.frame,
      overContentCol + expr.windowName.length + 1,
      expr.exclude,
    );
    return funcWithNullTreatment + ' OVER (' + expr.windowName + ' ' + frame + ')';
  }

  // Collect parts with their BY keyword length for alignment
  type OverPart = { text: string; byKeywordLen: number };
  const overParts: OverPart[] = [];

  if (expr.windowName) {
    overParts.push({
      text: expr.windowName,
      byKeywordLen: 0,
    });
  }

  if (expr.partitionBy) {
    overParts.push({
      text: 'PARTITION BY ' + expr.partitionBy.map(e => formatExpr(e)).join(', '),
      byKeywordLen: 12, // 'PARTITION BY'.length
    });
  }

  if (expr.orderBy) {
    overParts.push({
      text: 'ORDER BY ' + expr.orderBy.map(formatOrderByItem).join(', '),
      byKeywordLen: 8, // 'ORDER BY'.length
    });
  }

  // Compute BY-keyword alignment before generating frame text
  const byParts = overParts.filter(p => p.byKeywordLen > 0);
  const maxByLen = byParts.length > 0 ? Math.max(...byParts.map(p => p.byKeywordLen)) : 0;

  // Frame clause: align BETWEEN with BY keywords
  let frameExtraPad = 0;
  if (expr.frame) {
    // BY starts at column: overContentCol + maxByLen - 2
    // BETWEEN starts at column: overContentCol + frameExtraPad + frame.unit.length + 1
    // Align them: frameExtraPad = maxByLen - 2 - frame.unit.length - 1
    frameExtraPad = maxByLen > 0 ? Math.max(0, maxByLen - 2 - expr.frame.unit.length - 1) : 0;
    const frameStartCol = overContentCol + frameExtraPad;
    overParts.push({
      text: formatFrameClause(expr.frame, frameStartCol, expr.exclude),
      byKeywordLen: -1, // frame part
    });
  }

  if (overParts.length <= 1 && !expr.frame) {
    const inline = funcWithNullTreatment + ' OVER (' + overParts.map(p => p.text).join('') + ')';
    if (col + stringDisplayWidth(inline) <= runtime.maxLineLength) {
      return inline;
    }
  }

  const pad = ' '.repeat(overContentCol);
  let result = funcWithNullTreatment + ' OVER (';
  for (let i = 0; i < overParts.length; i++) {
    const part = overParts[i];
    let extraPad: string;
    if (part.byKeywordLen > 0 && maxByLen > 0) {
      extraPad = ' '.repeat(maxByLen - part.byKeywordLen);
    } else if (part.byKeywordLen === -1) {
      extraPad = ' '.repeat(frameExtraPad);
    } else {
      extraPad = '';
    }

    if (i === 0) {
      result += part.text;
    } else {
      result += '\n' + pad + extraPad + part.text;
    }
  }
  result += ')';
  return result;
}

function formatFrameBound(bound: AST.FrameBound): string {
  if (bound.kind === 'UNBOUNDED PRECEDING') return 'UNBOUNDED PRECEDING';
  if (bound.kind === 'UNBOUNDED FOLLOWING') return 'UNBOUNDED FOLLOWING';
  if (bound.kind === 'CURRENT ROW') return 'CURRENT ROW';
  return `${formatExpr(bound.value!)} ${bound.kind}`;
}

function formatFrameInline(frame: AST.FrameSpec, exclude?: string): string {
  let text = '';
  if (frame.end) {
    text = `${frame.unit} BETWEEN ${formatFrameBound(frame.start)} AND ${formatFrameBound(frame.end)}`;
  } else {
    text = `${frame.unit} ${formatFrameBound(frame.start)}`;
  }
  if (exclude) text += ' EXCLUDE ' + exclude;
  return text;
}

function formatFrameClause(frame: AST.FrameSpec, startCol: number, exclude?: string): string {
  if (!frame.end) {
    const head = `${frame.unit} ${formatFrameBound(frame.start)}`;
    return exclude
      ? `${head}\n${' '.repeat(startCol)}EXCLUDE ${exclude}`
      : head;
  }

  const low = formatFrameBound(frame.start);
  const high = formatFrameBound(frame.end);
  const head = `${frame.unit} BETWEEN ${low}`;
  // Right-align AND with frame unit keyword (ROWS/RANGE/GROUPS)
  const andPad = ' '.repeat(startCol + frame.unit.length - 3);
  let out = head + '\n' + andPad + 'AND ' + high;
  if (exclude) out += '\n' + ' '.repeat(startCol) + 'EXCLUDE ' + exclude;
  return out;
}

function formatWindowSpec(spec: AST.WindowSpec): string {
  const parts: string[] = [];
  if (spec.baseWindowName) {
    parts.push(spec.baseWindowName);
  }
  if (spec.partitionBy && spec.partitionBy.length > 0) {
    parts.push('PARTITION BY ' + spec.partitionBy.map(formatExpr).join(', '));
  }
  if (spec.orderBy && spec.orderBy.length > 0) {
    parts.push('ORDER BY ' + spec.orderBy.map(formatOrderByItem).join(', '));
  }
  if (spec.frame) {
    parts.push(formatFrameInline(spec.frame, spec.exclude));
  }
  return parts.join(' ');
}

// ─── INSERT ──────────────────────────────────────────────────────────

function isInsertColumnComment(text: string): boolean {
  const trimmed = text.trim();
  return trimmed.startsWith('--') || trimmed.startsWith('/*') || trimmed.startsWith('#');
}

function formatWriteIdentifier(name: string): string {
  if (isInsertColumnComment(name)) return name;
  return lowerIdent(name);
}

function formatInsertSourceAlias(
  alias: AST.InsertStatement['valuesAlias'] | undefined,
): string {
  if (!alias) return '';
  let out = ' AS ' + formatAlias(alias.name);
  if (alias.columns && alias.columns.length > 0) {
    out += '(' + alias.columns.map(lowerIdent).join(', ') + ')';
  }
  return out;
}

function formatInsertExecuteSource(source: string): string {
  return source
    .trim()
    .replace(/^EXECUTE\b/i, 'EXECUTE')
    .replace(/^EXEC\b/i, 'EXEC');
}

function shouldWrapValuesTuple(
  tupleText: string,
  prefix: string,
  maxLineLength: number,
  valueCount: number,
): boolean {
  if (valueCount <= 1) return false;
  const lineWidth = stringDisplayWidth(prefix + tupleText);
  if (valueCount >= 8 && lineWidth > maxLineLength) return true;
  return lineWidth > Math.max(maxLineLength * 2, 160);
}

function formatInsert(node: AST.InsertStatement, ctx: FormatContext): string {
  const dmlCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  const insertHead = rightAlign('INSERT', dmlCtx)
    + (node.ignore ? ' IGNORE' : '')
    + ' INTO '
    + lowerIdent(node.table)
    + (node.alias ? ' AS ' + formatAlias(node.alias) : '');
  const hasCommentColumns = node.columns.some(isInsertColumnComment);
  const formattedColumns = node.columns.map(formatWriteIdentifier);
  const inlineColumns = node.columns.length > 0
    ? ' (' + formattedColumns.join(', ') + ')'
    : '';
  const shouldWrapColumns =
    node.columns.length > 0
    && node.columns.length >= 8
    && stringDisplayWidth(insertHead + inlineColumns) > dmlCtx.runtime.maxLineLength;

  if (hasCommentColumns) {
    const colPad = contentPad(dmlCtx);
    lines.push(insertHead + ' (');
    for (let i = 0; i < formattedColumns.length; i++) {
      const text = formattedColumns[i];
      const hasComma = /,\s*$/.test(text);
      const comma = i < formattedColumns.length - 1 && !hasComma ? ',' : '';
      lines.push(colPad + text + comma);
    }
    let closeLine = colPad + ')';
    if (node.overriding) {
      closeLine += ' OVERRIDING ' + node.overriding;
    }
    lines.push(closeLine);
  } else if (shouldWrapColumns) {
    const colPad = contentPad(dmlCtx);
    lines.push(insertHead + ' (');
    for (let i = 0; i < formattedColumns.length; i++) {
      const comma = i < formattedColumns.length - 1 ? ',' : '';
      lines.push(colPad + formattedColumns[i] + comma);
    }
    let closeLine = colPad + ')';
    if (node.overriding) {
      closeLine += ' OVERRIDING ' + node.overriding;
    }
    lines.push(closeLine);
  } else {
    let header = insertHead + inlineColumns;
    if (node.overriding) {
      header += ' OVERRIDING ' + node.overriding;
    }
    lines.push(header);
  }

  if (node.valueClauseLeadingComments && node.valueClauseLeadingComments.length > 0) {
    emitComments(node.valueClauseLeadingComments, lines);
  }

  if (node.values) {
    for (let i = 0; i < node.values.length; i++) {
      const tupleNode = node.values[i];
      if (tupleNode.leadingComments && tupleNode.leadingComments.length > 0) {
        emitComments(tupleNode.leadingComments, lines);
      }
      const tupleValues = tupleNode.values.map(formatExpr);
      const tuple = '(' + tupleValues.join(', ') + ')';
      const comma = i < node.values.length - 1 ? ',' : '';
      const prefix = i === 0 ? rightAlign('VALUES', dmlCtx) + ' ' : contentPad(dmlCtx);
      const trailing = tupleNode.trailingComments && tupleNode.trailingComments.length > 0
        ? ' ' + tupleNode.trailingComments.map(c => c.text).join(' ')
        : '';
      const sourceAlias = i === node.values.length - 1 ? formatInsertSourceAlias(node.valuesAlias) : '';
      if (shouldWrapValuesTuple(tuple, prefix, dmlCtx.runtime.maxLineLength, tupleValues.length)) {
        const tuplePad = ' '.repeat(prefix.length);
        lines.push(prefix + '(');
        for (let j = 0; j < tupleValues.length; j++) {
          const valueComma = j < tupleValues.length - 1 ? ',' : '';
          lines.push(tuplePad + tupleValues[j] + valueComma);
        }
        lines.push(tuplePad + ')' + trailing + comma + sourceAlias);
      } else {
        lines.push(prefix + tuple + trailing + comma + sourceAlias);
      }
    }
  } else if (node.defaultValues) {
    lines.push(rightAlign('DEFAULT', dmlCtx) + ' VALUES');
  } else if (node.setItems && node.setItems.length > 0) {
    for (let i = 0; i < node.setItems.length; i++) {
      const item = node.setItems[i];
      const operator = item.assignmentOperator ?? '=';
      const text = item.methodCall
        ? formatExpr(item.value)
        : item.column + ' ' + operator + ' ' + formatExpr(item.value);
      const comma = i < node.setItems.length - 1 ? ',' : '';
      const sourceAlias = i === node.setItems.length - 1 ? formatInsertSourceAlias(node.valuesAlias) : '';
      if (i === 0) {
        lines.push(rightAlign('SET', dmlCtx) + ' ' + text + comma + sourceAlias);
      } else {
        lines.push(contentPad(dmlCtx) + text + comma + sourceAlias);
      }
    }
  } else if (node.tableSource) {
    let tableLine = rightAlign('TABLE', dmlCtx) + ' ' + lowerIdent(node.tableSource.table);
    if (node.tableSource.alias) {
      tableLine += ' AS ' + formatAlias(node.tableSource.alias);
      if (node.tableSource.aliasColumns && node.tableSource.aliasColumns.length > 0) {
        tableLine += '(' + node.tableSource.aliasColumns.map(lowerIdent).join(', ') + ')';
      }
    }
    lines.push(tableLine);
  } else if (node.executeSource) {
    const executeSource = formatInsertExecuteSource(node.executeSource);
    if (!hasCommentColumns && !shouldWrapColumns && node.columns.length === 0 && !node.overriding && lines.length > 0) {
      lines[lines.length - 1] += ' ' + executeSource;
    } else {
      lines.push(executeSource);
    }
  } else if (node.selectQuery) {
    lines.push(formatQueryExpressionForSubquery(node.selectQuery, dmlCtx.runtime));
  }

  if (node.onConflict) {
    let conflictTarget = '';
    if (node.onConflict.constraintName) {
      conflictTarget = ' ON CONSTRAINT ' + node.onConflict.constraintName;
    } else if (node.onConflict.columns && node.onConflict.columns.length > 0) {
      conflictTarget = ' (' + node.onConflict.columns.join(', ') + ')';
    }
    lines.push(rightAlign('ON', dmlCtx) + ' CONFLICT' + conflictTarget);

    if (node.onConflict.action === 'nothing') {
      lines.push(rightAlign('DO', dmlCtx) + ' NOTHING');
    } else {
      lines.push(rightAlign('DO', dmlCtx) + ' UPDATE');
      const conflictCtx: FormatContext = { ...dmlCtx, indentOffset: 7 };
      for (let i = 0; i < (node.onConflict.setItems || []).length; i++) {
        const item = node.onConflict.setItems![i];
        const operator = item.assignmentOperator ?? '=';
        const val = item.column + ' ' + operator + ' ' + formatExpr(item.value);
        const comma = i < node.onConflict.setItems!.length - 1 ? ',' : '';
        if (i === 0) {
          lines.push(rightAlign('SET', conflictCtx) + ' ' + val + comma);
        } else {
          lines.push(contentPad(conflictCtx) + val + comma);
        }
      }
      if (node.onConflict.where) {
        lines.push(rightAlign('WHERE', conflictCtx) + ' ' + formatCondition(node.onConflict.where, dmlCtx));
      }
    }
  }

  if (node.onDuplicateKeyUpdate && node.onDuplicateKeyUpdate.length > 0) {
    for (let i = 0; i < node.onDuplicateKeyUpdate.length; i++) {
      const item = node.onDuplicateKeyUpdate[i];
      const operator = item.assignmentOperator ?? '=';
      const val = item.column + ' ' + operator + ' ' + formatExpr(item.value);
      const comma = i < node.onDuplicateKeyUpdate.length - 1 ? ',' : '';
      if (i === 0) {
        lines.push(rightAlign('ON', dmlCtx) + ' DUPLICATE KEY UPDATE ' + val + comma);
      } else {
        lines.push(contentPad(dmlCtx) + val + comma);
      }
    }
  }

  if (appendReturningClause(lines, node.returning, dmlCtx, node.returningInto)) return lines.join('\n');
  return appendStatementSemicolon(lines.join('\n'));
}

// ─── UPDATE ──────────────────────────────────────────────────────────

function formatUpdate(node: AST.UpdateStatement, ctx: FormatContext): string {
  const dmlCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  const updateTargets: string[] = [];
  updateTargets.push(lowerIdent(node.table) + (node.alias ? ' AS ' + formatAlias(node.alias) : ''));
  if (node.additionalTables && node.additionalTables.length > 0) {
    for (const tableRef of node.additionalTables) {
      updateTargets.push(
        lowerIdent(tableRef.table) + (tableRef.alias ? ' AS ' + formatAlias(tableRef.alias) : '')
      );
    }
  }
  lines.push(rightAlign('UPDATE', dmlCtx) + ' ' + updateTargets.join(', '));

  if (node.joinSources && node.joinSources.length > 0) {
    const hasSubqueryJoins = node.joinSources.some(j => j.table.type === 'subquery');
    for (let i = 0; i < node.joinSources.length; i++) {
      const prev = i > 0 ? node.joinSources[i - 1] : undefined;
      const current = node.joinSources[i];
      const joinHasClause = !!(current.on || current.usingClause);
      const prevHasClause = !!(prev && (prev.on || prev.usingClause));
      const currentIsJoinClause = /\bJOIN$/i.test(current.joinType);
      const bothPlain = !!prev && prev.joinType === 'JOIN' && (current.joinType === 'JOIN' || currentIsJoinClause);
      const needsBlank = i > 0 && (joinHasClause || prevHasClause) && !bothPlain;
      lines.push(formatJoin(current, dmlCtx, needsBlank));
    }
    if (hasSubqueryJoins && node.joinSources.length > 1) {
      lines.push('');
    }
  }

  // SET right-aligned to river
  const setKw = rightAlign('SET', dmlCtx);
  const setContentCol = contentCol(dmlCtx);

  for (let i = 0; i < node.setItems.length; i++) {
    const item = node.setItems[i];
    const comma = i < node.setItems.length - 1 ? ',' : '';
    if (item.methodCall) {
      const val = formatExpr(item.value);
      if (i === 0) {
        lines.push(setKw + ' ' + val + comma);
      } else {
        lines.push(' '.repeat(setContentCol) + val + comma);
      }
      continue;
    }

    const columnName = lowerIdent(item.column);
    const operator = item.assignmentOperator ?? '=';
    const valueCol = i === 0
      ? setKw.length + 1 + columnName.length + operator.length + 2
      : setContentCol + columnName.length + operator.length + 2;
    const valExpr = item.value.type === 'subquery'
      ? formatSubqueryAtColumn(item.value, valueCol, dmlCtx.runtime)
      : formatExprAtColumn(item.value, valueCol, dmlCtx.runtime);
    const val = columnName + ' ' + operator + ' ' + valExpr;
    if (i === 0) {
      lines.push(setKw + ' ' + val + comma);
    } else {
      lines.push(' '.repeat(setContentCol) + val + comma);
    }
  }

  if (node.from && node.from.length > 0) {
    const fromKw = rightAlign('FROM', dmlCtx);
    lines.push(fromKw + ' ' + formatFromClause(node.from[0], dmlCtx) + (node.from.length > 1 ? ',' : ''));
    for (let i = 1; i < node.from.length; i++) {
      const comma = i < node.from.length - 1 ? ',' : '';
      lines.push(contentPad(dmlCtx) + formatFromClause(node.from[i], dmlCtx) + comma);
    }
  }

  if (node.fromJoins && node.fromJoins.length > 0) {
    const hasSubqueryJoins = node.fromJoins.some(j => j.table.type === 'subquery');
    const fromHasSubquery = node.from?.[0]?.table.type === 'subquery';
    for (let i = 0; i < node.fromJoins.length; i++) {
      const prev = i > 0 ? node.fromJoins[i - 1] : undefined;
      const current = node.fromJoins[i];
      const joinHasClause = !!(current.on || current.usingClause);
      const prevHasClause = !!(prev && (prev.on || prev.usingClause));
      const currentIsJoinClause = /\bJOIN$/i.test(current.joinType);
      const bothPlain = !!prev && prev.joinType === 'JOIN' && (current.joinType === 'JOIN' || currentIsJoinClause);
      const needsBlank = !!fromHasSubquery || (i > 0 && (joinHasClause || prevHasClause) && !bothPlain);
      lines.push(formatJoin(current, dmlCtx, needsBlank));
    }
    if (node.where && hasSubqueryJoins && node.fromJoins.length > 1) {
      lines.push('');
    }
  }

  if (node.where) {
    const whereKw = rightAlign('WHERE', dmlCtx);
    lines.push(whereKw + ' ' + formatCondition(node.where.condition, dmlCtx));
  }

  if (appendReturningClause(lines, node.returning, dmlCtx)) return lines.join('\n');

  return appendStatementSemicolon(lines.join('\n'));
}

// ─── DELETE ──────────────────────────────────────────────────────────

function formatDelete(node: AST.DeleteStatement, ctx: FormatContext): string {
  const dmlCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  const deleteKw = rightAlign('DELETE', dmlCtx);
  if (node.targets && node.targets.length > 0) {
    lines.push(deleteKw + ' ' + node.targets.map(formatAlias).join(', '));
  } else {
    lines.push(deleteKw);
  }
  lines.push(rightAlign('FROM', dmlCtx) + ' ' + lowerIdent(node.from) + (node.alias ? ' AS ' + formatAlias(node.alias) : ''));

  if (node.fromJoins && node.fromJoins.length > 0) {
    const hasSubqueryJoins = node.fromJoins.some(j => j.table.type === 'subquery');
    for (let i = 0; i < node.fromJoins.length; i++) {
      const prev = i > 0 ? node.fromJoins[i - 1] : undefined;
      const current = node.fromJoins[i];
      const joinHasClause = !!(current.on || current.usingClause);
      const prevHasClause = !!(prev && (prev.on || prev.usingClause));
      const currentIsJoinClause = /\bJOIN$/i.test(current.joinType);
      const bothPlain = !!prev && prev.joinType === 'JOIN' && (current.joinType === 'JOIN' || currentIsJoinClause);
      const needsBlank = i > 0 && (joinHasClause || prevHasClause) && !bothPlain;
      lines.push(formatJoin(current, dmlCtx, needsBlank));
    }
    if (node.where && hasSubqueryJoins && node.fromJoins.length > 1) {
      lines.push('');
    }
  }

  if (node.using && node.using.length > 0) {
    const usingKw = rightAlign('USING', dmlCtx);
    lines.push(usingKw + ' ' + formatFromClause(node.using[0], dmlCtx) + (node.using.length > 1 ? ',' : ''));
    for (let i = 1; i < node.using.length; i++) {
      const comma = i < node.using.length - 1 ? ',' : '';
      lines.push(contentPad(dmlCtx) + formatFromClause(node.using[i], dmlCtx) + comma);
    }
  }

  if (node.usingJoins && node.usingJoins.length > 0) {
    const hasSubqueryJoins = node.usingJoins.some(j => j.table.type === 'subquery');
    const usingHasSubquery = node.using?.[0]?.table.type === 'subquery';
    for (let i = 0; i < node.usingJoins.length; i++) {
      const prev = i > 0 ? node.usingJoins[i - 1] : undefined;
      const current = node.usingJoins[i];
      const joinHasClause = !!(current.on || current.usingClause);
      const prevHasClause = !!(prev && (prev.on || prev.usingClause));
      const currentIsJoinClause = /\bJOIN$/i.test(current.joinType);
      const bothPlain = !!prev && prev.joinType === 'JOIN' && (current.joinType === 'JOIN' || currentIsJoinClause);
      const needsBlank = !!usingHasSubquery || (i > 0 && (joinHasClause || prevHasClause) && !bothPlain);
      lines.push(formatJoin(current, dmlCtx, needsBlank));
    }
    if (node.where && hasSubqueryJoins && node.usingJoins.length > 1) {
      lines.push('');
    }
  }

  if (node.where) {
    const whereKw = rightAlign('WHERE', dmlCtx);
    lines.push(whereKw + ' ' + formatCondition(node.where.condition, dmlCtx));
  } else if (node.currentOf) {
    const whereKw = rightAlign('WHERE', dmlCtx);
    lines.push(whereKw + ' CURRENT OF ' + lowerIdent(node.currentOf));
  }

  if (appendReturningClause(lines, node.returning, dmlCtx)) return lines.join('\n');

  return appendStatementSemicolon(lines.join('\n'));
}

function appendReturningClause(
  lines: string[],
  returning: readonly AST.Expression[] | undefined,
  ctx: FormatContext,
  returningInto?: readonly string[],
): boolean {
  if (!returning || returning.length === 0) return false;
  let text = rightAlign('RETURNING', ctx) + ' ' + returning.map(formatExpr).join(', ');
  if (returningInto && returningInto.length > 0) {
    text += ' INTO ' + returningInto.join(', ');
  }
  lines.push(text + ';');
  return true;
}

function formatStandaloneValues(node: AST.StandaloneValuesStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  if (node.rows.length === 0) return lines.join('\n') + 'VALUES;';

  const rows = node.rows.map(r => '(' + r.values.map(formatExpr).join(', ') + ')');
  const contPad = ' '.repeat('VALUES '.length);
  for (let i = 0; i < rows.length; i++) {
    const comma = i < rows.length - 1 ? ',' : ';';
    const prefix = i === 0 ? 'VALUES ' : contPad;
    lines.push(prefix + rows[i] + comma);
  }

  return lines.join('\n');
}

function formatCreateIndex(node: AST.CreateIndexStatement, ctx: FormatContext): string {
  const idxCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  let header = 'CREATE';
  if (node.unique) header += ' UNIQUE';
  if (node.clustered) header += ' ' + node.clustered;
  header += ' INDEX';
  if (node.concurrently) header += ' CONCURRENTLY';
  if (node.ifNotExists) header += ' IF NOT EXISTS';
  if (node.name) header += ' ' + node.name;
  lines.push(header);

  const cols = node.columns.map(formatExpr).join(', ');
  const onTarget = node.only ? 'ONLY ' + node.table : node.table;
  if (node.using) {
    lines.push(rightAlign('ON', idxCtx) + ' ' + onTarget);
    lines.push(rightAlign('USING', idxCtx) + ' ' + node.using + ' (' + cols + ')');
  } else {
    lines.push(rightAlign('ON', idxCtx) + ' ' + onTarget + ' (' + cols + ')');
  }

  if (node.include && node.include.length > 0) {
    lines.push(rightAlign('INCLUDE', idxCtx) + ' (' + node.include.map(formatExpr).join(', ') + ')');
  }

  if (node.where) {
    lines.push(rightAlign('WHERE', idxCtx) + ' ' + formatCondition(node.where, idxCtx));
  }

  if (node.options) {
    lines.push(contentPad(idxCtx) + node.options);
  }

  lines[lines.length - 1] += ';';
  return lines.join('\n');
}

function formatCreateView(node: AST.CreateViewStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  let header = 'CREATE';
  if (node.orReplace) header += ' OR REPLACE';
  if (node.temporary) header += ' TEMPORARY';
  if (node.materialized) header += ' MATERIALIZED';
  header += ' VIEW';
  if (node.ifNotExists) header += ' IF NOT EXISTS';
  header += ' ' + lowerIdent(node.name);
  if (node.columnList && node.columnList.length > 0) {
    header += ' (' + node.columnList.map(lowerIdent).join(', ') + ')';
  }
  if (node.toTable) {
    header += ' TO ' + lowerIdent(node.toTable);
    if (node.toColumns && node.toColumns.length > 0) {
      header += ' (' + node.toColumns.map(lowerIdent).join(', ') + ')';
    }
  }
  if (node.comment) {
    header += ' COMMENT = ' + node.comment;
  }
  if (node.withOptions) {
    header += ' ' + node.withOptions.replace(/^WITH\(/i, 'WITH (');
  }
  header += ' AS';
  lines.push(header);

  const queryCtx: FormatContext = {
    indentOffset: 0,
    riverWidth: deriveRiverWidth(node.query as AST.Node),
    isSubquery: false,
    depth: ctx.depth + 1,
    runtime: ctx.runtime,
  };
  let queryStr = formatNode(node.query as AST.Node, queryCtx).trimEnd();
  if ((node.withData !== undefined || node.withClause !== undefined) && queryStr.endsWith(';')) {
    queryStr = queryStr.slice(0, -1);
  }
  lines.push(queryStr);

  if (node.withData !== undefined) {
    lines.push(node.withData ? '  WITH DATA;' : '  WITH NO DATA;');
  } else if (node.withClause) {
    lines.push('  ' + node.withClause + ';');
  }

  return lines.join('\n');
}

// ─── CREATE POLICY ───────────────────────────────────────────────────

function formatCreatePolicy(node: AST.CreatePolicyStatement, ctx: FormatContext): string {
  const policyCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  lines.push(`CREATE POLICY ${node.name}`);
  lines.push(rightAlign('ON', policyCtx) + ' ' + node.table);
  if (node.permissive) lines.push(rightAlign('AS', policyCtx) + ' ' + node.permissive);
  if (node.command) lines.push(rightAlign('FOR', policyCtx) + ' ' + node.command);
  if (node.roles) lines.push(rightAlign('TO', policyCtx) + ' ' + node.roles.join(', '));
  if (node.using) lines.push(rightAlign('USING', policyCtx) + ' (' + formatExpr(node.using) + ')');
  if (node.withCheck) lines.push(rightAlign('WITH', policyCtx) + ' CHECK (' + formatExpr(node.withCheck) + ')');

  lines[lines.length - 1] += ';';
  return lines.join('\n');
}

function formatGrant(node: AST.GrantStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);
  if (node.privileges.length === 0) {
    throw new Error('Invalid grant statement AST: missing privileges');
  }

  const head = node.kind
    + (node.kind === 'REVOKE' && node.grantOptionFor ? ' GRANT OPTION FOR' : '')
    + ' '
    + node.privileges.join(', ');
  lines.push(head);

  const hasRecipients = node.recipients.length > 0;
  if (node.kind === 'GRANT') {
    if (node.object) {
      lines.push(...formatGrantObjectLines(node.object, '   ON ', ctx.runtime.maxLineLength));
    }
    if (hasRecipients) {
      lines.push('   TO ' + node.recipients.join(', '));
    }
  } else {
    if (node.object) {
      lines.push(...formatGrantObjectLines(node.object, '  ON ', ctx.runtime.maxLineLength));
    }
    if (hasRecipients) {
      lines.push('FROM ' + node.recipients.join(', '));
    }
  }

  if (node.withGrantOption) {
    lines.push('WITH GRANT OPTION');
  }
  if (node.grantedBy) {
    lines.push('GRANTED BY ' + node.grantedBy);
  }
  if (node.cascade) {
    lines.push('CASCADE');
  } else if (node.restrict) {
    lines.push('RESTRICT');
  }

  lines[lines.length - 1] += ';';
  return lines.join('\n');
}

function formatGrantObjectLines(object: string, onPrefix: string, maxLineLength: number): string[] {
  const singleLine = onPrefix + object;
  if (stringDisplayWidth(singleLine) <= maxLineLength) return [singleLine];

  const fnMatch = object.match(/^FUNCTION\s+([^(]+)\(([\s\S]*)\)$/i);
  if (!fnMatch) return [singleLine];

  const functionName = fnMatch[1].trim();
  const paramsRaw = fnMatch[2].trim();
  if (!paramsRaw) return [onPrefix + `FUNCTION ${functionName}()`];

  const params = splitTopLevelCommaSegments(paramsRaw);
  if (params.length === 0) return [singleLine];

  const lines: string[] = [onPrefix + `FUNCTION ${functionName}(`];
  for (let i = 0; i < params.length; i++) {
    const comma = i < params.length - 1 ? ',' : '';
    lines.push('      ' + params[i] + comma);
  }
  lines.push('      )');
  return lines;
}

function splitTopLevelCommaSegments(text: string): string[] {
  const parts: string[] = [];
  let current = '';
  let parenDepth = 0;
  let bracketDepth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    const next = i + 1 < text.length ? text[i + 1] : '';

    if (inSingle) {
      current += ch;
      if (ch === "'" && next === "'") {
        current += next;
        i++;
      } else if (ch === "'") {
        inSingle = false;
      }
      continue;
    }
    if (inDouble) {
      current += ch;
      if (ch === '"' && next === '"') {
        current += next;
        i++;
      } else if (ch === '"') {
        inDouble = false;
      }
      continue;
    }
    if (inBacktick) {
      current += ch;
      if (ch === '`' && next === '`') {
        current += next;
        i++;
      } else if (ch === '`') {
        inBacktick = false;
      }
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      current += ch;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      current += ch;
      continue;
    }
    if (ch === '`') {
      inBacktick = true;
      current += ch;
      continue;
    }

    if (ch === '(') parenDepth++;
    else if (ch === ')') parenDepth = Math.max(0, parenDepth - 1);
    else if (ch === '[') bracketDepth++;
    else if (ch === ']') bracketDepth = Math.max(0, bracketDepth - 1);

    if (ch === ',' && parenDepth === 0 && bracketDepth === 0) {
      const trimmed = current.trim();
      if (trimmed) parts.push(trimmed);
      current = '';
      continue;
    }

    current += ch;
  }

  const tail = current.trim();
  if (tail) parts.push(tail);
  return parts;
}

function formatTruncate(node: AST.TruncateStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  lines.push('TRUNCATE ' + (node.tableKeyword ? 'TABLE ' : '') + node.table);
  const opts: string[] = [];
  if (node.restartIdentity) opts.push('RESTART IDENTITY');
  if (node.cascade) opts.push('CASCADE');
  if (opts.length > 0) {
    lines.push(opts.join(' ') + ';');
  } else {
    lines[lines.length - 1] += ';';
  }
  return lines.join('\n');
}

function formatMerge(node: AST.MergeStatement, ctx: FormatContext): string {
  const mergeCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  const target = node.target.table + (node.target.alias ? ' AS ' + node.target.alias : '');
  const sourceTable = typeof node.source.table === 'string'
    ? node.source.table
    : formatExpr(node.source.table);
  const source = sourceTable + (node.source.alias ? ' AS ' + node.source.alias : '');

  lines.push(rightAlign('MERGE', mergeCtx) + ' INTO ' + target);
  lines.push(rightAlign('USING', mergeCtx) + ' ' + source);
  const cCol = contentCol(mergeCtx);
  lines.push(rightAlign('ON', mergeCtx) + ' ' + formatCondition(node.on, mergeCtx));

  // Action body: content is indented at content column
  const actionPad = contentPad(mergeCtx);

  for (const wc of node.whenClauses) {
    const branch = wc.matched ? 'MATCHED' : 'NOT MATCHED';
    const cond = wc.condition ? ' AND ' + formatExpr(wc.condition) : '';
    lines.push(rightAlign('WHEN', mergeCtx) + ' ' + branch + cond + ' THEN');

    if (wc.action === 'delete') {
      lines.push(actionPad + 'DELETE');
      continue;
    }

    if (wc.action === 'update') {
      lines.push(actionPad + 'UPDATE');
      // SET is indented inside the action body, with its own sub-alignment
      const setOffset = cCol + 3; // 3 more spaces inside action body for SET block
      const setContinuePad = ' '.repeat(setOffset + 'SET '.length);
      for (let i = 0; i < (wc.setItems || []).length; i++) {
        const item = wc.setItems![i];
        const comma = i < wc.setItems!.length - 1 ? ',' : '';
        const itemText = item.methodCall
          ? formatExpr(item.value)
          : item.column + ' ' + (item.assignmentOperator ?? '=') + ' ' + formatExpr(item.value);
        if (i === 0) {
          lines.push(' '.repeat(setOffset) + 'SET ' + itemText + comma);
        } else {
          lines.push(setContinuePad + itemText + comma);
        }
      }
      continue;
    }

    if (wc.action === 'insert') {
      const cols = wc.columns ? ' (' + wc.columns.join(', ') + ')' : '';
      lines.push(actionPad + 'INSERT' + cols);
      lines.push(actionPad + 'VALUES (' + (wc.values || []).map(formatExpr).join(', ') + ')');
    }
  }

  lines[lines.length - 1] += ';';
  return lines.join('\n');
}

// ─── CREATE TABLE ────────────────────────────────────────────────────

function formatColumnConstraint(constraint: AST.ColumnConstraint): string {
  const prefix = constraint.name ? `CONSTRAINT ${constraint.name} ` : '';
  switch (constraint.type) {
    case 'not_null':
      return prefix + 'NOT NULL';
    case 'null':
      return prefix + 'NULL';
    case 'default':
      return prefix + 'DEFAULT ' + formatExpr(constraint.expr);
    case 'check':
      return prefix + 'CHECK(' + formatExpr(constraint.expr) + ')';
    case 'references': {
      let out = prefix + 'REFERENCES ' + constraint.table;
      if (constraint.columns && constraint.columns.length > 0) {
        out += ' (' + constraint.columns.join(', ') + ')';
      }
      if (constraint.matchType) {
        out += ' MATCH ' + constraint.matchType;
      }
      if (constraint.actions) {
        for (const action of constraint.actions) {
          out += ` ON ${action.event} ${action.action}`;
        }
      }
      if (constraint.deferrable) out += ' ' + constraint.deferrable;
      if (constraint.initially) out += ' INITIALLY ' + constraint.initially;
      return out;
    }
    case 'generated_identity':
      return `${prefix}GENERATED ${constraint.always ? 'ALWAYS' : 'BY DEFAULT'}${constraint.onNull ? ' ON NULL' : ''} AS IDENTITY${constraint.options ? ' ' + constraint.options : ''}`;
    case 'primary_key':
      return prefix + 'PRIMARY KEY';
    case 'unique':
      return prefix + 'UNIQUE';
    case 'raw':
      return normalizeRawColumnConstraint(constraint.text);
  }
}

function normalizeRawColumnConstraint(text: string): string {
  return text.replace(/\bAS\(/gi, 'AS (');
}

function wrapTextByWords(text: string, maxWidth: number): string[] {
  const words = packConstraintWordGroups(splitWordsPreservingQuotedLiterals(text.trim()));
  if (words.length === 0) return [];
  if (maxWidth < 8) return [words.join(' ')];

  const lines: string[] = [];
  let current = words[0];
  for (let i = 1; i < words.length; i++) {
    const word = words[i];
    const candidate = current + ' ' + word;
    if (stringDisplayWidth(candidate) <= maxWidth) {
      current = candidate;
      continue;
    }
    lines.push(current);
    current = word;
  }
  lines.push(current);
  return lines;
}

function packConstraintWordGroups(words: string[]): string[] {
  const packed: string[] = [];
  let i = 0;

  while (i < words.length) {
    const currentUpper = words[i]?.toUpperCase();
    const nextUpper = words[i + 1]?.toUpperCase();
    const thirdUpper = words[i + 2]?.toUpperCase();
    const fourthUpper = words[i + 3]?.toUpperCase();

    if (currentUpper === 'NOT' && nextUpper === 'NULL') {
      packed.push('NOT NULL');
      i += 2;
      continue;
    }

    if (currentUpper === 'DEFAULT' && words[i + 1]) {
      packed.push(`DEFAULT ${words[i + 1]}`);
      i += 2;
      continue;
    }

    if (currentUpper === 'NOT' && nextUpper === 'DEFERRABLE') {
      packed.push('NOT DEFERRABLE');
      i += 2;
      continue;
    }

    if (
      currentUpper === 'DEFERRABLE'
      && nextUpper === 'INITIALLY'
      && (thirdUpper === 'DEFERRED' || thirdUpper === 'IMMEDIATE')
    ) {
      packed.push(`DEFERRABLE INITIALLY ${thirdUpper}`);
      i += 3;
      continue;
    }

    if (currentUpper === 'INITIALLY' && (nextUpper === 'DEFERRED' || nextUpper === 'IMMEDIATE')) {
      packed.push(`INITIALLY ${nextUpper}`);
      i += 2;
      continue;
    }

    if (
      (currentUpper === '='
        || currentUpper === '<'
        || currentUpper === '>'
        || currentUpper === '<='
        || currentUpper === '>='
        || currentUpper === '<>'
        || currentUpper === '!=')
      && words[i + 1]
    ) {
      packed.push(`${words[i]} ${words[i + 1]}`);
      i += 2;
      continue;
    }

    if (
      currentUpper === 'ON'
      && (nextUpper === 'DELETE' || nextUpper === 'UPDATE')
      && thirdUpper
    ) {
      if (thirdUpper === 'NO' && fourthUpper === 'ACTION') {
        packed.push(`ON ${nextUpper} NO ACTION`);
        i += 4;
        continue;
      }
      if (thirdUpper === 'SET' && (fourthUpper === 'NULL' || fourthUpper === 'DEFAULT')) {
        packed.push(`ON ${nextUpper} SET ${fourthUpper}`);
        i += 4;
        continue;
      }
      packed.push(`ON ${nextUpper} ${thirdUpper}`);
      i += 3;
      continue;
    }

    if (currentUpper === 'MATCH' && (nextUpper === 'SIMPLE' || nextUpper === 'FULL' || nextUpper === 'PARTIAL')) {
      packed.push(`MATCH ${nextUpper}`);
      i += 2;
      continue;
    }

    packed.push(words[i]);
    i++;
  }

  return packed;
}

function splitWordsPreservingQuotedLiterals(text: string): string[] {
  const words: string[] = [];
  let token = '';
  let quote: "'" | '"' | null = null;

  const pushToken = () => {
    if (!token) return;
    words.push(token);
    token = '';
  };

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quote) {
      token += ch;
      if (ch === quote) {
        const next = text[i + 1];
        if (next === quote) {
          token += next;
          i++;
        } else {
          quote = null;
        }
      }
      continue;
    }

    if (ch === '\'' || ch === '"') {
      quote = ch;
      token += ch;
      continue;
    }

    if (/\s/.test(ch)) {
      pushToken();
      continue;
    }

    token += ch;
  }

  pushToken();
  return words;
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function formatColumnConstraints(constraints: readonly AST.ColumnConstraint[] | undefined): string | undefined {
  if (!constraints || constraints.length === 0) return undefined;
  return constraints.map(formatColumnConstraint).join(' ');
}

function lowerMaybeQualifiedNameWithIfNotExists(name: string): string {
  const match = name.match(/^IF\s+NOT\s+EXISTS\s+(.+)$/i);
  if (!match) return normalizeObjectName(name);
  return `IF NOT EXISTS ${normalizeObjectName(match[1])}`;
}

function lowerMaybeQualifiedNameWithIfExists(name: string): string {
  const match = name.match(/^IF\s+EXISTS\s+(.+)$/i);
  if (!match) return normalizeObjectName(name);
  return `IF EXISTS ${normalizeObjectName(match[1])}`;
}

function normalizeObjectName(name: string): string {
  const identifierCall = normalizeIdentifierCall(name);
  if (identifierCall) return identifierCall;
  return lowerIdent(name);
}

function normalizeIdentifierCall(name: string): string | null {
  const match = name.trim().match(/^IDENTIFIER\s*\(([\s\S]*)\)$/i);
  if (!match) return null;
  return `IDENTIFIER(${match[1].trim()})`;
}

function formatCreateTable(node: AST.CreateTableStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);
  const createPrefix = node.orReplace ? 'CREATE OR REPLACE TABLE ' : 'CREATE TABLE ';
  const tableName = lowerMaybeQualifiedNameWithIfNotExists(node.tableName);

  if (node.elements.length === 0 && (node.asQuery || node.asExecute)) {
    const options = node.tableOptions ? formatCreateTableOptions(node.tableOptions) : '';
    if (node.asExecute) {
      lines.push(createPrefix + tableName + options + ' AS ' + node.asExecute + ';');
      return lines.join('\n');
    }
    lines.push(createPrefix + tableName + options + ' AS');
    const query = formatQueryExpressionForSubquery(node.asQuery!, ctx.runtime);
    const queryLines = query.split('\n');
    if (queryLines.length > 0) {
      queryLines[queryLines.length - 1] = queryLines[queryLines.length - 1].replace(/;$/, '') + ';';
    }
    lines.push(...queryLines);
    return lines.join('\n');
  }

  if (node.likeTable) {
    lines.push(createPrefix + tableName + ' LIKE ' + lowerIdent(node.likeTable) + ';');
    return lines.join('\n');
  }

  lines.push(createPrefix + tableName + ' (');

  // Calculate column widths for alignment
  const colElems = node.elements.filter(e => e.elementType === 'column');
  let maxNameLen = 0;
  let maxTypeLen = 0;
  for (const col of colElems) {
    if (col.name) maxNameLen = Math.max(maxNameLen, col.name.length);
    if (col.dataType) maxTypeLen = Math.max(maxTypeLen, col.dataType.replace(/\s+/g, ' ').length);
  }
  maxTypeLen = Math.min(maxTypeLen, ctx.runtime.layoutPolicy.createTableTypeAlignMax);
  const tableConstraintIndent = ' '.repeat(Math.max(5, 4 + maxNameLen + 1));

  const hasDataElementAfter = (index: number): boolean => {
    for (let j = index + 1; j < node.elements.length; j++) {
      if (node.elements[j].elementType !== 'comment') return true;
    }
    return false;
  };

  for (let i = 0; i < node.elements.length; i++) {
    const elem = node.elements[i];
    const hasFollowingData = hasDataElementAfter(i);
    const comma = elem.elementType !== 'comment' && (hasFollowingData || (!!node.trailingComma && !hasFollowingData))
      ? ','
      : '';

    if (elem.elementType === 'comment') {
      lines.push('    ' + elem.raw);
      continue;
    }

    if (elem.elementType === 'primary_key') {
      lines.push('    ' + normalizeConstraintIdentifierCase(elem.raw) + comma);
    } else if (elem.elementType === 'column') {
      const loweredName = elem.name ? lowerIdent(elem.name) : '';
      const name = loweredName.padEnd(maxNameLen);
      const typeNorm = (elem.dataType || '').replace(/\s+/g, ' ');
      const type = typeNorm.padEnd(maxTypeLen);
      const constraints = formatColumnConstraints(elem.columnConstraints) || elem.constraints;
      const headRaw = '    ' + name + ' ' + type;
      const head = headRaw.trimEnd();
      const trailingComment = elem.trailingComment ? ' ' + elem.trailingComment : '';

      if (!constraints) {
        lines.push(head + trailingComment + comma);
        continue;
      }

      const isLongestType = typeNorm.length >= maxTypeLen;
      const constraintPad = maxTypeLen >= 13 && !isLongestType ? '' : ' ';
      let inlineRaw = headRaw + constraintPad + constraints;
      const inline = inlineRaw.trimEnd();
      if (stringDisplayWidth(inline) <= ctx.runtime.maxLineLength) {
        lines.push(inline + trailingComment + comma);
        continue;
      }

      const defaultContinuationWidth = stringDisplayWidth('    ' + name + ' ' + ' '.repeat(maxTypeLen) + ' ');
      const actualContinuationWidth = stringDisplayWidth('    ' + name + ' ' + type + ' ');
      const continuationIndentWidth = Math.min(actualContinuationWidth, defaultContinuationWidth);
      const continuationIndent = ' '.repeat(Math.max(5, continuationIndentWidth));
      const wrapped = wrapTextByWords(
        constraints,
        Math.max(20, ctx.runtime.maxLineLength - continuationIndentWidth),
      );

      if (wrapped.length === 0) {
        lines.push(head + comma);
        continue;
      }

      const firstInline = headRaw + constraintPad + wrapped[0];
      let wrappedStartIndex = 0;
      if (stringDisplayWidth(firstInline) <= ctx.runtime.maxLineLength) {
        const firstIsOnly = wrapped.length === 1;
        lines.push(firstInline.trimEnd() + (firstIsOnly ? trailingComment + comma : ''));
        wrappedStartIndex = 1;
      } else {
        lines.push(head);
      }

      for (let j = wrappedStartIndex; j < wrapped.length; j++) {
        const isLastWrapped = j === wrapped.length - 1;
        const maybeComment = isLastWrapped ? trailingComment : '';
        lines.push(continuationIndent + wrapped[j] + maybeComment + (isLastWrapped ? comma : ''));
      }
    } else if (elem.elementType === 'constraint') {
      if (elem.constraintName) {
        const headIndent = elem.constraintType === 'check' ? tableConstraintIndent : '    ';
        const bodyIndent = elem.constraintType === 'check' ? tableConstraintIndent : '        ';
        lines.push(headIndent + 'CONSTRAINT ' + elem.constraintName);
        if (elem.constraintType === 'check' && elem.checkExpr) {
          lines.push(bodyIndent + 'CHECK(' + formatExpr(elem.checkExpr) + ')');
        } else if (elem.constraintBody) {
          lines.push(bodyIndent + normalizeConstraintIdentifierCase(elem.constraintBody));
        } else {
          const pattern = new RegExp(`^CONSTRAINT\\s+${escapeRegExp(elem.constraintName)}\\b\\s*`, 'i');
          const body = elem.raw.trim().replace(pattern, '').trim();
          if (body) {
            lines.push(bodyIndent + normalizeConstraintIdentifierCase(body));
          }
        }
      } else if (elem.constraintType === 'check' && elem.checkExpr) {
        lines.push('    CHECK(' + formatExpr(elem.checkExpr) + ')');
      } else if (elem.constraintBody) {
        lines.push('    ' + normalizeConstraintIdentifierCase(elem.constraintBody));
      } else {
        lines.push('    ' + normalizeConstraintIdentifierCase(elem.raw));
      }
      if (comma) lines[lines.length - 1] += comma;
    } else if (elem.elementType === 'foreign_key') {
      if (elem.constraintName) {
        lines.push('    CONSTRAINT ' + elem.constraintName);
        lines.push('        ' + normalizeConstraintIdentifierCase('FOREIGN KEY (' + elem.fkColumns + ')'));
      } else {
        lines.push('    ' + normalizeConstraintIdentifierCase('FOREIGN KEY (' + elem.fkColumns + ')'));
      }
      let referenceLine = '        REFERENCES ' + elem.fkRefTable;
      if (elem.fkRefColumns) {
        referenceLine += ' (' + elem.fkRefColumns + ')';
      }
      referenceLine = '        ' + normalizeConstraintIdentifierCase(referenceLine.trim());
      const actionLines = elem.fkActions
        ? elem.fkActions.split(/\n/).map(action => action.trim()).filter(Boolean)
        : [];
      if (actionLines.length > 0 && actionLines[0].startsWith('MATCH ')) {
        referenceLine += ' ' + actionLines.shift();
      }
      lines.push(referenceLine);
      for (const action of actionLines) {
          const trimmed = action.trim();
          if (trimmed) lines.push('        ' + trimmed);
      }
      if (comma) lines[lines.length - 1] += comma;
    }
  }

  const options = node.tableOptions ? formatCreateTableOptions(node.tableOptions) : '';
  if (node.asQuery) {
    lines.push(')' + options + ' AS');
    const query = formatQueryExpressionForSubquery(node.asQuery, ctx.runtime);
    const queryLines = query.split('\n');
    if (queryLines.length > 0) {
      queryLines[queryLines.length - 1] = queryLines[queryLines.length - 1].replace(/;$/, '') + ';';
    }
    lines.push(...queryLines);
    return lines.join('\n');
  }
  if (node.asExecute) {
    lines.push(')' + options + ' AS ' + node.asExecute + ';');
    return lines.join('\n');
  }
  lines.push(')' + options + ';');
  return lines.join('\n');
}

function formatCreateTableOptions(options: string): string {
  let normalized = options
    .replace(/\bWITH\(/gi, 'WITH (')
    .replace(/\bORDER\s+BY\(/gi, 'ORDER BY (');

  if (!/\bENGINE\b/i.test(normalized)) {
    return ' ' + normalized;
  }

  const clausePatterns = [
    /\bPARTITION\s+BY\b/i,
    /\bORDER\s+BY\b/i,
    /\bPRIMARY\s+KEY\b/i,
    /\bSAMPLE\s+BY\b/i,
    /\bSETTINGS\b/i,
  ];
  const matches = clausePatterns
    .map(pattern => ({ pattern, index: normalized.search(pattern) }))
    .filter(item => item.index >= 0)
    .sort((a, b) => a.index - b.index);

  if (matches.length === 0) {
    return ' ' + normalized;
  }

  const firstBreak = matches[0].index;
  const head = normalized.slice(0, firstBreak).trimEnd();
  const tail = normalized.slice(firstBreak).trim();
  const tailLines = tail
    .replace(/\s+(PARTITION\s+BY|ORDER\s+BY|PRIMARY\s+KEY|SAMPLE\s+BY|SETTINGS)\b/gi, '\n$1')
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => '  ' + line);

  return ' ' + head + '\n' + tailLines.join('\n');
}

// ─── ALTER TABLE ─────────────────────────────────────────────────────

function formatAlterTable(node: AST.AlterTableStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  const objectType = node.objectType || 'TABLE';
  const header = `ALTER ${objectType} ${lowerMaybeQualifiedNameWithIfExists(node.objectName)}`;

  const actions = node.actions && node.actions.length > 0
    ? node.actions.map(formatAlterAction)
    : [];
  if (actions.length === 0) {
    lines.push(header + ';');
    return lines.join('\n');
  }
  lines.push(header);
  for (let i = 0; i < actions.length; i++) {
    const comma = i < actions.length - 1 ? ',' : ';';
    const actionLines = actions[i].split('\n');
    lines.push(' '.repeat(8) + actionLines[0]);
    for (let j = 1; j < actionLines.length; j++) {
      lines.push(' '.repeat(8) + actionLines[j]);
    }
    lines[lines.length - 1] += comma;
  }

  return lines.join('\n');
}

function formatAlterAction(action: AST.AlterAction): string {
  switch (action.type) {
    case 'add_column': {
      let out = 'ADD ';
      if (action.explicitColumnKeyword || action.ifNotExists) {
        out += 'COLUMN ';
      }
      if (action.ifNotExists) out += 'IF NOT EXISTS ';
      out += lowerIdent(action.columnName);
      if (action.definition) out += ' ' + action.definition;
      return out;
    }
    case 'drop_column': {
      let out = 'DROP COLUMN ';
      if (action.ifExists) out += 'IF EXISTS ';
      out += lowerIdent(action.columnName);
      if (action.behavior) out += ' ' + action.behavior;
      return out;
    }
    case 'drop_constraint': {
      let out = 'DROP CONSTRAINT ';
      if (action.ifExists) out += 'IF EXISTS ';
      out += action.constraintName;
      if (action.behavior) out += ' ' + action.behavior;
      return out;
    }
    case 'alter_column':
      return `ALTER COLUMN ${lowerIdent(action.columnName)} ${action.operation}`;
    case 'owner_to':
      return `OWNER TO ${lowerIdent(action.owner)}`;
    case 'rename_to':
      return `RENAME TO ${lowerIdent(action.newName)}`;
    case 'rename_column':
      return `RENAME COLUMN ${lowerIdent(action.columnName)} TO ${lowerIdent(action.newName)}`;
    case 'set_schema':
      return `SET SCHEMA ${lowerIdent(action.schema)}`;
    case 'set_tablespace':
      return `SET TABLESPACE ${lowerIdent(action.tablespace)}`;
    case 'raw':
      return formatRawAlterAction(action.text);
    default: {
      const _exhaustive: never = action;
      throw new Error(`Unknown alter action type: ${(_exhaustive as { type?: string }).type}`);
    }
  }
}

function formatRawAlterAction(text: string): string {
  const normalized = normalizeConstraintSpacing(text)
    .replace(/\bCHARACTER\s+SET\b/gi, 'CHARACTER SET')
    .trim();
  const normalizedIdentifiers = normalizeConstraintIdentifierCase(normalized);

  const fkMatch = normalizedIdentifiers.match(
    /^ADD\s+CONSTRAINT\s+(\S+)\s+FOREIGN\s+KEY\s*\(([^)]*)\)\s+REFERENCES\s+([^\s(]+)(?:\s*\(([^)]*)\))?(.*)$/i
  );
  if (fkMatch) {
    const [, name, fkCols, refTable, refCols, tail] = fkMatch;
    const lines: string[] = [];
    lines.push(`ADD CONSTRAINT ${name}`);
    lines.push(`FOREIGN KEY (${normalizeConstraintIdentifierList(fkCols.trim())})`);
    let ref = `REFERENCES ${normalizeConstraintIdentifierToken(refTable)}`;
    if (refCols && refCols.trim()) ref += ` (${normalizeConstraintIdentifierList(refCols.trim())})`;
    lines.push(ref);

    const actionTail = tail.trim();
    if (actionTail) {
      const packed = packConstraintWordGroups(splitWordsPreservingQuotedLiterals(actionTail));
      for (const segment of packed) {
        lines.push(segment);
      }
    }
    return lines.join('\n');
  }

  return normalizedIdentifiers;
}

function normalizeConstraintSpacing(text: string): string {
  return text
    .replace(/\bPRIMARY\s+KEY\s*\(/gi, 'PRIMARY KEY (')
    .replace(/\bFOREIGN\s+KEY\s*\(/gi, 'FOREIGN KEY (')
    .replace(/\bUNIQUE\s+KEY\s*\(/gi, 'UNIQUE KEY (')
    .replace(/\bUNIQUE\s+INDEX\s*\(/gi, 'UNIQUE INDEX (')
    .replace(/\bKEY\s*\(/gi, 'KEY (')
    .replace(/\bINDEX\s*\(/gi, 'INDEX (')
    .replace(/\bKEY\s+([^\s(]+)\s*\(/gi, 'KEY $1 (')
    .replace(/\bINDEX\s+([^\s(]+)\s*\(/gi, 'INDEX $1 (')
    .replace(/\bREFERENCES\s+([^\s(]+)\s*\(/gi, 'REFERENCES $1 (');
}

function normalizeConstraintIdentifierCase(text: string): string {
  return text
    .replace(/\bPRIMARY\s+KEY\s*\(([^)]*)\)/gi, (_, cols: string) => {
      return `PRIMARY KEY (${normalizeConstraintIdentifierList(cols)})`;
    })
    .replace(/\bFOREIGN\s+KEY\s*\(([^)]*)\)/gi, (_, cols: string) => {
      return `FOREIGN KEY (${normalizeConstraintIdentifierList(cols)})`;
    })
    .replace(/\bREFERENCES\s+([^\s(]+)\s*\(([^)]*)\)/gi, (_, table: string, cols: string) => {
      return `REFERENCES ${normalizeConstraintIdentifierToken(table)} (${normalizeConstraintIdentifierList(cols)})`;
    });
}

function normalizeConstraintIdentifierList(list: string): string {
  return list
    .split(',')
    .map(item => normalizeConstraintIdentifierToken(item))
    .join(', ');
}

function normalizeConstraintIdentifierToken(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return trimmed;
  if (!/^[A-Za-z_@`"\[][A-Za-z0-9_$@.`"\[\]]*$/.test(trimmed)) return trimmed;
  return lowerIdent(trimmed);
}

// ─── DROP TABLE ──────────────────────────────────────────────────────

function formatDropTable(node: AST.DropTableStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);

  const objectType = node.objectType || 'TABLE';
  let line = `DROP ${objectType}`;
  if (node.concurrently) line += ' CONCURRENTLY';
  if (node.ifExists) line += ' IF EXISTS';
  line += ' ' + lowerIdent(node.objectName);
  if (node.behavior) line += ' ' + node.behavior;
  line += ';';
  lines.push(line);
  return lines.join('\n');
}

// ─── UNION / INTERSECT / EXCEPT ──────────────────────────────────────

function formatUnion(node: AST.UnionStatement, ctx: FormatContext): string {
  const parts: string[] = [];
  emitComments(node.leadingComments, parts);
  const hasTail = !!(node.orderBy || node.limit || node.offset || node.fetch || node.lockingClause);

  for (let i = 0; i < node.members.length; i++) {
    const member = node.members[i];
    const isLast = i === node.members.length - 1;

    if (member.parenthesized) {
      // Format inner with no trailing semicolon, then shift subsequent lines by 1 for the paren
      const inner = formatQueryExpressionForSubquery(
        member.statement,
        ctx.runtime,
        1,
        ctx.depth + 1,
      );
      const innerLines = inner.split('\n');
      let str = '(' + innerLines[0];
      for (let j = 1; j < innerLines.length; j++) {
        str += '\n' + ' ' + innerLines[j];
      }
      str += ')';
      if (isLast && !ctx.isSubquery && !hasTail) {
        parts.push(str + ';');
      } else {
        parts.push(str);
      }
    } else {
      // Not parenthesized
      if (member.statement.type === 'select') {
        const selectCtx: FormatContext = {
          ...ctx,
          riverWidth: deriveSelectRiverWidth(member.statement),
          isSubquery: ctx.isSubquery ? true : (!isLast || hasTail),
        };
        parts.push(formatSelect(member.statement, selectCtx));
      } else {
        // Defensive fallback; parse currently emits non-parenthesized SELECT members.
        parts.push(formatQueryExpressionForSubquery(member.statement, ctx.runtime, undefined, ctx.depth + 1));
      }
    }

    if (i < node.operators.length) {
      const op = node.operators[i];
      const nextMember = node.members[i + 1];
      const isParenthesizedBridge = !!(member.parenthesized && nextMember?.parenthesized);
      if (member.parenthesized) {
        // Inside parenthesized union, operator indented to river
        if (isParenthesizedBridge) {
          parts.push('');
          parts.push('  ' + op);
          parts.push('');
        } else {
          parts.push('  ' + op);
        }
      } else {
        // Align operator to the river
        const firstWord = op.split(' ')[0];
        const rest = op.split(' ').slice(1).join(' ');
        const aligned = rightAlign(firstWord, ctx);
        parts.push(rest ? aligned + ' ' + rest : aligned);
      }
    }
  }

  if (node.orderBy) {
    const kw = rightAlign('ORDER', ctx);
    parts.push(...formatSelectOrderByLines(node.orderBy.items, kw, contentPad(ctx)));
  }
  if (node.limit) {
    parts.push(rightAlign('LIMIT', ctx) + ' ' + formatExpr(node.limit.count));
  }
  if (node.offset) {
    const rows = node.offset.rowsKeyword ? ' ROWS' : '';
    parts.push(rightAlign('OFFSET', ctx) + ' ' + formatExpr(node.offset.count) + rows);
  }
  if (node.fetch) {
    const ties = node.fetch.withTies ? ' WITH TIES' : ' ONLY';
    parts.push(rightAlign('FETCH', ctx) + ' FIRST ' + formatExpr(node.fetch.count) + ' ROWS' + ties);
  }
  if (node.lockingClause) {
    parts.push(rightAlign('FOR', ctx) + ' ' + node.lockingClause);
  }

  if (!ctx.isSubquery && hasTail && parts.length > 0) {
    const lastIdx = parts.length - 1;
    parts[lastIdx] = parts[lastIdx].replace(/;$/, '') + ';';
  }

  return parts.join('\n');
}

// ─── CTE (WITH) ─────────────────────────────────────────────────────

function formatCTE(node: AST.CTEStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  emitComments(node.leadingComments, lines);
  const firstCTEHasComments = !!(node.ctes[0]?.leadingComments && node.ctes[0].leadingComments.length > 0);
  if (
    node.leadingComments.length > 0
    && !firstCTEHasComments
    && lines.length > 0
    && lines[lines.length - 1] !== ''
  ) {
    lines.push('');
  }

  const withKw = rightAlign('WITH', ctx);
  const cteBodyIndent = contentCol(ctx) + 4; // align CTE body SELECT inside parens

  for (let i = 0; i < node.ctes.length; i++) {
    const cte = node.ctes[i];
    const isFirst = i === 0;
    const isLast = i === node.ctes.length - 1;

    // Emit leading comments for this CTE (comments between CTEs)
    if (cte.leadingComments && cte.leadingComments.length > 0) {
      emitCTELeadingComments(cte.leadingComments, lines, contentCol(ctx));
    } else if (!isFirst && !cte.materialized) {
      const bodyIdx = lines.length - 2;
      if (bodyIdx >= 0) {
        const bodyLineCount = (lines[bodyIdx].match(/\n/g) || []).length + 1;
        if (bodyLineCount > 4) {
          lines.push('');
        }
      }
    }

    const firstPrefix = node.recursive ? withKw + ' RECURSIVE ' : withKw + ' ';
    const prefix = isFirst ? firstPrefix : contentPad(ctx);
    const colList = cte.columnList ? ' (' + cte.columnList.join(', ') + ')' : '';
    const materialized =
      cte.materialized === 'materialized' ? ' MATERIALIZED'
      : cte.materialized === 'not_materialized' ? ' NOT MATERIALIZED'
      : '';
    lines.push(prefix + cte.name + colList + ' AS' + materialized + ' (');

    // CTE body
    const bodyCtx: FormatContext = {
      indentOffset: cteBodyIndent,
      riverWidth: deriveRiverWidth(cte.query as AST.Node),
      isSubquery: true,
      depth: ctx.depth + 1,
      runtime: ctx.runtime,
    };

    if (cte.query.type === 'values') {
      lines.push(formatValuesClause(cte.query, bodyCtx));
    } else if (cte.query.type === 'union') {
      lines.push(formatUnion(cte.query, bodyCtx));
    } else {
      lines.push(formatSelect(cte.query as AST.SelectStatement, bodyCtx));
    }

    // Closing ) — aligned to content column
    const closeIndent = ' '.repeat(contentCol(ctx));
    lines.push(closeIndent + ')' + (isLast ? '' : ','));
  }

  if (node.search) {
    const kw = rightAlign('SEARCH', ctx);
    lines.push(`${kw} ${node.search.mode} BY ${node.search.by.join(', ')} SET ${node.search.set}`);
  }
  if (node.cycle) {
    const kw = rightAlign('CYCLE', ctx);
    let cycle = `${kw} ${node.cycle.columns.join(', ')} SET ${node.cycle.set}`;
    if (node.cycle.to) cycle += ` TO ${formatExpr(node.cycle.to)}`;
    if (node.cycle.default) cycle += ` DEFAULT ${formatExpr(node.cycle.default)}`;
    if (node.cycle.using) cycle += ` USING ${node.cycle.using}`;
    lines.push(cycle);
  }

  // Main query
  const mainCtx: FormatContext = {
    indentOffset: 0,
    riverWidth: deriveRiverWidth(node.mainQuery),
    isSubquery: ctx.isSubquery,
    depth: ctx.depth + 1,
    runtime: ctx.runtime,
  };
  // Emit leading comments before main query (we handle them here, so clear them
  // from the node to avoid double-emitting in formatSelect/formatUnion)
  if (node.mainQuery.leadingComments && node.mainQuery.leadingComments.length > 0) {
    emitComments(node.mainQuery.leadingComments, lines);
  }
  lines.push(formatNode({ ...node.mainQuery, leadingComments: [] }, mainCtx));

  return lines.join('\n');
}

// Emit CTE-leading comments as-is so comment style is preserved.
function emitCTELeadingComments(comments: readonly AST.CommentNode[], lines: string[], _cteIndentCol: number): void {
  for (const c of comments) {
    const blanks = Math.min(c.blankLinesBefore || 0, 1);
    for (let i = 0; i < blanks; i++) {
      lines.push('');
    }
    lines.push(c.text);
  }
  if (lines.length > 0 && lines[lines.length - 1] !== '') {
    lines.push('');
  }
}

// Emit comments to a lines array, preserving blank lines between comment groups
function emitComments(comments: readonly AST.CommentNode[], lines: string[]): void {
  for (const c of comments) {
    const blanks = c.blankLinesBefore || 0;
    for (let i = 0; i < blanks; i++) {
      lines.push('');
    }
    lines.push(c.text);
    const blanksAfter = c.blankLinesAfter || 0;
    for (let i = 0; i < blanksAfter; i++) {
      lines.push('');
    }
  }
}

// ─── VALUES clause ───────────────────────────────────────────────────

function formatValuesClause(node: AST.ValuesClause, ctx: FormatContext): string {
  const lines: string[] = [];
  const valuesIndent = ' '.repeat(ctx.indentOffset);
  const rowIndent = valuesIndent + '    '; // 4 more spaces for rows

  lines.push(valuesIndent + 'VALUES');

  // Count non-empty rows for comma placement
  const totalDataRows = node.rows.filter(r => r.values.length > 0).length;
  let dataRowIndex = 0;

  for (const row of node.rows) {
    // Emit leading comments for this row
    if (row.leadingComments) {
      for (const c of row.leadingComments) {
        lines.push(rowIndent + c.text);
      }
    }

    // Skip empty rows (comment-only rows)
    if (row.values.length === 0) continue;

    dataRowIndex++;
    const vals = '(' + row.values.map(formatExpr).join(', ') + ')';
    const comma = dataRowIndex < totalDataRows ? ',' : '';
    const trailing = row.trailingComment ? '  ' + row.trailingComment.text : '';
    lines.push(rowIndent + vals + comma + trailing);
  }

  return lines.join('\n');
}

// ─── Expression formatting (context-free) ────────────────────────────

// Simple fallback: space-separated tokens for depth-exceeded expressions
function formatExprSimpleFallback(expr: AST.Expression): string {
  // Use depth=0 for the simple formatting — this won't recurse deeply
  // because it only produces inline token output
  return formatExpr(expr, 0);
}

function formatExpr(expr: AST.Expression, depth: number = 0): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    // At max depth, produce a minimal inline representation to avoid stack overflow.
    // We call the leaf formatters directly to avoid infinite recursion.
    if (expr.type === 'identifier') {
      const ident = expr.quoted ? expr.value : lowerIdent(expr.value);
      return expr.withDescendants ? ident + '*' : ident;
    }
    if (expr.type === 'literal') return expr.literalType === 'boolean' ? expr.value.toUpperCase() : expr.value;
    if (expr.type === 'null') return 'NULL';
    if (expr.type === 'star') return expr.qualifier ? lowerIdent(expr.qualifier) + '.*' : '*';
    if (expr.type === 'tuple') return '(' + expr.items.map(item => formatExpr(item, depth)).join(', ') + ')';
    if (expr.type === 'raw') return expr.text;
    // For anything else at max depth, return a best-effort inline string
    return '/* depth exceeded */';
  }
  const d = depth + 1;
  switch (expr.type) {
    case 'identifier':
      return (expr.quoted ? expr.value : lowerIdent(expr.value)) + (expr.withDescendants ? '*' : '');
    case 'literal':
      if (expr.literalType === 'boolean') return expr.value.toUpperCase();
      return expr.value;
    case 'null':
      return 'NULL';
    case 'interval':
      return `INTERVAL ${expr.value}`;
    case 'typed_string':
      return `${expr.dataType} ${expr.value}`;
    case 'star':
      return expr.qualifier ? lowerIdent(expr.qualifier) + '.*' : '*';
    case 'binary':
      if (expr.operator === ':') {
        return formatExpr(expr.left, d) + ':' + formatExpr(expr.right, d);
      }
      return formatExpr(expr.left, d) + ' ' + expr.operator + ' ' + formatExpr(expr.right, d);
    case 'unary':
      // Special case: NOT EXISTS should format like EXISTS with NOT prefix
      if (expr.operator === 'NOT' && expr.operand.type === 'exists') {
        return 'NOT EXISTS ' + formatSubquerySimple(expr.operand.subquery);
      }
      // No space for unary minus (e.g., -1), space for NOT
      if (expr.operator === '-' || expr.operator === '~') return expr.operator + formatExpr(expr.operand, d);
      return expr.operator + ' ' + formatExpr(expr.operand, d);
    case 'function_call':
      return formatFunctionCall(expr, d);
    case 'subquery':
      return formatSubquerySimple(expr);
    case 'case':
      return formatCaseSimple(expr, d);
    case 'between': {
      const neg = expr.negated ? 'NOT ' : '';
      return formatExpr(expr.expr, d) + ' ' + neg + 'BETWEEN ' + formatExpr(expr.low, d) + ' AND ' + formatExpr(expr.high, d);
    }
    case 'quantified_comparison': {
      const left = formatExpr(expr.left, d);
      if (expr.kind === 'subquery') {
        return left + ' ' + expr.operator + ' ' + expr.quantifier + ' ' + formatSubquerySimple(expr.subquery);
      }
      return left + ' ' + expr.operator + ' ' + expr.quantifier + '(' + expr.values.map(v => formatExpr(v, d)).join(', ') + ')';
    }
    case 'in': {
      const neg = expr.negated ? 'NOT ' : '';
      if (isInExprSubquery(expr)) {
        return formatExpr(expr.expr, d) + ' ' + neg + 'IN ' + formatSubquerySimple(getInExprSubquery(expr));
      }
      const vals = getInExprList(expr).map(v => formatExpr(v, d)).join(', ');
      return formatExpr(expr.expr, d) + ' ' + neg + 'IN (' + vals + ')';
    }
    case 'is':
      return formatExpr(expr.expr, d) + ' IS ' + expr.value;
    case 'like': {
      const neg = expr.negated ? 'NOT ' : '';
      let out = formatExpr(expr.expr, d) + ' ' + neg + 'LIKE ' + formatExpr(expr.pattern, d);
      if (expr.escape) out += ' ESCAPE ' + formatExpr(expr.escape, d);
      return out;
    }
    case 'exists':
      return 'EXISTS ' + formatSubquerySimple(expr.subquery);
    case 'paren':
      return '(' + formatExpr(expr.expr, d) + ')';
    case 'tuple':
      return '(' + expr.items.map(item => formatExpr(item, d)).join(', ') + ')';
    case 'cast':
      return 'CAST(' + formatExpr(expr.expr, d) + ' AS ' + expr.targetType + ')';
    case 'window_function':
      return formatWindowFunctionSimple(expr, d);
    case 'extract':
      return 'EXTRACT(' + expr.field + ' FROM ' + formatExpr(expr.source, d) + ')';
    case 'position':
      return `POSITION(${formatExpr(expr.substring, d)} IN ${formatExpr(expr.source, d)})`;
    case 'substring': {
      if (expr.style === 'comma') {
        let out = `SUBSTRING(${formatExpr(expr.source, d)}, ${formatExpr(expr.start, d)}`;
        if (expr.length) out += `, ${formatExpr(expr.length, d)}`;
        return out + ')';
      }
      let out = `SUBSTRING(${formatExpr(expr.source, d)} FROM ${formatExpr(expr.start, d)}`;
      if (expr.length) out += ` FOR ${formatExpr(expr.length, d)}`;
      return out + ')';
    }
    case 'overlay': {
      let out = `OVERLAY(${formatExpr(expr.source, d)} PLACING ${formatExpr(expr.replacement, d)} FROM ${formatExpr(expr.start, d)}`;
      if (expr.length) out += ` FOR ${formatExpr(expr.length, d)}`;
      return out + ')';
    }
    case 'trim': {
      let out = 'TRIM(';
      if (expr.side) {
        out += expr.side;
        if (expr.trimChar) out += ` ${formatExpr(expr.trimChar, d)} FROM ${formatExpr(expr.source, d)}`;
        else if (expr.fromSyntax) out += ` FROM ${formatExpr(expr.source, d)}`;
        else out += ` ${formatExpr(expr.source, d)}`;
      } else if (expr.trimChar) {
        out += `${formatExpr(expr.trimChar, d)} FROM ${formatExpr(expr.source, d)}`;
      } else if (expr.fromSyntax) {
        out += `FROM ${formatExpr(expr.source, d)}`;
      } else {
        out += formatExpr(expr.source, d);
      }
      return out + ')';
    }
    case 'aliased':
      return formatExpr(expr.expr, d) + ' AS ' + formatProjectionAlias(expr.alias);
    case 'array_subscript': {
      const lower = expr.lower ? formatExpr(expr.lower, d) : '';
      const upper = expr.upper ? formatExpr(expr.upper, d) : '';
      const body = expr.isSlice ? `${lower}:${upper}` : lower;
      return formatExpr(expr.array, d) + '[' + body + ']';
    }
    case 'ordered_expr':
      return formatExpr(expr.expr, d) + ' ' + expr.direction;
    case 'raw':
      return expr.text;
    // New expression types
    case 'pg_cast':
      return formatExpr(expr.expr, d) + '::' + expr.targetType;
    case 'ilike': {
      const neg = expr.negated ? 'NOT ' : '';
      let out = formatExpr(expr.expr, d) + ' ' + neg + 'ILIKE ' + formatExpr(expr.pattern, d);
      if (expr.escape) out += ' ESCAPE ' + formatExpr(expr.escape, d);
      return out;
    }
    case 'similar_to': {
      const neg = expr.negated ? 'NOT ' : '';
      return formatExpr(expr.expr, d) + ' ' + neg + 'SIMILAR TO ' + formatExpr(expr.pattern, d);
    }
    case 'array_constructor':
      return 'ARRAY[' + expr.elements.map(e => formatExpr(e, d)).join(', ') + ']';
    case 'is_distinct_from': {
      const kw = expr.negated ? 'IS NOT DISTINCT FROM' : 'IS DISTINCT FROM';
      return formatExpr(expr.left, d) + ' ' + kw + ' ' + formatExpr(expr.right, d);
    }
    case 'regex_match':
      return formatExpr(expr.left, d) + ' ' + expr.operator + ' ' + formatExpr(expr.right, d);
    case 'collate':
      return formatExpr(expr.expr, d) + ' COLLATE ' + expr.collation;
  }

  return assertNeverExpr(expr);
}

function assertNeverExpr(expr: never): never {
  throw new FormatterError(
    `Unhandled expression node: ${(expr as { type?: string }).type ?? 'unknown'}`,
    (expr as { type?: string }).type
  );
}

function formatFunctionCall(expr: AST.FunctionCallExpr, depth: number = 0): string {
  const name = formatFunctionName(expr.name);
  const functionNameParts = splitQualifiedIdentifier(expr.name);
  const upperSimpleName = functionNameParts[functionNameParts.length - 1]?.toUpperCase() ?? '';
  const distinct = expr.distinct ? 'DISTINCT ' : '';
  const args = expr.args.map((arg, index) => {
    if (upperSimpleName === 'CONVERT' && index === 0) {
      return formatConvertTypeArgument(arg, depth);
    }
    return formatExpr(arg, depth);
  }).join(', ');
  let body = distinct + args;
  if (expr.orderBy && expr.orderBy.length > 0) {
    body += ' ORDER BY ' + expr.orderBy.map(formatOrderByItem).join(', ');
  }
  if (expr.separator) {
    body += (body ? ' ' : '') + 'SEPARATOR ' + formatExpr(expr.separator, depth);
  }

  let out = name + '(' + body + ')';
  if (expr.withinGroup) {
    out += ' WITHIN GROUP (ORDER BY ' + expr.withinGroup.orderBy.map(formatOrderByItem).join(', ') + ')';
  }
  if (expr.filter) {
    out += ' FILTER (WHERE ' + formatExpr(expr.filter, depth) + ')';
  }
  return out;
}

function formatConvertTypeArgument(expr: AST.Expression, depth: number = 0): string {
  if (expr.type === 'identifier') {
    return expr.quoted ? expr.value : expr.value.toUpperCase();
  }
  if (expr.type === 'function_call') {
    const nameParts = splitQualifiedIdentifier(expr.name);
    const typeName = nameParts.length === 1
      ? nameParts[0].toUpperCase()
      : lowerIdentStrict(expr.name);
    const args = expr.args.map(arg => formatExpr(arg, depth + 1)).join(', ');
    return `${typeName}(${args})`;
  }
  return formatExpr(expr, depth + 1);
}

function formatSubquerySimple(expr: AST.SubqueryExpr): string {
  const inner = formatQueryExpressionForSubquery(expr.query, {
    maxLineLength: TERMINAL_WIDTH,
    layoutPolicy: buildLayoutPolicy(TERMINAL_WIDTH),
  });
  return '(' + inner + ')';
}

function formatCaseSimple(expr: AST.CaseExpr, depth: number = 0): string {
  let s = 'CASE';
  if (expr.operand) s += ' ' + formatExpr(expr.operand, depth);
  for (const wc of expr.whenClauses) {
    s += ' WHEN ' + formatExpr(wc.condition, depth) + ' THEN ' + formatExpr(wc.result, depth);
    if (wc.trailingComment) s += ' ' + wc.trailingComment;
  }
  if (expr.elseResult) s += ' ELSE ' + formatExpr(expr.elseResult, depth);
  s += ' END';
  return s;
}

function formatWindowFunctionSimple(expr: AST.WindowFunctionExpr, depth: number = 0): string {
  const func = formatFunctionCall(expr.func, depth);
  const funcWithNullTreatment = expr.nullTreatment ? `${func} ${expr.nullTreatment}` : func;
  const hasWindowSpecParts = !!(expr.partitionBy || expr.orderBy || expr.frame || expr.exclude);
  if (expr.windowName && !hasWindowSpecParts) return funcWithNullTreatment + ' OVER ' + expr.windowName;
  let over = '';
  if (expr.windowName) over += expr.windowName;
  if (expr.partitionBy) {
    if (over) over += ' ';
    over += 'PARTITION BY ' + expr.partitionBy.map(e => formatExpr(e, depth)).join(', ');
  }
  if (expr.orderBy) {
    if (over) over += ' ';
    over += 'ORDER BY ' + expr.orderBy.map(formatOrderByItem).join(', ');
  }
  if (expr.frame) {
    if (over) over += ' ';
    over += formatFrameInline(expr.frame);
  }
  if (expr.exclude) {
    if (!expr.frame) {
      if (over) over += ' ';
      over += 'EXCLUDE ' + expr.exclude;
    }
  }
  return funcWithNullTreatment + ' OVER (' + over + ')';
}

function formatOrderByItem(item: AST.OrderByItem): string {
  let s = formatExpr(item.expr);
  if (item.usingOperator) s += ' USING ' + item.usingOperator;
  if (item.direction) s += ' ' + item.direction;
  if (item.nulls) s += ' NULLS ' + item.nulls;
  return s;
}

function formatAlias(alias: string): string {
  if (alias.startsWith('"')) return alias;
  if (isMixedCaseIdentifierPart(alias)) return alias;
  return alias.toLowerCase();
}

function formatProjectionAlias(alias: string): string {
  return alias;
}

function formatFunctionName(name: string): string {
  const parts = splitQualifiedIdentifier(name);
  const last = parts[parts.length - 1];
  if (isQuotedIdentifierPart(last)) return lowerIdent(name);
  const upperLast = last.toUpperCase();
  if (FUNCTION_KEYWORDS.has(upperLast) || upperLast === 'AGE') {
    parts[parts.length - 1] = upperLast;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!isQuotedIdentifierPart(parts[i]) && !parts[i].startsWith('@') && !isMixedCaseIdentifierPart(parts[i])) {
        parts[i] = parts[i].toLowerCase();
      }
    }
    return parts.join('.');
  }
  return lowerIdent(name);
}

// Lowercase identifiers, preserving qualified name dots and quoted identifiers
function lowerIdent(name: string): string {
  return splitQualifiedIdentifier(name).map(p => {
    if (p.startsWith('@')) return p;
    if (isQuotedIdentifierPart(p)) return p;
    if (isMixedCaseIdentifierPart(p)) return p;
    return p.toLowerCase();
  }).join('.');
}

function lowerIdentStrict(name: string): string {
  return splitQualifiedIdentifier(name).map(p => {
    if (p.startsWith('@')) return p;
    if (isQuotedIdentifierPart(p)) return p;
    return p.toLowerCase();
  }).join('.');
}

function isMixedCaseIdentifierPart(part: string): boolean {
  return /[A-Z]/.test(part) && /[a-z]/.test(part);
}

function isQuotedIdentifierPart(part: string): boolean {
  return part.startsWith('"') || part.startsWith('`') || part.startsWith('[');
}

function splitQualifiedIdentifier(name: string): string[] {
  const parts: string[] = [];
  let current = '';
  let quote: '"' | '`' | '[' | null = null;

  for (let i = 0; i < name.length; i++) {
    const ch = name[i];
    if (quote) {
      current += ch;
      if (quote === '[') {
        if (ch === ']') {
          if (name[i + 1] === ']') {
            current += ']';
            i++;
          } else {
            quote = null;
          }
        }
      } else if (ch === quote) {
        if (name[i + 1] === quote) {
          current += quote;
          i++;
        } else {
          quote = null;
        }
      }
      continue;
    }

    if (ch === '.') {
      parts.push(current);
      current = '';
      continue;
    }

    if (ch === '"' || ch === '`' || ch === '[') {
      quote = ch === '[' ? '[' : ch;
    }
    current += ch;
  }

  parts.push(current);
  return parts;
}
