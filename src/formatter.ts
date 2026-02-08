import * as AST from './ast';
import { FUNCTION_KEYWORDS } from './keywords';
import { DEFAULT_MAX_DEPTH, TERMINAL_WIDTH } from './constants';

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
  return parts.join('\n\n') + '\n';
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
      if (node.where) width = Math.max(width, 'WHERE'.length);
      if (node.returning && node.returning.length > 0) {
        width = Math.max(width, 'RETURNING'.length);
      }
      return width;
    }
    case 'union': {
      let width = DEFAULT_RIVER;
      for (const member of node.members) {
        width = Math.max(width, deriveSelectRiverWidth(member.statement));
      }
      for (const op of node.operators) {
        width = Math.max(width, op.split(' ')[0].length);
      }
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
      if (node.where) width = Math.max(width, 'WHERE'.length);
      return width;
    }
    case 'create_view':
      return deriveRiverWidth(node.query as AST.Node);
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
    case 'grant': return formatGrant(node, ctx);
    case 'truncate': return formatTruncate(node, ctx);
    case 'standalone_values': return formatStandaloneValues(node, ctx);
    case 'explain': return formatExplain(node, ctx);
    case 'raw': return node.text;
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

function formatExplain(node: AST.ExplainStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  const options: string[] = [];
  if (node.analyze) options.push('ANALYZE');
  if (node.verbose) options.push('VERBOSE');
  if (node.costs !== undefined) options.push(`COSTS ${node.costs ? 'ON' : 'OFF'}`);
  if (node.buffers !== undefined) options.push(`BUFFERS ${node.buffers ? 'ON' : 'OFF'}`);
  if (node.timing !== undefined) options.push(`TIMING ${node.timing ? 'ON' : 'OFF'}`);
  if (node.summary !== undefined) options.push(`SUMMARY ${node.summary ? 'ON' : 'OFF'}`);
  if (node.settings !== undefined) options.push(`SETTINGS ${node.settings ? 'ON' : 'OFF'}`);
  if (node.wal !== undefined) options.push(`WAL ${node.wal ? 'ON' : 'OFF'}`);
  if (node.format) options.push(`FORMAT ${node.format}`);

  let header = 'EXPLAIN';
  if (options.length > 0) header += ' (' + options.join(', ') + ')';
  lines.push(header);

  const inner = formatNode(node.statement, {
    ...ctx,
    riverWidth: deriveRiverWidth(node.statement),
    isSubquery: true,
    depth: ctx.depth + 1,
  });
  lines.push(inner);

  let result = lines.join('\n');
  if (!ctx.isSubquery) result += ';';
  return result;
}

// ─── SELECT ──────────────────────────────────────────────────────────

function formatSelect(node: AST.SelectStatement, ctx: FormatContext): string {
  const lines: string[] = [];

  for (const c of node.leadingComments) lines.push(c.text);

  // SELECT [DISTINCT] columns
  const selectKw = rightAlign('SELECT', ctx);
  const distinctStr = node.distinctOn
    ? ` DISTINCT ON (${node.distinctOn.map(e => formatExpr(e)).join(', ')})`
    : node.distinct
      ? ' DISTINCT'
      : '';
  const colStartCol = contentCol(ctx) + stringDisplayWidth(distinctStr);
  const colStr = formatColumnList(node.columns, colStartCol, ctx);
  lines.push(selectKw + distinctStr + ' ' + colStr);

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
    const needsBlank = fromHasSubquery || (i > 0 && (joinHasClause || prevHasClause));
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
  if (!ctx.isSubquery) result += ';';
  return result;
}

// ─── Column List ─────────────────────────────────────────────────────

interface FormattedColumnPart {
  text: string;
  comment?: AST.CommentNode;
}

function formatColumnList(columns: readonly AST.ColumnExpr[], firstColStartCol: number, ctx: FormatContext): string {
  if (columns.length === 0) return '';

  const parts = buildFormattedColumnParts(columns, ctx);
  const inlineResult = tryFormatInlineColumnList(parts, columns, firstColStartCol, ctx);
  if (inlineResult) return inlineResult;

  const hasMultiLine = parts.some(p => p.text.includes('\n'));
  const cCol = contentCol(ctx);
  const indent = ' '.repeat(cCol);

  // If any multi-line expression, one-per-line
  if (hasMultiLine) {
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
    if (col.alias && !isRedundantAlias(col.expr, col.alias)) {
      text += ' AS ' + formatAlias(col.alias);
    }
    return { text, comment: col.trailingComment };
  });
}

function hasEffectiveAlias(column: AST.ColumnExpr): boolean {
  return !!(column.alias && !isRedundantAlias(column.expr, column.alias));
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
    const groupComment = lastCol.comment ? '  ' + lastCol.comment.text : '';
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
        const comment = col.comment ? '  ' + col.comment.text : '';
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
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    const isLast = i === parts.length - 1;
    const comma = isLast ? '' : ',';
    const comment = p.comment ? '  ' + p.comment.text : '';
    const text = p.text + comma + comment;
    if (i === 0) {
      result.push(text);
    } else {
      result.push(indent + text);
    }
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
    return formatCaseAtColumn(expr, colStart, depth + 1);
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
    const thenStr = formatExpr(wc.result);
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
  if (expr.operator === '||') {
    const parts = flattenBinaryChain(expr, '||').map(part => formatExpr(part));
    const wrapPad = ' '.repeat(colStart + 2);
    const lines = [parts[0]];
    for (let i = 1; i < parts.length; i++) {
      lines.push(wrapPad + '|| ' + parts[i]);
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
  if (expr.type === 'case') return formatCaseAtColumn(expr, colStart);
  if (expr.type === 'subquery') return formatSubqueryAtColumn(expr, colStart, runtime);
  if (expr.type === 'window_function') return formatWindowFunctionAtColumn(expr, colStart, runtime);
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
  if (from.lateral) result = 'LATERAL ' + result;
  if (from.tablesample) {
    result += ' TABLESAMPLE ' + from.tablesample.method + '(' + from.tablesample.args.map(formatExpr).join(', ') + ')';
    if (from.tablesample.repeatable) {
      result += ' REPEATABLE(' + formatExpr(from.tablesample.repeatable) + ')';
    }
  }
  if (from.alias) {
    result += ' AS ' + formatAlias(from.alias);
    if (from.aliasColumns && from.aliasColumns.length > 0) {
      result += '(' + from.aliasColumns.join(', ') + ')';
    }
  }
  if (from.trailingComments && from.trailingComments.length > 0) {
    for (const comment of from.trailingComments) {
      result += ' ' + comment.text;
    }
  }
  return result;
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
      lines.push(usingPad + 'USING (' + join.usingClause.join(', ') + ')');
    }
  } else {
    // Qualified JOIN: indented at content column
    const indent = ' '.repeat(cCol);
    let tableStr = formatJoinTable(join, cCol + join.joinType.length + 1, ctx.runtime);
    lines.push(indent + join.joinType + ' ' + tableStr);

    if (join.on) {
      const cond = formatJoinOn(join.on, cCol + 3, ctx.runtime); // 3 for "ON "
      lines.push(indent + 'ON ' + cond);
    } else if (join.usingClause && join.usingClause.length > 0) {
      lines.push(indent + 'USING (' + join.usingClause.join(', ') + ')');
    }
  }

  if (join.trailingComment && lines.length > 0) {
    lines[lines.length - 1] += '  ' + join.trailingComment.text;
  }

  return lines.join('\n');
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
  if (join.lateral) result = 'LATERAL ' + result;
  if (join.alias) {
    result += ' AS ' + formatAlias(join.alias);
    if (join.aliasColumns && join.aliasColumns.length > 0) {
      result += '(' + join.aliasColumns.join(', ') + ')';
    }
  }
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
    return left + '\n' + indent + expr.operator + ' ' + formatJoinOn(expr.right, baseCol, runtime, depth + 1);
  }
  const inline = formatExpr(expr);
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
  return formatExprInCondition(expr, ctx);
}

function formatGroupByClause(groupBy: AST.GroupByClause, ctx: FormatContext): string {
  const plainItems = groupBy.items.map(e => formatExpr(e));
  if (!groupBy.groupingSets || groupBy.groupingSets.length === 0) {
    return plainItems.join(', ');
  }

  const specs = groupBy.groupingSets.map(spec => formatGroupingSpec(spec, ctx));
  const all = [...plainItems, ...specs];
  return all.join(', ');
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
    return '(' + formatParenLogical(expr.expr, contentCol(ctx) + 1) + ')';
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

function formatParenLogical(expr: AST.BinaryExpr, opCol: number, depth: number = 0): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return formatExpr(expr);
  }
  const left = formatParenOperand(expr.left, opCol, expr.operator, depth + 1);
  const right = formatParenOperand(expr.right, opCol, expr.operator, depth + 1);
  return left + '\n' + ' '.repeat(opCol) + expr.operator + ' ' + right;
}

function formatParenOperand(expr: AST.Expression, opCol: number, parentOp: string, depth: number = 0): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return formatExpr(expr);
  }
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    return formatParenLogical(expr, opCol, depth);
  }
  if (expr.type === 'paren' && expr.expr.type === 'binary' && (expr.expr.operator === 'AND' || expr.expr.operator === 'OR')) {
    return '(' + formatParenLogical(expr.expr, opCol + parentOp.length + 2, depth) + ')';
  }
  return formatExpr(expr);
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

function formatCaseAtColumn(expr: AST.CaseExpr, col: number, depth: number = 0): string {
  if (depth >= MAX_FORMATTER_DEPTH) {
    return formatCaseSimple(expr, depth);
  }
  const pad = ' '.repeat(col);
  let result = 'CASE';
  if (expr.operand) result += ' ' + formatExpr(expr.operand);
  result += '\n';

  for (const wc of expr.whenClauses) {
    const thenExpr = formatCaseThenResult(wc.result, col + 'WHEN '.length + formatExpr(wc.condition).length + ' THEN '.length, depth + 1);
    result += pad + 'WHEN ' + formatExpr(wc.condition) + ' THEN ' + thenExpr + '\n';
  }

  if (expr.elseResult) {
    result += pad + 'ELSE ' + formatExpr(expr.elseResult) + '\n';
  }

  result += pad + 'END';
  return result;
}

function formatCaseThenResult(expr: AST.Expression, col: number, depth: number = 0): string {
  if (expr.type === 'case') {
    return formatCaseAtColumn(expr, col, depth);
  }
  return formatExpr(expr);
}

// ─── Window function formatting ──────────────────────────────────────

function formatWindowFunctionAtColumn(
  expr: AST.WindowFunctionExpr,
  col: number,
  runtime: FormatterRuntime
): string {
  const func = formatFunctionCall(expr.func);
  if (expr.windowName) {
    return func + ' OVER ' + expr.windowName;
  }
  const overStart = func + ' OVER (';
  const overContentCol = col + overStart.length;

  // Collect parts with their BY keyword length for alignment
  type OverPart = { text: string; byKeywordLen: number };
  const overParts: OverPart[] = [];

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

  if (expr.frame) {
    overParts.push({
      text: formatFrameClause(expr.frame, overContentCol, expr.exclude),
      byKeywordLen: 0, // not a BY keyword
    });
  }

  if (overParts.length <= 1 && !expr.frame) {
    const inline = func + ' OVER (' + overParts.map(p => p.text).join('') + ')';
    // Keep inline if it fits within terminal width
    if (col + stringDisplayWidth(inline) <= runtime.maxLineLength) {
      return inline;
    }
    // Otherwise fall through to multi-line formatting
  }

  // Multi-line OVER: right-align BY keywords to the longest
  const byParts = overParts.filter(p => p.byKeywordLen > 0);
  const maxByLen = byParts.length > 0 ? Math.max(...byParts.map(p => p.byKeywordLen)) : 0;

  const pad = ' '.repeat(overContentCol);
  let result = func + ' OVER (';
  for (let i = 0; i < overParts.length; i++) {
    const part = overParts[i];
    const extraPad = (part.byKeywordLen > 0 && maxByLen > 0)
      ? ' '.repeat(maxByLen - part.byKeywordLen)
      : '';

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
  const offsetAdjust = /^(INTERVAL|\d+)/.test(low) ? 1 : 0;
  const head = `${frame.unit} BETWEEN ${low}`;
  const betweenIdx = head.indexOf('BETWEEN');
  const andPad = ' '.repeat(startCol + offsetAdjust + betweenIdx + 'BETWEEN '.length - 'AND '.length);
  let out = (offsetAdjust ? ' ' : '') + head + '\n' + andPad + 'AND ' + high;
  if (exclude) out += '\n' + ' '.repeat(startCol + offsetAdjust) + 'EXCLUDE ' + exclude;
  return out;
}

function formatWindowSpec(spec: AST.WindowSpec): string {
  const parts: string[] = [];
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

function formatInsert(node: AST.InsertStatement, ctx: FormatContext): string {
  const dmlCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  let header = rightAlign('INSERT', dmlCtx) + ' INTO ' + node.table;
  if (node.columns.length > 0) {
    header += ' (' + node.columns.join(', ') + ')';
  }
  if (node.overriding) {
    header += ' OVERRIDING ' + node.overriding;
  }
  lines.push(header);

  if (node.values) {
    const tuples = node.values.map(vl =>
      '(' + vl.values.map(formatExpr).join(', ') + ')'
    );
    for (let i = 0; i < tuples.length; i++) {
      const comma = i < tuples.length - 1 ? ',' : '';
      const prefix = i === 0 ? rightAlign('VALUES', dmlCtx) + ' ' : contentPad(dmlCtx);
      lines.push(prefix + tuples[i] + comma);
    }
  } else if (node.defaultValues) {
    lines.push(rightAlign('DEFAULT', dmlCtx) + ' VALUES');
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
        const val = item.column + ' = ' + formatExpr(item.value);
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

  if (appendReturningClause(lines, node.returning, dmlCtx)) return lines.join('\n');

  lines[lines.length - 1] += ';';
  return lines.join('\n');
}

// ─── UPDATE ──────────────────────────────────────────────────────────

function formatUpdate(node: AST.UpdateStatement, ctx: FormatContext): string {
  const dmlCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  lines.push(rightAlign('UPDATE', dmlCtx) + ' ' + node.table);
  if (node.alias) {
    lines[lines.length - 1] += ' AS ' + formatAlias(node.alias);
  }

  // SET right-aligned to river
  const setKw = rightAlign('SET', dmlCtx);
  const setContentCol = contentCol(dmlCtx);

  for (let i = 0; i < node.setItems.length; i++) {
    const item = node.setItems[i];
    const valueCol = i === 0
      ? setKw.length + 1 + item.column.length + 3
      : setContentCol + item.column.length + 3;
    const valExpr = item.value.type === 'subquery'
      ? formatSubqueryAtColumn(item.value, valueCol, dmlCtx.runtime)
      : formatExpr(item.value);
    const val = item.column + ' = ' + valExpr;
    const comma = i < node.setItems.length - 1 ? ',' : '';
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
      const needsBlank = !!fromHasSubquery || (i > 0 && (joinHasClause || prevHasClause));
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

  return lines.join('\n') + ';';
}

// ─── DELETE ──────────────────────────────────────────────────────────

function formatDelete(node: AST.DeleteStatement, ctx: FormatContext): string {
  const dmlCtx: FormatContext = {
    ...ctx,
    riverWidth: deriveRiverWidth(node),
  };
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  lines.push(rightAlign('DELETE', dmlCtx));
  lines.push(rightAlign('FROM', dmlCtx) + ' ' + node.from + (node.alias ? ' AS ' + formatAlias(node.alias) : ''));

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
      const needsBlank = !!usingHasSubquery || (i > 0 && (joinHasClause || prevHasClause));
      lines.push(formatJoin(current, dmlCtx, needsBlank));
    }
    if (node.where && hasSubqueryJoins && node.usingJoins.length > 1) {
      lines.push('');
    }
  }

  if (node.where) {
    const whereKw = rightAlign('WHERE', dmlCtx);
    lines.push(whereKw + ' ' + formatCondition(node.where.condition, dmlCtx));
  }

  if (appendReturningClause(lines, node.returning, dmlCtx)) return lines.join('\n');

  return lines.join('\n') + ';';
}

function appendReturningClause(
  lines: string[],
  returning: readonly AST.Expression[] | undefined,
  ctx: FormatContext
): boolean {
  if (!returning || returning.length === 0) return false;
  lines.push(rightAlign('RETURNING', ctx) + ' ' + returning.map(formatExpr).join(', ') + ';');
  return true;
}

function formatStandaloneValues(node: AST.StandaloneValuesStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

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
  for (const c of node.leadingComments) lines.push(c.text);

  let header = 'CREATE';
  if (node.unique) header += ' UNIQUE';
  header += ' INDEX';
  if (node.concurrently) header += ' CONCURRENTLY';
  if (node.ifNotExists) header += ' IF NOT EXISTS';
  header += ' ' + node.name;
  lines.push(header);

  const cols = node.columns.map(formatExpr).join(', ');
  if (node.using) {
    lines.push(rightAlign('ON', idxCtx) + ' ' + node.table);
    lines.push(rightAlign('USING', idxCtx) + ' ' + node.using + ' (' + cols + ')');
  } else {
    lines.push(rightAlign('ON', idxCtx) + ' ' + node.table + ' (' + cols + ')');
  }

  if (node.where) {
    lines.push(rightAlign('WHERE', idxCtx) + ' ' + formatCondition(node.where, idxCtx) + ';');
  } else {
    lines[lines.length - 1] += ';';
  }
  return lines.join('\n');
}

function formatCreateView(node: AST.CreateViewStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  let header = 'CREATE';
  if (node.orReplace) header += ' OR REPLACE';
  if (node.materialized) header += ' MATERIALIZED';
  header += ' VIEW';
  if (node.ifNotExists) header += ' IF NOT EXISTS';
  header += ' ' + node.name + ' AS';
  lines.push(header);

  const queryCtx: FormatContext = {
    indentOffset: 0,
    riverWidth: deriveRiverWidth(node.query as AST.Node),
    isSubquery: false,
    depth: ctx.depth + 1,
    runtime: ctx.runtime,
  };
  let queryStr = formatNode(node.query as AST.Node, queryCtx).trimEnd();
  if (node.withData !== undefined && queryStr.endsWith(';')) {
    queryStr = queryStr.slice(0, -1);
  }
  lines.push(queryStr);

  if (node.withData !== undefined) {
    lines.push(node.withData ? '  WITH DATA;' : '  WITH NO DATA;');
  }

  return lines.join('\n');
}

function formatGrant(node: AST.GrantStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);
  if (node.privileges.length === 0 || !node.object || node.recipients.length === 0) {
    throw new Error('Invalid grant statement AST: missing privileges, object, or recipients');
  }

  const head = node.kind
    + (node.kind === 'REVOKE' && node.grantOptionFor ? ' GRANT OPTION FOR' : '')
    + ' '
    + node.privileges.join(', ');
  lines.push(head);

  if (node.kind === 'GRANT') {
    lines.push('   ON ' + node.object);
    lines.push('   TO ' + node.recipients.join(', '));
  } else {
    lines.push('  ON ' + node.object);
    lines.push('FROM ' + node.recipients.join(', '));
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

function formatTruncate(node: AST.TruncateStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

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
  for (const c of node.leadingComments) lines.push(c.text);

  const target = node.target.table + (node.target.alias ? ' AS ' + node.target.alias : '');
  const source = node.source.table + (node.source.alias ? ' AS ' + node.source.alias : '');

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
        if (i === 0) {
          lines.push(' '.repeat(setOffset) + 'SET ' + item.column + ' = ' + formatExpr(item.value) + comma);
        } else {
          lines.push(setContinuePad + item.column + ' = ' + formatExpr(item.value) + comma);
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
      return `${prefix}GENERATED ${constraint.always ? 'ALWAYS' : 'BY DEFAULT'} AS IDENTITY${constraint.options ? ' ' + constraint.options : ''}`;
    case 'primary_key':
      return prefix + 'PRIMARY KEY';
    case 'unique':
      return prefix + 'UNIQUE';
    case 'raw':
      return constraint.text;
  }
}

function formatColumnConstraints(constraints: readonly AST.ColumnConstraint[] | undefined): string | undefined {
  if (!constraints || constraints.length === 0) return undefined;
  return constraints.map(formatColumnConstraint).join(' ');
}

function formatCreateTable(node: AST.CreateTableStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  lines.push('CREATE TABLE ' + node.tableName + ' (');

  // Calculate column widths for alignment
  const colElems = node.elements.filter(e => e.elementType === 'column');
  let maxNameLen = 0;
  let maxTypeLen = 0;
  for (const col of colElems) {
    if (col.name) maxNameLen = Math.max(maxNameLen, col.name.length);
    if (col.dataType) maxTypeLen = Math.max(maxTypeLen, col.dataType.replace(/\s+/g, ' ').length);
  }
  maxTypeLen = Math.min(maxTypeLen, ctx.runtime.layoutPolicy.createTableTypeAlignMax);

  for (let i = 0; i < node.elements.length; i++) {
    const elem = node.elements[i];
    const isLast = i === node.elements.length - 1;
    const comma = isLast ? '' : ',';

    if (elem.elementType === 'primary_key') {
      lines.push('    ' + elem.raw + comma);
    } else if (elem.elementType === 'column') {
      const name = (elem.name || '').padEnd(maxNameLen);
      const typeNorm = (elem.dataType || '').replace(/\s+/g, ' ');
      const type = typeNorm.padEnd(maxTypeLen);
      let line = '    ' + name + ' ' + type;
      const constraints = formatColumnConstraints(elem.columnConstraints) || elem.constraints;
      if (constraints) {
        const isLongestType = typeNorm.length >= maxTypeLen;
        if (maxTypeLen >= 13 && !isLongestType) {
          line += constraints;
        } else {
          line += ' ' + constraints;
        }
      }
      lines.push(line.trimEnd() + comma);
    } else if (elem.elementType === 'constraint') {
      // Indent constraint name to align with type column
      const constraintPad = ' '.repeat(4 + maxNameLen + 1);
      lines.push(constraintPad + 'CONSTRAINT ' + elem.constraintName);
      if (elem.constraintType === 'check' && elem.checkExpr) {
        lines.push(constraintPad + 'CHECK(' + formatExpr(elem.checkExpr) + ')');
      } else if (elem.constraintBody) {
        lines.push(constraintPad + elem.constraintBody);
      }
      // No comma after constraint body block — it's the last usually
    } else if (elem.elementType === 'foreign_key') {
      lines.push('    CONSTRAINT ' + elem.constraintName);
      lines.push('        FOREIGN KEY (' + elem.fkColumns + ')');
      lines.push('        REFERENCES ' + elem.fkRefTable + ' (' + elem.fkRefColumns + ')');
      if (elem.fkActions) {
        for (const action of elem.fkActions.split(/\n/)) {
          const trimmed = action.trim();
          if (trimmed) lines.push('        ' + trimmed);
        }
      }
      // Add comma to last line
      if (!isLast) {
        lines[lines.length - 1] += comma;
      }
    }
  }

  lines.push(');');
  return lines.join('\n');
}

// ─── ALTER TABLE ─────────────────────────────────────────────────────

function formatAlterTable(node: AST.AlterTableStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  const objectType = node.objectType || 'TABLE';
  const header = `ALTER ${objectType} ${node.objectName}`;

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
    lines.push(' '.repeat(8) + actions[i] + comma);
  }

  return lines.join('\n');
}

function formatAlterAction(action: AST.AlterAction): string {
  switch (action.type) {
    case 'add_column': {
      let out = 'ADD COLUMN ';
      if (action.ifNotExists) out += 'IF NOT EXISTS ';
      out += action.columnName;
      if (action.definition) out += ' ' + action.definition;
      return out;
    }
    case 'drop_column': {
      let out = 'DROP COLUMN ';
      if (action.ifExists) out += 'IF EXISTS ';
      out += action.columnName;
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
      return `ALTER COLUMN ${action.columnName} ${action.operation}`;
    case 'rename_to':
      return `RENAME TO ${action.newName}`;
    case 'rename_column':
      return `RENAME COLUMN ${action.columnName} TO ${action.newName}`;
    case 'set_schema':
      return `SET SCHEMA ${action.schema}`;
    case 'set_tablespace':
      return `SET TABLESPACE ${action.tablespace}`;
    case 'raw':
      return action.text;
    default: {
      const _exhaustive: never = action;
      throw new Error(`Unknown alter action type: ${(_exhaustive as { type?: string }).type}`);
    }
  }
}

// ─── DROP TABLE ──────────────────────────────────────────────────────

function formatDropTable(node: AST.DropTableStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  const objectType = node.objectType || 'TABLE';
  let line = `DROP ${objectType}`;
  if (node.concurrently) line += ' CONCURRENTLY';
  if (node.ifExists) line += ' IF EXISTS';
  line += ' ' + node.objectName;
  if (node.behavior) line += ' ' + node.behavior;
  line += ';';
  lines.push(line);
  return lines.join('\n');
}

// ─── UNION / INTERSECT / EXCEPT ──────────────────────────────────────

function formatUnion(node: AST.UnionStatement, ctx: FormatContext): string {
  const parts: string[] = [];
  for (const c of node.leadingComments) parts.push(c.text);

  for (let i = 0; i < node.members.length; i++) {
    const member = node.members[i];
    const isLast = i === node.members.length - 1;

    if (member.parenthesized) {
      // Format inner with indentOffset=0, then shift subsequent lines by 1 for the paren
      const innerCtx: FormatContext = {
        indentOffset: 0,
        riverWidth: deriveSelectRiverWidth(member.statement),
        isSubquery: true,
        outerColumnOffset: 1,
        depth: ctx.depth + 1,
        runtime: ctx.runtime,
      };
      const inner = formatSelect(member.statement, innerCtx);
      const innerLines = inner.split('\n');
      let str = '(' + innerLines[0];
      for (let j = 1; j < innerLines.length; j++) {
        str += '\n' + ' ' + innerLines[j];
      }
      str += ')';
      if (isLast && !ctx.isSubquery) {
        parts.push(str + ';');
      } else {
        parts.push(str);
      }
    } else {
      // Not parenthesized
      const selectCtx: FormatContext = {
        ...ctx,
        riverWidth: deriveSelectRiverWidth(member.statement),
        isSubquery: ctx.isSubquery ? true : !isLast, // Only last gets semicolon, unless already in subquery context
      };
      parts.push(formatSelect(member.statement, selectCtx));
    }

    if (i < node.operators.length) {
      parts.push('');
      const op = node.operators[i];
      if (member.parenthesized) {
        // Inside parenthesized union, operator indented to river
        parts.push('  ' + op);
      } else {
        // Align operator to the river
        const firstWord = op.split(' ')[0];
        const rest = op.split(' ').slice(1).join(' ');
        const aligned = rightAlign(firstWord, ctx);
        parts.push(rest ? aligned + ' ' + rest : aligned);
      }
      parts.push('');
    }
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
  if (node.mainQuery.type === 'select') {
    lines.push(formatSelect({ ...node.mainQuery, leadingComments: [] }, mainCtx));
  } else {
    lines.push(formatUnion({ ...node.mainQuery, leadingComments: [] }, mainCtx));
  }

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
    if (expr.type === 'identifier') return expr.quoted ? expr.value : lowerIdent(expr.value);
    if (expr.type === 'literal') return expr.literalType === 'boolean' ? expr.value.toUpperCase() : expr.value;
    if (expr.type === 'null') return 'NULL';
    if (expr.type === 'star') return expr.qualifier ? lowerIdent(expr.qualifier) + '.*' : '*';
    if (expr.type === 'raw') return expr.text;
    // For anything else at max depth, return a best-effort inline string
    return '/* depth exceeded */';
  }
  const d = depth + 1;
  switch (expr.type) {
    case 'identifier':
      return expr.quoted ? expr.value : lowerIdent(expr.value);
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
    case 'cast':
      return 'CAST(' + formatExpr(expr.expr, d) + ' AS ' + expr.targetType + ')';
    case 'window_function':
      return formatWindowFunctionSimple(expr, d);
    case 'extract':
      return 'EXTRACT(' + expr.field + ' FROM ' + formatExpr(expr.source, d) + ')';
    case 'position':
      return `POSITION(${formatExpr(expr.substring, d)} IN ${formatExpr(expr.source, d)})`;
    case 'substring': {
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
      return formatExpr(expr.expr, d) + ' AS ' + formatAlias(expr.alias);
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
  const distinct = expr.distinct ? 'DISTINCT ' : '';
  const args = expr.args.map(a => formatExpr(a, depth)).join(', ');
  let body = distinct + args;
  if (expr.orderBy && expr.orderBy.length > 0) {
    body += ' ORDER BY ' + expr.orderBy.map(formatOrderByItem).join(', ');
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
  }
  if (expr.elseResult) s += ' ELSE ' + formatExpr(expr.elseResult, depth);
  s += ' END';
  return s;
}

function formatWindowFunctionSimple(expr: AST.WindowFunctionExpr, depth: number = 0): string {
  const func = formatFunctionCall(expr.func, depth);
  if (expr.windowName) return func + ' OVER ' + expr.windowName;
  let over = '';
  if (expr.partitionBy) over += 'PARTITION BY ' + expr.partitionBy.map(e => formatExpr(e, depth)).join(', ');
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
  return func + ' OVER (' + over + ')';
}

function formatOrderByItem(item: AST.OrderByItem): string {
  let s = formatExpr(item.expr);
  if (item.direction) s += ' ' + item.direction;
  if (item.nulls) s += ' NULLS ' + item.nulls;
  return s;
}

// Check if an alias is redundant (matches the last component of the expression)
function isRedundantAlias(expr: AST.Expression, alias: string): boolean {
  if (expr.type === 'identifier') {
    // For "a.b", the last component is "b"
    const parts = expr.value.split('.');
    const lastPart = parts[parts.length - 1].toLowerCase();
    return lastPart === alias.toLowerCase();
  }
  return false;
}

function formatAlias(alias: string): string {
  if (alias.startsWith('"')) return alias;
  return alias.toLowerCase();
}

function formatFunctionName(name: string): string {
  const parts = name.split('.');
  const last = parts[parts.length - 1];
  if (last.startsWith('"')) return lowerIdent(name);
  const upperLast = last.toUpperCase();
  if (FUNCTION_KEYWORDS.has(upperLast)) {
    parts[parts.length - 1] = upperLast;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!parts[i].startsWith('"')) parts[i] = parts[i].toLowerCase();
    }
    return parts.join('.');
  }
  return lowerIdent(name);
}

// Lowercase identifiers, preserving qualified name dots and quoted identifiers
function lowerIdent(name: string): string {
  return name.split('.').map(p => {
    if (p.startsWith('"')) return p;
    return p.toLowerCase();
  }).join('.');
}
