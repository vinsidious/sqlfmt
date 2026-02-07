import * as AST from './ast';
import { isKeyword } from './keywords';

// The formatter walks the AST and produces formatted SQL per the Holywell style guide.
// Key concept: "The River" — top-level clause keywords are right-aligned so content
// starts at a consistent column position.

const DEFAULT_RIVER = 6; // length of SELECT keyword

interface FormatContext {
  indentOffset: number;  // extra left-margin offset for nested contexts
  riverWidth: number;
  isSubquery: boolean;
  outerColumnOffset?: number;  // additional columns in the final output (for threshold calc in subqueries)
}

export function formatStatements(nodes: AST.Node[]): string {
  const parts: string[] = [];
  for (const node of nodes) {
    parts.push(formatNode(node, {
      indentOffset: 0,
      riverWidth: deriveRiverWidth(node),
      isSubquery: false,
    }));
  }
  return parts.join('\n\n') + '\n';
}

function deriveRiverWidth(node: AST.Node): number {
  switch (node.type) {
    case 'select':
      return deriveSelectRiverWidth(node);
    case 'insert': {
      let width = 'INSERT'.length;
      if (node.values) width = Math.max(width, 'VALUES'.length);
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
      if (node.from) width = Math.max(width, 'FROM'.length);
      if (node.where) width = Math.max(width, 'WHERE'.length);
      if (node.returning && node.returning.length > 0) {
        width = Math.max(width, 'RETURNING'.length);
      }
      return width;
    }
    case 'delete': {
      let width = Math.max('DELETE'.length, 'FROM'.length);
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
    case 'create_view':
      return deriveRiverWidth(node.query as AST.Node);
    default:
      return DEFAULT_RIVER;
  }
}

function deriveSelectRiverWidth(node: AST.SelectStatement): number {
  let width = 'SELECT'.length;
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
    case 'raw': return node.text;
    case 'comment': return node.text;
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

// ─── SELECT ──────────────────────────────────────────────────────────

function formatSelect(node: AST.SelectStatement, ctx: FormatContext): string {
  const lines: string[] = [];

  for (const c of node.leadingComments) lines.push(c.text);

  // SELECT [DISTINCT] columns
  const selectKw = rightAlign('SELECT', ctx);
  const distinctStr = node.distinct ? ' DISTINCT' : '';
  const colStartCol = contentCol(ctx) + (node.distinct ? ' DISTINCT'.length : 0);
  const colStr = formatColumnList(node.columns, colStartCol, ctx);
  lines.push(selectKw + distinctStr + ' ' + colStr);

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
    lines.push(kw + ' BY ' + node.orderBy.items.map(fmtOrderByItem).join(', '));
  }

  // LIMIT
  if (node.limit) {
    lines.push(rightAlign('LIMIT', ctx) + ' ' + fmtExpr(node.limit.count));
  }

  // OFFSET
  if (node.offset) {
    const rows = node.offset.rowsKeyword ? ' ROWS' : '';
    lines.push(rightAlign('OFFSET', ctx) + ' ' + fmtExpr(node.offset.count) + rows);
  }

  // FETCH
  if (node.fetch) {
    const suffix = node.fetch.withTies ? ' WITH TIES' : ' ONLY';
    lines.push(rightAlign('FETCH', ctx) + ' FIRST ' + fmtExpr(node.fetch.count) + ' ROWS' + suffix);
  }

  let result = lines.join('\n');
  if (!ctx.isSubquery) result += ';';
  return result;
}

// ─── Column List ─────────────────────────────────────────────────────

function formatColumnList(columns: AST.ColumnExpr[], firstColStartCol: number, ctx: FormatContext): string {
  if (columns.length === 0) return '';

  // Format each column to a string, tracking which have comments
  const parts = columns.map(col => {
    let s = fmtExprInSelect(col.expr, contentCol(ctx), ctx.outerColumnOffset || 0);
    if (col.alias && !isRedundantAlias(col.expr, col.alias)) {
      s += ' AS ' + fmtAlias(col.alias);
    }
    return { text: s, comment: col.trailingComment };
  });

  const hasComments = parts.some(p => p.comment);
  const hasMultiLine = parts.some(p => p.text.includes('\n'));
  const hasAliases = columns.some(c => c.alias && !isRedundantAlias(c.expr, c.alias));
  const aliasCount = columns.filter(c => c.alias && !isRedundantAlias(c.expr, c.alias)).length;

  // Build single-line version
  const singleLine = parts.map(p => p.text).join(', ');
  const totalLen = firstColStartCol + singleLine.length;

  // Account for outer nesting (subqueries are shifted in the final output)
  const effectiveLen = totalLen + (ctx.outerColumnOffset || 0);
  const maxInlineLen = ctx.indentOffset > 0
    ? (columns.length <= 2 && hasAliases ? 66 : 80)
    : 66;

  // Single-line if fits, no comments, no multi-line expressions
  const aliasBreak = ctx.indentOffset === 0 && aliasCount >= 2 && columns.length >= 3 && effectiveLen > 50;
  const concatTailBreak = ctx.indentOffset > 0 && columns.length >= 4 && parts.slice(3).some(p => p.text.includes('||')) && effectiveLen > 66;
  if (effectiveLen <= maxInlineLen && !hasComments && !hasMultiLine && !aliasBreak && !concatTailBreak) {
    return singleLine;
  }

  const cCol = contentCol(ctx);
  const indent = ' '.repeat(cCol);

  // If any multi-line expression, one-per-line
  if (hasMultiLine) {
    return formatColumnsOnePerLine(parts, indent);
  }

  if (
    ctx.indentOffset > 0 &&
    !hasComments &&
    columns.length >= 4 &&
    parts.slice(3).some(p => p.text.includes('||'))
  ) {
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

  // Multi-line with grouped continuation:
  // First column always on its own line (the SELECT line)
  const firstComment = parts[0].comment ? '  ' + parts[0].comment.text : '';
  const firstComma = parts.length > 1 ? ',' : '';
  const lines: string[] = [parts[0].text + firstComma + firstComment];

  if (parts.length === 1) return lines[0];

  // Group remaining columns by comment boundaries
  const remaining = parts.slice(1);
  const lineGroups: typeof parts[] = [];
  let currentGroup: typeof parts = [];

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
    const groupLen = cCol + groupLine.length;

    // Get trailing comment from last column in group (if any)
    const lastCol = group[group.length - 1];
    const groupComment = lastCol.comment ? '  ' + lastCol.comment.text : '';
    const groupComma = isLastGroup ? '' : ',';

    const effectiveGroupLen = groupLen + (ctx.outerColumnOffset || 0);
    if ((group.length >= 3 && groupLen <= 66) ||
        (group.length >= 2 && (ctx.outerColumnOffset || 0) > 0 && effectiveGroupLen <= 80)) {
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

function formatColumnsOnePerLine(parts: { text: string; comment?: AST.CommentNode }[], indent: string): string {
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
function fmtExprInSelect(expr: AST.Expr, colStart: number, outerOffset: number = 0): string {
  if (expr.type === 'case') {
    return fmtCaseAtColumn(expr, colStart);
  }
  if (expr.type === 'subquery') {
    return fmtSubqueryAtColumn(expr, colStart);
  }
  if (expr.type === 'window_function') {
    return fmtWindowFunctionAtColumn(expr, colStart);
  }

  if (expr.type === 'binary' && expr.right.type === 'subquery') {
    const left = fmtExpr(expr.left);
    const op = ' ' + expr.operator + ' ';
    const subq = fmtSubqueryAtColumn(expr.right, colStart + left.length + op.length + 1);
    return left + op + subq;
  }

  // Check if single-line would be too long
  const simple = fmtExpr(expr);
  const effectiveLen = colStart + outerOffset + simple.length;

  // Function call with CASE argument that's too long — wrap compactly
  if (expr.type === 'function_call' && effectiveLen > 80) {
    const wrapped = fmtFunctionCallWrapped(expr, colStart, outerOffset);
    if (wrapped !== null) return wrapped;
    const multiline = fmtFunctionCallMultiline(expr, colStart);
    if (multiline !== null) return multiline;
  }

  // Binary expression that's too long — wrap at outermost operator
  if (expr.type === 'binary' && effectiveLen > 80) {
    return fmtBinaryWrapped(expr, colStart);
  }

  return simple;
}

// Wrap a function call when its argument is a CASE expression
function fmtFunctionCallWrapped(expr: AST.FunctionCallExpr, colStart: number, outerOffset: number): string | null {
  if (expr.args.length === 1 && expr.args[0].type === 'case') {
    const name = expr.name.toUpperCase();
    const distinct = expr.distinct ? 'DISTINCT ' : '';
    const prefix = name + '(' + distinct;
    const caseCol = colStart + prefix.length;
    const caseFmt = fmtCaseCompact(expr.args[0] as AST.CaseExpr, caseCol, outerOffset);
    return prefix + caseFmt + ')';
  }
  return null;
}

function fmtFunctionCallMultiline(expr: AST.FunctionCallExpr, colStart: number): string | null {
  const name = expr.name.toUpperCase();
  const innerCol = colStart + 4;
  const innerPad = ' '.repeat(innerCol);
  const closePad = ' '.repeat(colStart);

  if (name === 'JSONB_BUILD_OBJECT' && expr.args.length >= 6 && expr.args.length % 2 === 0) {
    const lines: string[] = [];
    lines.push(name + '(');
    for (let i = 0; i < expr.args.length; i += 2) {
      const key = fmtExpr(expr.args[i]);
      const valueCol = innerCol;
      const value = fmtExprAtColumn(expr.args[i + 1], valueCol);
      const comma = i + 2 < expr.args.length ? ',' : '';
      lines.push(innerPad + key + ', ' + value + comma);
    }
    lines.push(closePad + ')');
    return lines.join('\n');
  }

  if (name === 'GENERATE_SERIES') {
    const lines: string[] = [];
    lines.push(name + '(');
    const inlineArgs = expr.args.map(fmtExpr).join(', ');
    const preferMultiline = expr.args.every(isLiteralLike);
    if (!preferMultiline) {
      lines.push(innerPad + inlineArgs);
    } else {
      for (let i = 0; i < expr.args.length; i++) {
        const arg = fmtExprAtColumn(expr.args[i], innerCol);
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
    lines.push(innerPad + fmtExprAtColumn(expr.args[0], innerCol));
    lines.push(innerPad + 'ORDER BY ' + expr.orderBy.map(fmtOrderByItem).join(', '));
    lines.push(closePad + ')');
    return lines.join('\n');
  }

  return null;
}

// Compact CASE formatting for inside function calls:
// Single WHEN with multi-line condition:
//   CASE WHEN <multi-line cond>
//        THEN result ELSE else_result END
// Multiple WHENs:
//   CASE WHEN <cond> THEN <result>
//        WHEN <cond2> THEN <result2>
//        ELSE <else_result> END
function fmtCaseCompact(expr: AST.CaseExpr, col: number, outerOffset: number): string {
  const whenCol = col + 'CASE '.length;
  const pad = ' '.repeat(whenCol);
  const singleWhen = expr.whenClauses.length === 1;

  let result = 'CASE';
  if (expr.operand) result += ' ' + fmtExpr(expr.operand);

  for (let i = 0; i < expr.whenClauses.length; i++) {
    const wc = expr.whenClauses[i];
    const condCol = whenCol + 'WHEN '.length;
    const condStr = fmtExprColumnAware(wc.condition, condCol, outerOffset);
    const thenStr = fmtExpr(wc.result);
    const isMultiLine = condStr.includes('\n');

    if (i === 0) {
      result += ' WHEN ' + condStr;
      if (isMultiLine) {
        // For single WHEN: put THEN + ELSE + END all on one continuation line
        let line = 'THEN ' + thenStr;
        if (singleWhen && expr.elseResult) {
          line += ' ELSE ' + fmtExpr(expr.elseResult) + ' END';
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
    result += '\n' + pad + 'ELSE ' + fmtExpr(expr.elseResult);
  }
  result += ' END';
  return result;
}

// Format an expression with column-awareness (for wrapping IN lists etc.)
function fmtExprColumnAware(expr: AST.Expr, col: number, outerOffset: number): string {
  if (expr.type === 'in') {
    const simple = fmtExpr(expr);
    if (col + outerOffset + simple.length > 80) {
      return fmtInExprWrapped(expr as AST.InExpr, col);
    }
  }
  return fmtExpr(expr);
}

// Wrap IN list values across lines when too long
function fmtInExprWrapped(expr: AST.InExpr, col: number): string {
  const neg = expr.negated ? 'NOT ' : '';
  const prefix = fmtExpr(expr.expr) + ' ' + neg + 'IN (';
  const vals = (expr.values as AST.Expr[]).map(fmtExpr);
  const valStartCol = col + prefix.length;
  const valPad = ' '.repeat(valStartCol);

  let result = prefix + vals[0] + ',';
  for (let i = 1; i < vals.length; i++) {
    const comma = i < vals.length - 1 ? ',' : '';
    result += '\n' + valPad + vals[i] + comma;
  }
  result += ')';
  return result;
}

// Wrap a binary expression at the outermost operator
function fmtBinaryWrapped(expr: AST.BinaryExpr, colStart: number): string {
  const left = fmtExpr(expr.left);
  const right = fmtExpr(expr.right);
  const wrapPad = ' '.repeat(colStart + 4);
  return left + '\n' + wrapPad + expr.operator + ' ' + right;
}

function fmtExprAtColumn(expr: AST.Expr, colStart: number): string {
  if (expr.type === 'case') return fmtCaseAtColumn(expr, colStart);
  if (expr.type === 'subquery') return fmtSubqueryAtColumn(expr, colStart);
  if (expr.type === 'window_function') return fmtWindowFunctionAtColumn(expr, colStart);
  if (expr.type === 'function_call') {
    const wrapped = fmtFunctionCallMultiline(expr, colStart);
    if (wrapped) return wrapped;
  }
  return fmtExpr(expr);
}

function isLiteralLike(expr: AST.Expr): boolean {
  if (expr.type === 'literal') return true;
  if (expr.type === 'raw') return /^INTERVAL\s+/.test(expr.text) || /^NULL$/i.test(expr.text);
  if (expr.type === 'pg_cast') return isLiteralLike(expr.expr as AST.Expr);
  if (expr.type === 'paren') return isLiteralLike(expr.expr);
  return false;
}

// ─── FROM ────────────────────────────────────────────────────────────

function formatFromClause(from: AST.FromClause, ctx: FormatContext): string {
  const baseCol = contentCol(ctx);
  const lateralOffset = from.lateral && from.table.type !== 'function_call' ? 'LATERAL '.length : 0;
  let result = fmtExprAtColumn(from.table, baseCol + lateralOffset);
  if (from.lateral) result = 'LATERAL ' + result;
  if (from.tablesample) {
    result += ' TABLESAMPLE ' + from.tablesample.method + '(' + from.tablesample.args.map(fmtExpr).join(', ') + ')';
    if (from.tablesample.repeatable) {
      result += ' REPEATABLE(' + fmtExpr(from.tablesample.repeatable as AST.Expr) + ')';
    }
  }
  if (from.alias) {
    result += ' AS ' + from.alias;
    if (from.aliasColumns && from.aliasColumns.length > 0) {
      result += '(' + from.aliasColumns.join(', ') + ')';
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
    let tableStr = formatJoinTable(join, cCol);
    lines.push(kw + ' ' + tableStr);

    if (join.on) {
      // "    ON" — ON is indented 4 spaces from the river start
      const onPad = ' '.repeat(ctx.indentOffset + 4);
      const cond = formatJoinOn(join.on, ctx.indentOffset + 4 + 3);
      lines.push(onPad + 'ON ' + cond);
    } else if (join.usingClause && join.usingClause.length > 0) {
      const usingPad = ' '.repeat(ctx.indentOffset + 4);
      lines.push(usingPad + 'USING (' + join.usingClause.join(', ') + ')');
    }
  } else {
    // Qualified JOIN: indented at content column
    const indent = ' '.repeat(cCol);
    let tableStr = formatJoinTable(join, cCol + join.joinType.length + 1);
    lines.push(indent + join.joinType + ' ' + tableStr);

    if (join.on) {
      const cond = formatJoinOn(join.on, cCol + 3); // 3 for "ON "
      lines.push(indent + 'ON ' + cond);
    } else if (join.usingClause && join.usingClause.length > 0) {
      lines.push(indent + 'USING (' + join.usingClause.join(', ') + ')');
    }
  }

  return lines.join('\n');
}

function formatJoinTable(join: AST.JoinClause, tableStartCol: number): string {
  const lateralOffset = join.lateral && join.table.type !== 'function_call' ? 'LATERAL '.length : 0;
  let result = fmtExprAtColumn(join.table, tableStartCol + lateralOffset);
  if (join.lateral) result = 'LATERAL ' + result;
  if (join.alias) {
    result += ' AS ' + join.alias;
    if (join.aliasColumns && join.aliasColumns.length > 0) {
      result += '(' + join.aliasColumns.join(', ') + ')';
    }
  }
  return result;
}

function formatJoinOn(expr: AST.Expr, baseCol: number): string {
  if (expr.type === 'binary' && expr.operator === 'AND') {
    const left = formatJoinOn(expr.left, baseCol);
    const indent = ' '.repeat(baseCol);
    return left + '\n' + indent + 'AND ' + fmtExpr(expr.right);
  }
  return fmtExpr(expr);
}

// ─── WHERE/HAVING condition ──────────────────────────────────────────

function formatCondition(expr: AST.Expr, ctx: FormatContext): string {
  return formatConditionInner(expr, ctx);
}

function formatConditionInner(expr: AST.Expr, ctx: FormatContext): string {
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    const left = formatConditionInner(expr.left, ctx);
    const opKw = rightAlign(expr.operator, ctx);
    const right = formatConditionRight(expr.right, ctx);
    return left + '\n' + opKw + ' ' + right;
  }
  return fmtExprInCondition(expr, ctx);
}

function formatConditionRight(expr: AST.Expr, ctx: FormatContext): string {
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    return formatConditionInner(expr, ctx);
  }
  return fmtExprInCondition(expr, ctx);
}

function formatGroupByClause(groupBy: AST.GroupByClause, ctx: FormatContext): string {
  const plainItems = groupBy.items.map(e => fmtExpr(e));
  if (!groupBy.groupingSets || groupBy.groupingSets.length === 0) {
    return plainItems.join(', ');
  }

  const specs = groupBy.groupingSets.map(spec => formatGroupingSpec(spec, ctx));
  const all = [...plainItems, ...specs];
  return all.join(', ');
}

function formatGroupingSpec(
  spec: { type: 'grouping_sets' | 'rollup' | 'cube'; sets: AST.Expression[][] },
  ctx: FormatContext
): string {
  const kind = spec.type === 'grouping_sets' ? 'GROUPING SETS' : spec.type.toUpperCase();
  if (spec.type !== 'grouping_sets') {
    const flat = spec.sets.map(set => {
      const exprs = (set as AST.Expr[]).map(e => fmtExpr(e));
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
    const set = spec.sets[i] as AST.Expr[];
    const isLast = i === spec.sets.length - 1;
    const comma = isLast ? '' : ',';
    let text: string;

    if (spec.type === 'grouping_sets') {
      text = set.length === 0 ? '()' : '(' + set.map(e => fmtExpr(e)).join(', ') + ')';
    } else {
      if (set.length === 1) text = fmtExpr(set[0]);
      else text = '(' + set.map(e => fmtExpr(e)).join(', ') + ')';
    }
    lines.push(itemIndent + text + comma);
  }
  lines.push(closeIndent + ')');
  return lines.join('\n');
}

// Format expression in WHERE/HAVING context — handles IN subquery, EXISTS, comparisons with subqueries
function fmtExprInCondition(expr: AST.Expr, ctx: FormatContext): string {
  if (expr.type === 'paren' && expr.expr.type === 'binary' && (expr.expr.operator === 'AND' || expr.expr.operator === 'OR')) {
    return '(' + formatParenLogical(expr.expr, contentCol(ctx) + 1) + ')';
  }

  // IN with subquery
  if (expr.type === 'in' && 'type' in expr.values && (expr.values as any).type === 'subquery') {
    const e = fmtExpr(expr.expr);
    const neg = expr.negated ? 'NOT ' : '';
    const subqExpr = expr.values as AST.SubqueryExpr;

    // NOT IN: always on new line
    if (expr.negated) {
      const subq = fmtSubqueryAtColumn(subqExpr, contentCol(ctx));
      return e + ' NOT IN\n' + contentPad(ctx) + subq;
    }

    // IN: inline if subquery is short (≤ 2 lines), new line otherwise
    const inner = formatSelect(subqExpr.query, {
      indentOffset: 0,
      riverWidth: deriveSelectRiverWidth(subqExpr.query),
      isSubquery: true,
    });
    const lineCount = inner.split('\n').length;

    if (lineCount <= 2) {
      const prefix = e + ' IN ';
      const parenCol = contentCol(ctx) + prefix.length;
      return prefix + wrapSubqueryLines(inner, parenCol);
    }

    const subq = wrapSubqueryLines(inner, contentCol(ctx));
    return e + ' IN\n' + contentPad(ctx) + subq;
  }

  // EXISTS: always on new line
  if (expr.type === 'exists') {
    const subq = fmtSubqueryAtColumn(expr.subquery, contentCol(ctx));
    return 'EXISTS\n' + contentPad(ctx) + subq;
  }

  // Comparison where right side is a subquery
  if (expr.type === 'binary' && ['=', '<>', '!=', '<', '>', '<=', '>='].includes(expr.operator)) {
    if (expr.right.type === 'subquery') {
      const left = fmtExpr(expr.left);
      const inner = formatSelect(expr.right.query, {
        indentOffset: 0,
        riverWidth: deriveSelectRiverWidth(expr.right.query),
        isSubquery: true,
      });
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

  return fmtExpr(expr);
}

function formatParenLogical(expr: AST.BinaryExpr, opCol: number): string {
  const left = formatParenOperand(expr.left, opCol, expr.operator);
  const right = formatParenOperand(expr.right, opCol, expr.operator);
  return left + '\n' + ' '.repeat(opCol) + expr.operator + ' ' + right;
}

function formatParenOperand(expr: AST.Expr, opCol: number, parentOp: string): string {
  if (expr.type === 'binary' && (expr.operator === 'AND' || expr.operator === 'OR')) {
    return formatParenLogical(expr, opCol);
  }
  if (expr.type === 'paren' && expr.expr.type === 'binary' && (expr.expr.operator === 'AND' || expr.expr.operator === 'OR')) {
    return '(' + formatParenLogical(expr.expr, opCol + parentOp.length + 2) + ')';
  }
  return fmtExpr(expr);
}

// ─── Subquery formatting ─────────────────────────────────────────────

// Format subquery at a given column offset.
// The ( sits at `col` in the final output. SELECT starts at col+1.
// We format the inner query with indentOffset=0 (its own coordinate system),
// then shift subsequent lines by (col+1) to align under SELECT.
function fmtSubqueryAtColumn(expr: AST.SubqueryExpr, col: number): string {
  const inner = formatSelect(expr.query, {
    indentOffset: 0,
    riverWidth: deriveSelectRiverWidth(expr.query),
    isSubquery: true,
    outerColumnOffset: col + 1,
  });
  return wrapSubqueryLines(inner, col);
}

function wrapSubqueryLines(innerFormatted: string, col: number): string {
  const lines = innerFormatted.split('\n');
  const pad = ' '.repeat(col + 1);
  let result = '(' + lines[0];
  for (let i = 1; i < lines.length; i++) {
    result += '\n' + pad + lines[i];
  }
  result += ')';
  return result;
}

// ─── CASE formatting ─────────────────────────────────────────────────

function fmtCaseAtColumn(expr: AST.CaseExpr, col: number): string {
  const pad = ' '.repeat(col);
  let result = 'CASE';
  if (expr.operand) result += ' ' + fmtExpr(expr.operand);
  result += '\n';

  for (const wc of expr.whenClauses) {
    const thenExpr = fmtCaseThenResult(wc.result, col + 'WHEN '.length + fmtExpr(wc.condition).length + ' THEN '.length);
    result += pad + 'WHEN ' + fmtExpr(wc.condition) + ' THEN ' + thenExpr + '\n';
  }

  if (expr.elseResult) {
    result += pad + 'ELSE ' + fmtExpr(expr.elseResult) + '\n';
  }

  result += pad + 'END';
  return result;
}

function fmtCaseThenResult(expr: AST.Expr, col: number): string {
  if (expr.type === 'case') {
    return fmtCaseAtColumn(expr, col);
  }
  return fmtExpr(expr);
}

// ─── Window function formatting ──────────────────────────────────────

function fmtWindowFunctionAtColumn(expr: AST.WindowFunctionExpr, col: number): string {
  const func = fmtFunctionCall(expr.func);
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
      text: 'PARTITION BY ' + expr.partitionBy.map(e => fmtExpr(e)).join(', '),
      byKeywordLen: 12, // 'PARTITION BY'.length
    });
  }

  if (expr.orderBy) {
    overParts.push({
      text: 'ORDER BY ' + expr.orderBy.map(fmtOrderByItem).join(', '),
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
    return func + ' OVER (' + overParts.map(p => p.text).join('') + ')';
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

function formatFrameClause(frame: string, startCol: number, exclude?: string): string {
  const offsetAdjust = /\bBETWEEN\s+(INTERVAL|\d+)/.test(frame) ? 1 : 0;
  // Frame like "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
  // Split at "AND" for alignment
  const parts = frame.split(' AND ');
  if (parts.length === 2) {
    const pad = ' '.repeat(startCol + offsetAdjust + 'ROWS BETWEEN '.length - 'AND '.length);
    // Actually align AND under BETWEEN
    const betweenIdx = parts[0].indexOf('BETWEEN');
    if (betweenIdx >= 0) {
      const betweenPad = ' '.repeat(startCol + offsetAdjust + betweenIdx + 'BETWEEN '.length - 'AND '.length);
      let out = (offsetAdjust ? ' ' : '') + parts[0] + '\n' + betweenPad + 'AND ' + parts[1];
      if (exclude) out += '\n' + ' '.repeat(startCol + offsetAdjust) + 'EXCLUDE ' + exclude;
      return out;
    }
    let out = (offsetAdjust ? ' ' : '') + parts[0] + '\n' + pad + 'AND ' + parts[1];
    if (exclude) out += '\n' + ' '.repeat(startCol + offsetAdjust) + 'EXCLUDE ' + exclude;
    return out;
  }
  return exclude
    ? (offsetAdjust ? ' ' : '') + frame + '\n' + ' '.repeat(startCol + offsetAdjust) + 'EXCLUDE ' + exclude
    : (offsetAdjust ? ' ' : '') + frame;
}

function formatWindowSpec(spec: AST.WindowSpec): string {
  const parts: string[] = [];
  if (spec.partitionBy && spec.partitionBy.length > 0) {
    parts.push('PARTITION BY ' + spec.partitionBy.map(fmtExpr).join(', '));
  }
  if (spec.orderBy && spec.orderBy.length > 0) {
    parts.push('ORDER BY ' + spec.orderBy.map(fmtOrderByItem).join(', '));
  }
  if (spec.frame) {
    const exclude = spec.exclude ? ' EXCLUDE ' + spec.exclude : '';
    parts.push(spec.frame + exclude);
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
  lines.push(header);

  if (node.values) {
    const tuples = node.values.map(vl =>
      '(' + vl.values.map(fmtExpr).join(', ') + ')'
    );
    for (let i = 0; i < tuples.length; i++) {
      const comma = i < tuples.length - 1 ? ',' : '';
      const prefix = i === 0 ? rightAlign('VALUES', dmlCtx) + ' ' : contentPad(dmlCtx);
      lines.push(prefix + tuples[i] + comma);
    }
  } else if (node.selectQuery) {
    const selectStr = formatSelect(node.selectQuery, {
      indentOffset: 0,
      riverWidth: deriveSelectRiverWidth(node.selectQuery),
      isSubquery: true,
    });
    lines.push(selectStr);
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
        const val = item.column + ' = ' + fmtExpr(item.value as AST.Expr);
        const comma = i < node.onConflict.setItems!.length - 1 ? ',' : '';
        if (i === 0) {
          lines.push(rightAlign('SET', conflictCtx) + ' ' + val + comma);
        } else {
          lines.push(contentPad(conflictCtx) + val + comma);
        }
      }
      if (node.onConflict.where) {
        lines.push(rightAlign('WHERE', conflictCtx) + ' ' + formatCondition(node.onConflict.where as AST.Expr, dmlCtx));
      }
    }
  }

  if (node.returning && node.returning.length > 0) {
    lines.push('RETURNING ' + node.returning.map(e => fmtExpr(e as AST.Expr)).join(', ') + ';');
    return lines.join('\n');
  }

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

  // SET right-aligned to river
  const setKw = rightAlign('SET', dmlCtx);
  const setContentCol = contentCol(dmlCtx);

  for (let i = 0; i < node.setItems.length; i++) {
    const item = node.setItems[i];
    const valueCol = i === 0
      ? setKw.length + 1 + item.column.length + 3
      : setContentCol + item.column.length + 3;
    const valExpr = item.value.type === 'subquery'
      ? fmtSubqueryAtColumn(item.value, valueCol)
      : fmtExpr(item.value);
    const val = item.column + ' = ' + valExpr;
    const comma = i < node.setItems.length - 1 ? ',' : '';
    if (i === 0) {
      lines.push(setKw + ' ' + val + comma);
    } else {
      lines.push(' '.repeat(setContentCol) + val + comma);
    }
  }

  if (node.where) {
    const whereKw = rightAlign('WHERE', dmlCtx);
    lines.push(whereKw + ' ' + formatCondition(node.where.condition, dmlCtx));
  }

  if (node.from) {
    const fromKw = rightAlign('FROM', dmlCtx);
    lines.splice(1 + node.setItems.length, 0, fromKw + ' ' + formatFromClause(node.from, dmlCtx));
  }

  if (node.returning && node.returning.length > 0) {
    lines.push('RETURNING ' + node.returning.map(e => fmtExpr(e as AST.Expr)).join(', ') + ';');
    return lines.join('\n');
  }

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
  lines.push(rightAlign('FROM', dmlCtx) + ' ' + node.from);

  if (node.where) {
    const whereKw = rightAlign('WHERE', dmlCtx);
    lines.push(whereKw + ' ' + formatCondition(node.where.condition, dmlCtx));
  }

  if (node.returning && node.returning.length > 0) {
    lines.push('RETURNING ' + node.returning.map(e => fmtExpr(e as AST.Expr)).join(', ') + ';');
    return lines.join('\n');
  }

  return lines.join('\n') + ';';
}

function formatStandaloneValues(node: AST.StandaloneValuesStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  if (node.rows.length === 0) return lines.join('\n') + 'VALUES;';

  const rows = node.rows.map(r => '(' + r.values.map(fmtExpr).join(', ') + ')');
  const contPad = ' '.repeat('VALUES '.length);
  for (let i = 0; i < rows.length; i++) {
    const comma = i < rows.length - 1 ? ',' : ';';
    const prefix = i === 0 ? 'VALUES ' : contPad;
    lines.push(prefix + rows[i] + comma);
  }

  return lines.join('\n');
}

function formatCreateIndex(node: AST.CreateIndexStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  let header = 'CREATE';
  if (node.unique) header += ' UNIQUE';
  header += ' INDEX';
  if (node.concurrently) header += ' CONCURRENTLY';
  if (node.ifNotExists) header += ' IF NOT EXISTS';
  header += ' ' + node.name;
  lines.push(header);

  const cols = node.columns.map(c => fmtExpr(c as AST.Expr)).join(', ');
  if (node.using) {
    lines.push('    ON ' + node.table);
    lines.push(' USING ' + node.using + ' (' + cols + ')');
  } else {
    lines.push('    ON ' + node.table + ' (' + cols + ')');
  }

  if (node.where) {
    lines.push(' WHERE ' + formatCondition(node.where as AST.Expr, {
      ...ctx,
      indentOffset: 0,
      riverWidth: DEFAULT_RIVER,
      isSubquery: false,
    }) + ';');
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

  const raw = node.raw
    .replace(/\s*,\s*/g, ', ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .map(part => {
      const bare = part.replace(/[^A-Za-z_]/g, '');
      const upper = bare.toUpperCase();
      if (upper === 'TABLES') return part.replace(bare, 'TABLES');
      if (bare && isKeyword(bare)) return part.replace(bare, upper);
      return part;
    })
    .join(' ');
  const upper = raw.toUpperCase();

  if (upper.startsWith('GRANT ') && upper.includes(' ON ') && upper.includes(' TO ')) {
    const [grantPart, rest] = raw.split(/\s+ON\s+/i);
    const [onPart, toPart] = rest.split(/\s+TO\s+/i);
    lines.push(grantPart);
    lines.push('   ON ' + onPart);
    lines.push('   TO ' + toPart + ';');
    return lines.join('\n');
  }

  if (upper.startsWith('REVOKE ') && upper.includes(' ON ') && upper.includes(' FROM ')) {
    const [revokePart, rest] = raw.split(/\s+ON\s+/i);
    const [onPart, fromPart] = rest.split(/\s+FROM\s+/i);
    lines.push(revokePart);
    lines.push('  ON ' + onPart);
    lines.push('FROM ' + fromPart + ';');
    return lines.join('\n');
  }

  lines.push(raw + ';');
  return lines.join('\n');
}

function formatTruncate(node: AST.TruncateStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  lines.push('TRUNCATE TABLE ' + node.table);
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
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  const target = node.target.table + (node.target.alias ? ' AS ' + node.target.alias : '');
  const source = node.source.table + (node.source.alias ? ' AS ' + node.source.alias : '');

  lines.push(' MERGE INTO ' + target);
  lines.push('       USING ' + source);
  lines.push('          ON ' + formatJoinOn(node.on as AST.Expr, 9));

  for (const wc of node.whenClauses) {
    const branch = wc.matched ? 'MATCHED' : 'NOT MATCHED';
    const cond = wc.condition ? ' AND ' + fmtExpr(wc.condition as AST.Expr) : '';
    lines.push('  WHEN ' + branch + cond + ' THEN');

    if (wc.action === 'delete') {
      lines.push('       DELETE');
      continue;
    }

    if (wc.action === 'update') {
      lines.push('       UPDATE');
      for (let i = 0; i < (wc.setItems || []).length; i++) {
        const item = wc.setItems![i];
        const comma = i < wc.setItems!.length - 1 ? ',' : '';
        if (i === 0) {
          lines.push('          SET ' + item.column + ' = ' + fmtExpr(item.value as AST.Expr) + comma);
        } else {
          lines.push('              ' + item.column + ' = ' + fmtExpr(item.value as AST.Expr) + comma);
        }
      }
      continue;
    }

    if (wc.action === 'insert') {
      const cols = wc.columns ? ' (' + wc.columns.join(', ') + ')' : '';
      lines.push('       INSERT' + cols);
      lines.push('       VALUES (' + (wc.values || []).map(v => fmtExpr(v as AST.Expr)).join(', ') + ')');
    }
  }

  lines[lines.length - 1] += ';';
  return lines.join('\n');
}

// ─── CREATE TABLE ────────────────────────────────────────────────────

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
  maxTypeLen = Math.min(maxTypeLen, 13);

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
      if (elem.constraints) {
        const isLongestType = typeNorm.length >= maxTypeLen;
        if (maxTypeLen >= 13 && !isLongestType) {
          line += elem.constraints;
        } else {
          line += ' ' + elem.constraints;
        }
      }
      lines.push(line.trimEnd() + comma);
    } else if (elem.elementType === 'constraint') {
      // Indent constraint name to align with type column
      const constraintPad = ' '.repeat(4 + maxNameLen + 1);
      lines.push(constraintPad + 'CONSTRAINT ' + elem.constraintName);
      if (elem.constraintBody) {
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

  const header = 'ALTER TABLE ' + node.tableName;
  const indent = ' '.repeat(header.length - node.action.split(' ')[0].length);
  lines.push(header);
  lines.push(' '.repeat(8) + node.action + ';');

  return lines.join('\n');
}

// ─── DROP TABLE ──────────────────────────────────────────────────────

function formatDropTable(node: AST.DropTableStatement, ctx: FormatContext): string {
  const lines: string[] = [];
  for (const c of node.leadingComments) lines.push(c.text);

  let line = 'DROP TABLE';
  if (node.ifExists) line += ' IF EXISTS';
  line += ' ' + node.tableName + ';';
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
      };
      const inner = formatSelect(member.statement, innerCtx);
      const innerLines = inner.split('\n');
      let str = '(' + innerLines[0];
      for (let j = 1; j < innerLines.length; j++) {
        str += '\n' + ' ' + innerLines[j];
      }
      str += ')';
      if (isLast) {
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

  // Main query
  const mainCtx: FormatContext = {
    indentOffset: 0,
    riverWidth: deriveRiverWidth(node.mainQuery),
    isSubquery: false,
  };
  // Emit leading comments before main query (we handle them here, so clear them
  // from the node to avoid double-emitting in formatSelect/formatUnion)
  if (node.mainQuery.leadingComments && node.mainQuery.leadingComments.length > 0) {
    emitComments(node.mainQuery.leadingComments, lines);
    node.mainQuery.leadingComments = [];
  }
  if (node.mainQuery.type === 'select') {
    lines.push(formatSelect(node.mainQuery, mainCtx));
  } else {
    lines.push(formatUnion(node.mainQuery, mainCtx));
  }

  return lines.join('\n');
}

// Check if a comment is a decorative section header (contains ====, ####, ****, etc.)
function isSectionHeaderComment(comment: AST.CommentNode): boolean {
  // Match comments that consist of decorative characters
  const text = comment.text.replace(/^--\s*/, '');
  return /^[=#+*\-]{4,}$/.test(text.trim());
}

// Emit CTE leading comments, converting single CTE description comments to /* ... */ format
function emitCTELeadingComments(comments: AST.CommentNode[], lines: string[], cteIndentCol: number): void {
  const cteIndent = ' '.repeat(cteIndentCol);

  // Classify: is this a section header block or a simple CTE description?
  // A section header block starts with a decorative line (===, ###, ***, etc.)
  // A simple description is a single comment line without decorative patterns

  // Find groups: section headers vs descriptions
  // A section header is a contiguous group of comments where the first/last lines are decorative
  const hasSectionHeaders = comments.some(c => isSectionHeaderComment(c));

  if (hasSectionHeaders) {
    // Check if the last comment(s) after the section headers are simple description comments
    // Find the last section header comment index
    let lastSectionIdx = -1;
    for (let i = comments.length - 1; i >= 0; i--) {
      if (isSectionHeaderComment(comments[i])) {
        lastSectionIdx = i;
        break;
      }
    }

    // Emit section header comments at column 0
    for (let i = 0; i <= lastSectionIdx; i++) {
      const c = comments[i];
      // Cap blank lines at 1 between CTE definitions (the closing ), already provides line break)
      const blanks = Math.min(c.blankLinesBefore || 0, 1);
      for (let j = 0; j < blanks; j++) {
        lines.push('');
      }
      lines.push(c.text);
    }

    // Check for trailing description comments after the section headers
    const trailingDescComments = comments.slice(lastSectionIdx + 1);
    if (trailingDescComments.length === 1 && !isSectionHeaderComment(trailingDescComments[0])) {
      // Single trailing description comment — convert to /* ... */
      const text = trailingDescComments[0].text.replace(/^--\s*/, '');
      lines.push('');
      lines.push(cteIndent + '/* ' + text + ' */');
    } else if (trailingDescComments.length > 0) {
      // Multiple trailing description comments — emit at column 0
      for (const c of trailingDescComments) {
        const blanks = c.blankLinesBefore || 0;
        for (let j = 0; j < blanks; j++) {
          lines.push('');
        }
        lines.push(c.text);
      }
      if (lines.length > 0 && lines[lines.length - 1] !== '') {
        lines.push('');
      }
    } else {
      // No trailing description comments — just add blank line after section
      if (lines.length > 0 && lines[lines.length - 1] !== '') {
        lines.push('');
      }
    }
  } else {
    // Simple CTE description comments - convert to /* ... */ at CTE indent level
    // Typically this is a single comment line
    if (comments.length === 1) {
      const text = comments[0].text.replace(/^--\s*/, '');
      if (lines.length > 0 && lines[lines.length - 1] !== '') {
        lines.push('');
      }
      lines.push(cteIndent + '/* ' + text + ' */');
    } else {
      // Multiple description comments - emit as line comments at column 0
      for (const c of comments) {
        const blanks = c.blankLinesBefore || 0;
        for (let i = 0; i < blanks; i++) {
          lines.push('');
        }
        lines.push(c.text);
      }
      if (lines.length > 0 && lines[lines.length - 1] !== '') {
        lines.push('');
      }
    }
  }
}

// Emit comments to a lines array, preserving blank lines between comment groups
function emitComments(comments: AST.CommentNode[], lines: string[]): void {
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

  for (const row of node.rows) {
    // Emit leading comments for this row
    if (row.leadingComments) {
      for (const c of row.leadingComments) {
        lines.push(rowIndent + c.text);
      }
    }

    // Skip empty rows (comment-only rows)
    if (row.values.length === 0) continue;

    const vals = '(' + row.values.map(fmtExpr).join(', ') + ')';
    const trailing = row.trailingComment ? '  ' + row.trailingComment.text : '';
    lines.push(rowIndent + vals + trailing);
  }

  return lines.join('\n');
}

// ─── Expression formatting (context-free) ────────────────────────────

function fmtExpr(expr: AST.Expr): string {
  switch (expr.type) {
    case 'identifier':
      return expr.quoted ? expr.value : lowerIdent(expr.value);
    case 'literal':
      if (expr.literalType === 'boolean') return expr.value.toUpperCase();
      return expr.value;
    case 'star':
      return expr.qualifier ? expr.qualifier + '.*' : '*';
    case 'binary':
      return fmtExpr(expr.left) + ' ' + expr.operator + ' ' + fmtExpr(expr.right);
    case 'unary':
      // No space for unary minus (e.g., -1), space for NOT
      if (expr.operator === '-' || expr.operator === '~') return expr.operator + fmtExpr(expr.operand);
      return expr.operator + ' ' + fmtExpr(expr.operand);
    case 'function_call':
      return fmtFunctionCall(expr);
    case 'subquery':
      return fmtSubquerySimple(expr);
    case 'case':
      return fmtCaseSimple(expr);
    case 'between': {
      const neg = expr.negated ? 'NOT ' : '';
      return fmtExpr(expr.expr) + ' ' + neg + 'BETWEEN ' + fmtExpr(expr.low) + ' AND ' + fmtExpr(expr.high);
    }
    case 'in': {
      const neg = expr.negated ? 'NOT ' : '';
      if ('type' in expr.values && (expr.values as any).type === 'subquery') {
        return fmtExpr(expr.expr) + ' ' + neg + 'IN ' + fmtSubquerySimple(expr.values as AST.SubqueryExpr);
      }
      const vals = (expr.values as AST.Expr[]).map(fmtExpr).join(', ');
      return fmtExpr(expr.expr) + ' ' + neg + 'IN (' + vals + ')';
    }
    case 'is':
      return fmtExpr(expr.expr) + ' IS ' + expr.value;
    case 'like': {
      const neg = expr.negated ? 'NOT ' : '';
      return fmtExpr(expr.expr) + ' ' + neg + 'LIKE ' + fmtExpr(expr.pattern);
    }
    case 'exists':
      return 'EXISTS ' + fmtSubquerySimple(expr.subquery);
    case 'paren':
      return '(' + fmtExpr(expr.expr) + ')';
    case 'cast':
      return 'CAST(' + fmtExpr(expr.expr) + ' AS ' + expr.targetType + ')';
    case 'window_function':
      return fmtWindowFunctionSimple(expr);
    case 'extract':
      return 'EXTRACT(' + expr.field + ' FROM ' + fmtExpr(expr.source) + ')';
    case 'raw':
      return expr.text;
    // New expression types
    case 'pg_cast':
      return fmtExpr(expr.expr as AST.Expr) + '::' + expr.targetType;
    case 'ilike': {
      const neg = expr.negated ? 'NOT ' : '';
      return fmtExpr(expr.expr) + ' ' + neg + 'ILIKE ' + fmtExpr(expr.pattern);
    }
    case 'similar_to': {
      const neg = expr.negated ? 'NOT ' : '';
      return fmtExpr(expr.expr) + ' ' + neg + 'SIMILAR TO ' + fmtExpr(expr.pattern);
    }
    case 'array_constructor':
      return 'ARRAY[' + expr.elements.map(e => fmtExpr(e as AST.Expr)).join(', ') + ']';
    case 'is_distinct_from': {
      const kw = expr.negated ? 'IS NOT DISTINCT FROM' : 'IS DISTINCT FROM';
      return fmtExpr(expr.left as AST.Expr) + ' ' + kw + ' ' + fmtExpr(expr.right as AST.Expr);
    }
    case 'regex_match':
      return fmtExpr(expr.left as AST.Expr) + ' ' + expr.operator + ' ' + fmtExpr(expr.right as AST.Expr);
  }
}

function fmtFunctionCall(expr: AST.FunctionCallExpr): string {
  const name = expr.name.toUpperCase();
  const distinct = expr.distinct ? 'DISTINCT ' : '';
  const args = expr.args.map(fmtExpr).join(', ');
  let body = distinct + args;
  if (expr.orderBy && expr.orderBy.length > 0) {
    body += ' ORDER BY ' + expr.orderBy.map(fmtOrderByItem).join(', ');
  }

  let out = name + '(' + body + ')';
  if (expr.withinGroup) {
    out += ' WITHIN GROUP (ORDER BY ' + expr.withinGroup.orderBy.map(fmtOrderByItem).join(', ') + ')';
  }
  if (expr.filter) {
    out += ' FILTER (WHERE ' + fmtExpr(expr.filter as AST.Expr) + ')';
  }
  return out;
}

function fmtSubquerySimple(expr: AST.SubqueryExpr): string {
  const inner = formatSelect(expr.query, {
    indentOffset: 0,
    riverWidth: deriveSelectRiverWidth(expr.query),
    isSubquery: true,
  });
  return '(' + inner + ')';
}

function fmtCaseSimple(expr: AST.CaseExpr): string {
  let s = 'CASE';
  if (expr.operand) s += ' ' + fmtExpr(expr.operand);
  for (const wc of expr.whenClauses) {
    s += ' WHEN ' + fmtExpr(wc.condition) + ' THEN ' + fmtExpr(wc.result);
  }
  if (expr.elseResult) s += ' ELSE ' + fmtExpr(expr.elseResult);
  s += ' END';
  return s;
}

function fmtWindowFunctionSimple(expr: AST.WindowFunctionExpr): string {
  const func = fmtFunctionCall(expr.func);
  if (expr.windowName) return func + ' OVER ' + expr.windowName;
  let over = '';
  if (expr.partitionBy) over += 'PARTITION BY ' + expr.partitionBy.map(fmtExpr).join(', ');
  if (expr.orderBy) {
    if (over) over += ' ';
    over += 'ORDER BY ' + expr.orderBy.map(fmtOrderByItem).join(', ');
  }
  if (expr.frame) {
    if (over) over += ' ';
    over += expr.frame;
  }
  if (expr.exclude) {
    if (over) over += ' ';
    over += 'EXCLUDE ' + expr.exclude;
  }
  return func + ' OVER (' + over + ')';
}

function fmtOrderByItem(item: AST.OrderByItem): string {
  let s = fmtExpr(item.expr);
  if (item.direction) s += ' ' + item.direction;
  return s;
}

// Check if an alias is redundant (matches the last component of the expression)
function isRedundantAlias(expr: AST.Expr, alias: string): boolean {
  if (expr.type === 'identifier') {
    // For "a.b", the last component is "b"
    const parts = expr.value.split('.');
    const lastPart = parts[parts.length - 1].toLowerCase();
    return lastPart === alias.toLowerCase();
  }
  return false;
}

function fmtAlias(alias: string): string {
  if (alias.startsWith('"')) return alias;
  return alias.toLowerCase();
}

// Lowercase identifiers, preserving qualified name dots and quoted identifiers
function lowerIdent(name: string): string {
  return name.split('.').map(p => {
    if (p.startsWith('"')) return p;
    return p.toLowerCase();
  }).join('.');
}
