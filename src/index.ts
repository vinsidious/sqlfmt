export { formatSQL } from './format';
export type { FormatOptions } from './format';
export { tokenize, TokenizeError } from './tokenizer';
export type { Token, TokenType } from './tokenizer';
export { Parser, parse, ParseError, MaxDepthError } from './parser';
export type { ParseOptions, ParseRecoveryContext } from './parser';
export { formatStatements } from './formatter';
export type { FormatterOptions } from './formatter';
export type {
  Statement,
  Node,
  Expression,
  QueryExpression,
  SelectStatement,
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  MergeStatement,
  CTEStatement,
  CTESearchClause,
  CTECycleClause,
  UnionStatement,
  ExplainStatement,
  CommentNode,
  ColumnExpr,
  JoinClause,
  FromClause,
  RawExpression,
  RawReason,
  FrameSpec,
  FrameBound,
  ColumnConstraint,
  CreateTableStatement,
  CreateIndexStatement,
  CreateViewStatement,
  AlterTableStatement,
  DropTableStatement,
  GrantStatement,
  TruncateStatement,
} from './ast';

// Injected at build time by tsup's `define` option from package.json.
declare const __SQLFMT_VERSION__: string | undefined;
export const version: string =
  typeof __SQLFMT_VERSION__ !== 'undefined'
    ? __SQLFMT_VERSION__
    : '0.0.0-dev';
