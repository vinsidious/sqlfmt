// AST node types for the SQL formatter

export type Statement =
  | SelectStatement
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | CreateTableStatement
  | AlterTableStatement
  | DropTableStatement
  | UnionStatement
  | CTEStatement
  | MergeStatement
  | CreateIndexStatement
  | CreateViewStatement
  | GrantStatement
  | TruncateStatement
  | StandaloneValuesStatement;

export type QueryExpression = SelectStatement | UnionStatement | CTEStatement;

export type Node =
  | Statement
  | ValuesClause
  | RawExpression
  | CommentNode;

export type Expression =
  | IdentifierExpr
  | LiteralExpr
  | StarExpr
  | BinaryExpr
  | UnaryExpr
  | FunctionCallExpr
  | SubqueryExpr
  | CaseExpr
  | BetweenExpr
  | InExpr
  | IsExpr
  | LikeExpr
  | IlikeExpr
  | SimilarToExpr
  | ExistsExpr
  | ParenExpr
  | CastExpr
  | PgCastExpr
  | WindowFunctionExpr
  | ExtractExpr
  | ArrayConstructorExpr
  | IsDistinctFromExpr
  | RegexExpr
  | RawExpression;

// Keep Expr as an alias for backward compatibility
export type Expr = Expression;

export interface SelectStatement {
  type: 'select';
  distinct: boolean;
  columns: ColumnExpr[];
  from?: FromClause;
  additionalFromItems?: FromClause[];
  joins: JoinClause[];
  where?: WhereClause;
  groupBy?: GroupByClause;
  having?: HavingClause;
  orderBy?: OrderByClause;
  limit?: LimitClause;
  offset?: OffsetClause;
  fetch?: { count: Expression; withTies?: boolean };
  windowClause?: { name: string; spec: WindowSpec }[];
  leadingComments: CommentNode[];
  parenthesized?: boolean;
}

export interface InsertStatement {
  type: 'insert';
  table: string;
  columns: string[];
  values?: ValuesList[];
  selectQuery?: SelectStatement;
  returning?: Expression[];
  onConflict?: {
    columns?: string[];
    constraintName?: string;
    action: 'nothing' | 'update';
    setItems?: { column: string; value: Expression }[];
    where?: Expression;
  };
  leadingComments: CommentNode[];
}

export interface UpdateStatement {
  type: 'update';
  table: string;
  setItems: SetItem[];
  from?: FromClause;
  where?: WhereClause;
  returning?: Expression[];
  leadingComments: CommentNode[];
}

export interface DeleteStatement {
  type: 'delete';
  from: string;
  where?: WhereClause;
  returning?: Expression[];
  leadingComments: CommentNode[];
}

export interface CreateTableStatement {
  type: 'create_table';
  tableName: string;
  elements: TableElement[];
  leadingComments: CommentNode[];
}

export interface AlterTableStatement {
  type: 'alter_table';
  tableName: string;
  action: string; // raw text like "ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT ''"
  leadingComments: CommentNode[];
}

export interface DropTableStatement {
  type: 'drop_table';
  ifExists: boolean;
  tableName: string;
  leadingComments: CommentNode[];
}

export interface UnionStatement {
  type: 'union';
  members: { statement: SelectStatement; parenthesized: boolean }[];
  operators: string[]; // 'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT'
  leadingComments: CommentNode[];
}

export interface CTEStatement {
  type: 'cte';
  recursive?: boolean;
  ctes: CTEDefinition[];
  mainQuery: SelectStatement | UnionStatement;
  leadingComments: CommentNode[];
}

export interface CTEDefinition {
  name: string;
  columnList?: string[];   // e.g., (revenue_date, amount)
  materialized?: 'materialized' | 'not_materialized';
  query: SelectStatement | UnionStatement | ValuesClause;
  leadingComments?: CommentNode[];  // comments before this CTE definition
}

export interface MergeStatement {
  type: 'merge';
  target: { table: string; alias?: string };
  source: { table: string; alias?: string };
  on: Expression;
  whenClauses: MergeWhenClause[];
  leadingComments: CommentNode[];
}

export interface MergeWhenClause {
  matched: boolean;
  condition?: Expression;
  action: 'delete' | 'update' | 'insert';
  setItems?: { column: string; value: Expression }[];
  columns?: string[];
  values?: Expression[];
}

export interface CreateIndexStatement {
  type: 'create_index';
  unique?: boolean;
  concurrently?: boolean;
  ifNotExists?: boolean;
  name: string;
  table: string;
  using?: string;
  columns: Expression[];
  where?: Expression;
  leadingComments: CommentNode[];
}

export interface CreateViewStatement {
  type: 'create_view';
  orReplace?: boolean;
  materialized?: boolean;
  ifNotExists?: boolean;
  name: string;
  query: Statement;
  withData?: boolean;
  leadingComments: CommentNode[];
}

export interface GrantStatement {
  type: 'grant';
  raw: string;
  leadingComments: CommentNode[];
}

export interface TruncateStatement {
  type: 'truncate';
  table: string;
  restartIdentity?: boolean;
  cascade?: boolean;
  leadingComments: CommentNode[];
}

export interface StandaloneValuesStatement {
  type: 'standalone_values';
  rows: ValuesRow[];
  leadingComments: CommentNode[];
}

export interface ValuesClause {
  type: 'values';
  rows: ValuesRow[];
  leadingComments: CommentNode[];
}

export interface ValuesRow {
  values: Expr[];
  trailingComment?: CommentNode;
  leadingComments?: CommentNode[];
}

export interface CommentNode {
  type: 'comment';
  style: 'line' | 'block';
  text: string;
  blankLinesBefore?: number;  // number of blank lines preceding this comment (0 = none)
}

// Expressions

export interface IdentifierExpr {
  type: 'identifier';
  value: string;
  // lowercase the identifier? Only for non-quoted identifiers
  quoted: boolean;
}

export interface LiteralExpr {
  type: 'literal';
  value: string;
  literalType: 'string' | 'number' | 'boolean';
}

export interface StarExpr {
  type: 'star';
  qualifier?: string; // e.g., "t" in "t.*"
}

export interface BinaryExpr {
  type: 'binary';
  left: Expr;
  operator: string;
  right: Expr;
}

export interface UnaryExpr {
  type: 'unary';
  operator: string;
  operand: Expr;
}

export interface FunctionCallExpr {
  type: 'function_call';
  name: string;
  args: Expr[];
  distinct?: boolean;
  orderBy?: OrderByItem[];
  filter?: Expression;
  withinGroup?: { orderBy: OrderByItem[] };
}

export interface SubqueryExpr {
  type: 'subquery';
  query: QueryExpression;
}

export interface CaseExpr {
  type: 'case';
  operand?: Expr;  // for simple CASE
  whenClauses: { condition: Expr; result: Expr }[];
  elseResult?: Expr;
}

export interface BetweenExpr {
  type: 'between';
  expr: Expr;
  low: Expr;
  high: Expr;
  negated: boolean;
}

export interface InExpr {
  type: 'in';
  expr: Expr;
  values: Expr[] | SubqueryExpr;
  negated: boolean;
}

export interface IsExpr {
  type: 'is';
  expr: Expr;
  value: 'NULL' | 'NOT NULL' | 'TRUE' | 'FALSE' | 'NOT TRUE' | 'NOT FALSE';
}

export interface LikeExpr {
  type: 'like';
  expr: Expr;
  pattern: Expr;
  negated: boolean;
}

export interface IlikeExpr {
  type: 'ilike';
  expr: Expr;
  pattern: Expr;
  negated: boolean;
}

export interface SimilarToExpr {
  type: 'similar_to';
  expr: Expr;
  pattern: Expr;
  negated: boolean;
}

export interface ExistsExpr {
  type: 'exists';
  subquery: SubqueryExpr;
}

export interface ParenExpr {
  type: 'paren';
  expr: Expr;
}

export interface CastExpr {
  type: 'cast';
  expr: Expr;
  targetType: string;
}

export interface PgCastExpr {
  type: 'pg_cast';
  expr: Expression;
  targetType: string;
}

export interface WindowSpec {
  partitionBy?: Expr[];
  orderBy?: OrderByItem[];
  frame?: string;
  exclude?: string;
}

export interface WindowFunctionExpr {
  type: 'window_function';
  func: FunctionCallExpr;
  partitionBy?: Expr[];
  orderBy?: OrderByItem[];
  frame?: string; // raw frame clause like "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
  exclude?: string;
  windowName?: string;
}

export interface ExtractExpr {
  type: 'extract';
  field: string;   // DAY, MONTH, YEAR, etc.
  source: Expr;    // the expression to extract from
}

export interface ArrayConstructorExpr {
  type: 'array_constructor';
  elements: Expression[];
}

export interface IsDistinctFromExpr {
  type: 'is_distinct_from';
  left: Expression;
  right: Expression;
  negated: boolean;
}

export interface RegexExpr {
  type: 'regex_match';
  left: Expression;
  operator: string;
  right: Expression;
}

export interface RawExpression {
  type: 'raw';
  text: string;
}

// Column expression with optional alias and comment
export interface ColumnExpr {
  expr: Expr;
  alias?: string;
  trailingComment?: CommentNode;
}

export interface FromClause {
  table: Expr;
  alias?: string;
  aliasColumns?: string[];
  lateral?: boolean;
  tablesample?: { method: string; args: Expression[]; repeatable?: Expression };
}

export interface JoinClause {
  joinType: string; // 'INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', etc.
  table: Expr;
  alias?: string;
  aliasColumns?: string[];
  lateral?: boolean;
  on?: Expr;
  usingClause?: string[];
}

export interface WhereClause {
  condition: Expr;
  trailingComment?: CommentNode;
}

export interface GroupByClause {
  items: Expr[];
  groupingSets?: { type: 'grouping_sets' | 'rollup' | 'cube'; sets: Expression[][] }[];
}

export interface HavingClause {
  condition: Expr;
}

export interface OrderByClause {
  items: OrderByItem[];
}

export interface OrderByItem {
  expr: Expr;
  direction?: 'ASC' | 'DESC';
}

export interface LimitClause {
  count: Expr;
}

export interface OffsetClause {
  count: Expr;
  rowsKeyword?: boolean;
}

export interface SetItem {
  column: string;
  value: Expr;
}

export interface ValuesList {
  values: Expr[];
}

export interface TableElement {
  elementType: 'column' | 'primary_key' | 'constraint' | 'foreign_key';
  raw: string; // We'll store structured data for formatting
  name?: string;
  dataType?: string;
  constraints?: string;
  constraintName?: string;
  constraintBody?: string;
  fkColumns?: string;
  fkRefTable?: string;
  fkRefColumns?: string;
  fkActions?: string;
}
