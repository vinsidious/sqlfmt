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
  | NullLiteralExpr
  | IntervalExpr
  | TypedStringExpr
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
  | PositionExpr
  | SubstringExpr
  | OverlayExpr
  | TrimExpr
  | ArrayConstructorExpr
  | IsDistinctFromExpr
  | RegexExpr
  | AliasedExpr
  | ArraySubscriptExpr
  | OrderedExpr
  | RawExpression;

export interface SelectStatement {
  readonly type: 'select';
  readonly distinct: boolean;
  readonly columns: readonly ColumnExpr[];
  readonly from?: FromClause;
  readonly additionalFromItems?: readonly FromClause[];
  readonly joins: readonly JoinClause[];
  readonly where?: WhereClause;
  readonly groupBy?: GroupByClause;
  readonly having?: HavingClause;
  readonly orderBy?: OrderByClause;
  readonly limit?: LimitClause;
  readonly offset?: OffsetClause;
  readonly fetch?: { readonly count: Expression; readonly withTies?: boolean };
  readonly lockingClause?: string;
  readonly windowClause?: readonly { readonly name: string; readonly spec: WindowSpec }[];
  readonly leadingComments: readonly CommentNode[];
  readonly parenthesized?: boolean;
}

export interface InsertStatement {
  readonly type: 'insert';
  readonly table: string;
  readonly columns: readonly string[];
  readonly defaultValues?: boolean;
  readonly values?: readonly ValuesList[];
  readonly selectQuery?: QueryExpression;
  readonly returning?: readonly Expression[];
  readonly onConflict?: {
    readonly columns?: readonly string[];
    readonly constraintName?: string;
    readonly action: 'nothing' | 'update';
    readonly setItems?: readonly { readonly column: string; readonly value: Expression }[];
    readonly where?: Expression;
  };
  readonly leadingComments: readonly CommentNode[];
}

export interface UpdateStatement {
  readonly type: 'update';
  readonly table: string;
  readonly setItems: readonly SetItem[];
  readonly from?: FromClause;
  readonly where?: WhereClause;
  readonly returning?: readonly Expression[];
  readonly leadingComments: readonly CommentNode[];
}

export interface DeleteStatement {
  readonly type: 'delete';
  readonly from: string;
  readonly using?: readonly FromClause[];
  readonly where?: WhereClause;
  readonly returning?: readonly Expression[];
  readonly leadingComments: readonly CommentNode[];
}

export interface CreateTableStatement {
  readonly type: 'create_table';
  readonly tableName: string;
  readonly elements: readonly TableElement[];
  readonly leadingComments: readonly CommentNode[];
}

export interface AlterTableStatement {
  readonly type: 'alter_table';
  readonly objectType: string;
  readonly objectName: string;
  readonly actions: readonly AlterAction[];
  readonly leadingComments: readonly CommentNode[];
}

export type AlterAction =
  | AlterAddColumnAction
  | AlterDropColumnAction
  | AlterRenameToAction
  | AlterRenameColumnAction
  | AlterSetSchemaAction
  | AlterSetTablespaceAction
  | AlterRawAction;

export interface AlterAddColumnAction {
  readonly type: 'add_column';
  readonly ifNotExists?: boolean;
  readonly columnName: string;
  readonly definition?: string;
}

export interface AlterDropColumnAction {
  readonly type: 'drop_column';
  readonly ifExists?: boolean;
  readonly columnName: string;
  readonly behavior?: 'CASCADE' | 'RESTRICT';
}

export interface AlterRenameToAction {
  readonly type: 'rename_to';
  readonly newName: string;
}

export interface AlterRenameColumnAction {
  readonly type: 'rename_column';
  readonly columnName: string;
  readonly newName: string;
}

export interface AlterSetSchemaAction {
  readonly type: 'set_schema';
  readonly schema: string;
}

export interface AlterSetTablespaceAction {
  readonly type: 'set_tablespace';
  readonly tablespace: string;
}

export interface AlterRawAction {
  readonly type: 'raw';
  readonly text: string;
}

export interface DropTableStatement {
  readonly type: 'drop_table';
  readonly objectType: string;
  readonly ifExists: boolean;
  readonly objectName: string;
  readonly leadingComments: readonly CommentNode[];
}

export interface UnionStatement {
  readonly type: 'union';
  readonly members: readonly { readonly statement: SelectStatement; readonly parenthesized: boolean }[];
  readonly operators: readonly string[]; // 'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT'
  readonly leadingComments: readonly CommentNode[];
}

export interface CTEStatement {
  readonly type: 'cte';
  readonly recursive?: boolean;
  readonly ctes: readonly CTEDefinition[];
  readonly mainQuery: SelectStatement | UnionStatement;
  readonly leadingComments: readonly CommentNode[];
}

export interface CTEDefinition {
  readonly name: string;
  readonly columnList?: readonly string[];   // e.g., (revenue_date, amount)
  readonly materialized?: 'materialized' | 'not_materialized';
  readonly query: SelectStatement | UnionStatement | ValuesClause;
  readonly leadingComments?: readonly CommentNode[];  // comments before this CTE definition
}

export interface MergeStatement {
  readonly type: 'merge';
  readonly target: { readonly table: string; readonly alias?: string };
  readonly source: { readonly table: string; readonly alias?: string };
  readonly on: Expression;
  readonly whenClauses: readonly MergeWhenClause[];
  readonly leadingComments: readonly CommentNode[];
}

export interface MergeWhenClause {
  readonly matched: boolean;
  readonly condition?: Expression;
  readonly action: 'delete' | 'update' | 'insert';
  readonly setItems?: readonly { readonly column: string; readonly value: Expression }[];
  readonly columns?: readonly string[];
  readonly values?: readonly Expression[];
}

export interface CreateIndexStatement {
  readonly type: 'create_index';
  readonly unique?: boolean;
  readonly concurrently?: boolean;
  readonly ifNotExists?: boolean;
  readonly name: string;
  readonly table: string;
  readonly using?: string;
  readonly columns: readonly Expression[];
  readonly where?: Expression;
  readonly leadingComments: readonly CommentNode[];
}

export interface CreateViewStatement {
  readonly type: 'create_view';
  readonly orReplace?: boolean;
  readonly materialized?: boolean;
  readonly ifNotExists?: boolean;
  readonly name: string;
  readonly query: Statement;
  readonly withData?: boolean;
  readonly leadingComments: readonly CommentNode[];
}

export interface GrantStatement {
  readonly type: 'grant';
  readonly kind: 'GRANT' | 'REVOKE';
  readonly grantOptionFor?: boolean;
  readonly privileges: readonly string[];
  readonly object: string;
  readonly recipientKeyword: 'TO' | 'FROM';
  readonly recipients: readonly string[];
  readonly withGrantOption?: boolean;
  readonly grantedBy?: string;
  readonly cascade?: boolean;
  readonly restrict?: boolean;
  readonly leadingComments: readonly CommentNode[];
}

export interface TruncateStatement {
  readonly type: 'truncate';
  readonly table: string;
  readonly tableKeyword?: boolean;
  readonly restartIdentity?: boolean;
  readonly cascade?: boolean;
  readonly leadingComments: readonly CommentNode[];
}

export interface StandaloneValuesStatement {
  readonly type: 'standalone_values';
  readonly rows: readonly ValuesRow[];
  readonly leadingComments: readonly CommentNode[];
}

export interface ValuesClause {
  readonly type: 'values';
  readonly rows: readonly ValuesRow[];
  readonly leadingComments: readonly CommentNode[];
}

export interface ValuesRow {
  readonly values: readonly Expression[];
  readonly trailingComment?: CommentNode;
  readonly leadingComments?: readonly CommentNode[];
}

export interface CommentNode {
  readonly type: 'comment';
  readonly style: 'line' | 'block';
  readonly text: string;
  readonly blankLinesBefore?: number;  // number of blank lines preceding this comment (0 = none)
}

// Expressions

export interface IdentifierExpr {
  readonly type: 'identifier';
  readonly value: string;
  readonly quoted: boolean;
}

export interface LiteralExpr {
  readonly type: 'literal';
  readonly value: string;
  readonly literalType: 'string' | 'number' | 'boolean';
}

export interface NullLiteralExpr {
  readonly type: 'null';
}

export interface IntervalExpr {
  readonly type: 'interval';
  readonly value: string;
}

export interface TypedStringExpr {
  readonly type: 'typed_string';
  readonly dataType: 'DATE' | 'TIME' | 'TIMESTAMP';
  readonly value: string;
}

export interface StarExpr {
  readonly type: 'star';
  readonly qualifier?: string; // e.g., "t" in "t.*"
}

export interface BinaryExpr {
  readonly type: 'binary';
  readonly left: Expression;
  readonly operator: string;
  readonly right: Expression;
}

export interface UnaryExpr {
  readonly type: 'unary';
  readonly operator: string;
  readonly operand: Expression;
}

export interface FunctionCallExpr {
  readonly type: 'function_call';
  readonly name: string;
  readonly args: readonly Expression[];
  readonly distinct?: boolean;
  readonly orderBy?: readonly OrderByItem[];
  readonly filter?: Expression;
  readonly withinGroup?: { readonly orderBy: readonly OrderByItem[] };
}

export interface SubqueryExpr {
  readonly type: 'subquery';
  readonly query: QueryExpression;
}

export interface CaseExpr {
  readonly type: 'case';
  readonly operand?: Expression;  // for simple CASE
  readonly whenClauses: readonly { readonly condition: Expression; readonly result: Expression }[];
  readonly elseResult?: Expression;
}

export interface BetweenExpr {
  readonly type: 'between';
  readonly expr: Expression;
  readonly low: Expression;
  readonly high: Expression;
  readonly negated: boolean;
}

// InExpr discriminated union: list vs subquery
export type InExpr = InExprList | InExprSubquery;

export interface InExprList {
  readonly type: 'in';
  readonly expr: Expression;
  readonly values: readonly Expression[];
  readonly negated: boolean;
  readonly subquery: false;
}

export interface InExprSubquery {
  readonly type: 'in';
  readonly expr: Expression;
  readonly values: SubqueryExpr;
  readonly negated: boolean;
  readonly subquery: true;
}

export interface IsExpr {
  readonly type: 'is';
  readonly expr: Expression;
  readonly value: 'NULL' | 'NOT NULL' | 'TRUE' | 'FALSE' | 'NOT TRUE' | 'NOT FALSE';
}

export interface LikeExpr {
  readonly type: 'like';
  readonly expr: Expression;
  readonly pattern: Expression;
  readonly negated: boolean;
  readonly escape?: Expression;
}

export interface IlikeExpr {
  readonly type: 'ilike';
  readonly expr: Expression;
  readonly pattern: Expression;
  readonly negated: boolean;
  readonly escape?: Expression;
}

export interface SimilarToExpr {
  readonly type: 'similar_to';
  readonly expr: Expression;
  readonly pattern: Expression;
  readonly negated: boolean;
}

export interface ExistsExpr {
  readonly type: 'exists';
  readonly subquery: SubqueryExpr;
}

export interface ParenExpr {
  readonly type: 'paren';
  readonly expr: Expression;
}

export interface CastExpr {
  readonly type: 'cast';
  readonly expr: Expression;
  readonly targetType: string;
}

export interface PgCastExpr {
  readonly type: 'pg_cast';
  readonly expr: Expression;
  readonly targetType: string;
}

export interface WindowSpec {
  readonly partitionBy?: readonly Expression[];
  readonly orderBy?: readonly OrderByItem[];
  readonly frame?: string;
  readonly exclude?: string;
}

export interface WindowFunctionExpr {
  readonly type: 'window_function';
  readonly func: FunctionCallExpr;
  readonly partitionBy?: readonly Expression[];
  readonly orderBy?: readonly OrderByItem[];
  readonly frame?: string; // raw frame clause like "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
  readonly exclude?: string;
  readonly windowName?: string;
}

export interface ExtractExpr {
  readonly type: 'extract';
  readonly field: string;   // DAY, MONTH, YEAR, etc.
  readonly source: Expression;    // the expression to extract from
}

export interface PositionExpr {
  readonly type: 'position';
  readonly substring: Expression;
  readonly source: Expression;
}

export interface SubstringExpr {
  readonly type: 'substring';
  readonly source: Expression;
  readonly start: Expression;
  readonly length?: Expression;
}

export interface OverlayExpr {
  readonly type: 'overlay';
  readonly source: Expression;
  readonly replacement: Expression;
  readonly start: Expression;
  readonly length?: Expression;
}

export interface TrimExpr {
  readonly type: 'trim';
  readonly side?: 'LEADING' | 'TRAILING' | 'BOTH';
  readonly trimChar?: Expression;
  readonly source: Expression;
  readonly fromSyntax: boolean;
}

export interface ArrayConstructorExpr {
  readonly type: 'array_constructor';
  readonly elements: readonly Expression[];
}

export interface IsDistinctFromExpr {
  readonly type: 'is_distinct_from';
  readonly left: Expression;
  readonly right: Expression;
  readonly negated: boolean;
}

export interface RegexExpr {
  readonly type: 'regex_match';
  readonly left: Expression;
  readonly operator: string;
  readonly right: Expression;
}

export interface AliasedExpr {
  readonly type: 'aliased';
  readonly expr: Expression;
  readonly alias: string;
}

export interface ArraySubscriptExpr {
  readonly type: 'array_subscript';
  readonly array: Expression;
  readonly isSlice: boolean;
  readonly lower?: Expression;
  readonly upper?: Expression;
}

export interface OrderedExpr {
  readonly type: 'ordered_expr';
  readonly expr: Expression;
  readonly direction: 'ASC' | 'DESC';
}

export interface RawExpression {
  readonly type: 'raw';
  readonly text: string;
}

// Column expression with optional alias and comment
export interface ColumnExpr {
  readonly expr: Expression;
  readonly alias?: string;
  readonly trailingComment?: CommentNode;
}

export interface FromClause {
  readonly table: Expression;
  readonly alias?: string;
  readonly aliasColumns?: readonly string[];
  readonly lateral?: boolean;
  readonly tablesample?: { readonly method: string; readonly args: readonly Expression[]; readonly repeatable?: Expression };
}

export interface JoinClause {
  readonly joinType: string; // 'INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', etc.
  readonly table: Expression;
  readonly alias?: string;
  readonly aliasColumns?: readonly string[];
  readonly lateral?: boolean;
  readonly on?: Expression;
  readonly usingClause?: readonly string[];
  readonly trailingComment?: CommentNode;
}

export interface WhereClause {
  readonly condition: Expression;
  readonly trailingComment?: CommentNode;
}

export interface GroupByClause {
  readonly items: readonly Expression[];
  readonly groupingSets?: readonly { readonly type: 'grouping_sets' | 'rollup' | 'cube'; readonly sets: readonly (readonly Expression[])[] }[];
}

export interface HavingClause {
  readonly condition: Expression;
}

export interface OrderByClause {
  readonly items: readonly OrderByItem[];
}

export interface OrderByItem {
  readonly expr: Expression;
  readonly direction?: 'ASC' | 'DESC';
  readonly nulls?: 'FIRST' | 'LAST';
  readonly trailingComment?: CommentNode;
}

export interface LimitClause {
  readonly count: Expression;
}

export interface OffsetClause {
  readonly count: Expression;
  readonly rowsKeyword?: boolean;
}

export interface SetItem {
  readonly column: string;
  readonly value: Expression;
}

export interface ValuesList {
  readonly values: readonly Expression[];
}

export interface TableElement {
  readonly elementType: 'column' | 'primary_key' | 'constraint' | 'foreign_key';
  readonly raw: string;
  readonly name?: string;
  readonly dataType?: string;
  readonly constraints?: string;
  readonly constraintName?: string;
  readonly constraintBody?: string;
  readonly constraintType?: 'check' | 'raw';
  readonly checkExpr?: Expression;
  readonly fkColumns?: string;
  readonly fkRefTable?: string;
  readonly fkRefColumns?: string;
  readonly fkActions?: string;
}
