// AST node types for the SQL formatter.

/** Top-level SQL statements that the parser can produce as structured nodes. */
export type Statement =
  | SelectStatement
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | ExplainStatement
  | CreateTableStatement
  | AlterTableStatement
  | DropTableStatement
  | UnionStatement
  | CTEStatement
  | MergeStatement
  | CreateIndexStatement
  | CreateViewStatement
  | CreatePolicyStatement
  | GrantStatement
  | TruncateStatement
  | StandaloneValuesStatement;

/** Query-producing statements that are valid in subquery positions. */
export type QueryExpression = SelectStatement | UnionStatement | CTEStatement | ValuesClause;

/** Statements that can legally appear as the primary statement after a WITH clause. */
export type CTEMainStatement =
  | SelectStatement
  | UnionStatement
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | MergeStatement;

/** Any top-level node emitted by `parse()`. */
export type Node =
  | Statement
  | ValuesClause
  | RawExpression
  | CommentNode;

/** Any expression node used in SELECT/DML/DDL contexts. */
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
  | QuantifiedComparisonExpr
  | InExpr
  | IsExpr
  | LikeExpr
  | IlikeExpr
  | SimilarToExpr
  | ExistsExpr
  | ParenExpr
  | TupleExpr
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
  | CollateExpr
  | AliasedExpr
  | ArraySubscriptExpr
  | OrderedExpr
  | RawExpression;

export interface SelectStatement {
  readonly type: 'select';
  readonly distinct: boolean;
  readonly distinctOn?: readonly Expression[];
  readonly top?: string;
  readonly into?: string;
  readonly columns: readonly ColumnExpr[];
  readonly from?: FromClause;
  readonly additionalFromItems?: readonly FromClause[];
  readonly joins: readonly JoinClause[];
  readonly where?: WhereClause;
  readonly startWith?: Expression;
  readonly connectBy?: {
    readonly condition: Expression;
    readonly noCycle?: boolean;
  };
  readonly groupBy?: GroupByClause;
  readonly having?: HavingClause;
  readonly orderBy?: OrderByClause;
  readonly limit?: LimitClause;
  readonly offset?: OffsetClause;
  readonly fetch?: { readonly count: Expression; readonly withTies?: boolean };
  readonly lockingClause?: string;
  readonly optionClause?: string;
  readonly windowClause?: readonly { readonly name: string; readonly spec: WindowSpec }[];
  readonly leadingComments: readonly CommentNode[];
  readonly parenthesized?: boolean;
}

export interface InsertStatement {
  readonly type: 'insert';
  readonly ignore?: boolean;
  readonly orConflictAction?: 'ROLLBACK' | 'ABORT' | 'FAIL' | 'IGNORE' | 'REPLACE';
  readonly table: string;
  readonly alias?: string;
  readonly columns: readonly string[];
  readonly overriding?: 'SYSTEM VALUE' | 'USER VALUE';
  readonly valueClauseLeadingComments?: readonly CommentNode[];
  readonly defaultValues?: boolean;
  readonly values?: readonly ValuesList[];
  readonly setItems?: readonly SetItem[];
  readonly valuesAlias?: {
    readonly name: string;
    readonly columns?: readonly string[];
  };
  readonly executeSource?: string;
  readonly tableSource?: {
    readonly table: string;
    readonly alias?: string;
    readonly aliasColumns?: readonly string[];
  };
  readonly selectQuery?: QueryExpression;
  readonly onDuplicateKeyUpdate?: readonly SetItem[];
  readonly returning?: readonly Expression[];
  readonly returningInto?: readonly string[];
  readonly onConflict?: {
    readonly columns?: readonly Expression[];
    readonly constraintName?: string;
    readonly targetWhere?: Expression;
    readonly action: 'nothing' | 'update';
    readonly setItems?: readonly SetItem[];
    readonly where?: Expression;
  };
  readonly leadingComments: readonly CommentNode[];
}

export interface UpdateStatement {
  readonly type: 'update';
  readonly table: string;
  readonly alias?: string;
  readonly additionalTables?: readonly { readonly table: string; readonly alias?: string }[];
  readonly joinSources?: readonly JoinClause[];
  readonly setItems: readonly SetItem[];
  readonly from?: readonly FromClause[];
  readonly fromJoins?: readonly JoinClause[];
  readonly where?: WhereClause;
  readonly returning?: readonly Expression[];
  readonly leadingComments: readonly CommentNode[];
}

export interface DeleteStatement {
  readonly type: 'delete';
  readonly targets?: readonly string[];
  readonly from: string;
  readonly alias?: string;
  readonly fromJoins?: readonly JoinClause[];
  readonly using?: readonly FromClause[];
  readonly usingJoins?: readonly JoinClause[];
  readonly where?: WhereClause;
  readonly currentOf?: string;
  readonly returning?: readonly Expression[];
  readonly leadingComments: readonly CommentNode[];
}

export interface CreateTableStatement {
  readonly type: 'create_table';
  readonly orReplace?: boolean;
  readonly tableName: string;
  readonly likeTable?: string;
  readonly elements: readonly TableElement[];
  readonly trailingComma?: boolean;
  readonly tableOptions?: string;
  readonly asQuery?: QueryExpression;
  readonly asExecute?: string;
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
  | AlterDropConstraintAction
  | AlterAlterColumnAction
  | AlterOwnerToAction
  | AlterRenameToAction
  | AlterRenameColumnAction
  | AlterSetSchemaAction
  | AlterSetTablespaceAction
  | AlterRawAction;

export interface AlterAddColumnAction {
  readonly type: 'add_column';
  readonly explicitColumnKeyword?: boolean;
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

export interface AlterDropConstraintAction {
  readonly type: 'drop_constraint';
  readonly ifExists?: boolean;
  readonly constraintName: string;
  readonly behavior?: 'CASCADE' | 'RESTRICT';
}

export interface AlterAlterColumnAction {
  readonly type: 'alter_column';
  readonly columnName: string;
  readonly operation: string;
}

export interface AlterOwnerToAction {
  readonly type: 'owner_to';
  readonly owner: string;
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
  readonly concurrently?: boolean;
  readonly ifExists: boolean;
  readonly objectName: string;
  readonly withOptions?: string;
  readonly behavior?: 'CASCADE' | 'RESTRICT' | 'CASCADE CONSTRAINT' | 'CASCADE CONSTRAINTS';
  readonly leadingComments: readonly CommentNode[];
}

export interface UnionStatement {
  readonly type: 'union';
  readonly members: readonly { readonly statement: QueryExpression; readonly parenthesized: boolean }[];
  readonly operators: readonly string[]; // 'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT'
  readonly orderBy?: OrderByClause;
  readonly limit?: LimitClause;
  readonly offset?: OffsetClause;
  readonly fetch?: { readonly count: Expression; readonly withTies?: boolean };
  readonly lockingClause?: string;
  readonly leadingComments: readonly CommentNode[];
}

export interface CTEStatement {
  readonly type: 'cte';
  readonly recursive?: boolean;
  readonly ctes: readonly CTEDefinition[];
  readonly search?: CTESearchClause;
  readonly cycle?: CTECycleClause;
  readonly mainQuery: CTEMainStatement;
  readonly leadingComments: readonly CommentNode[];
}

export interface CTESearchClause {
  readonly mode: 'DEPTH FIRST' | 'BREADTH FIRST';
  readonly by: readonly string[];
  readonly set: string;
}

export interface CTECycleClause {
  readonly columns: readonly string[];
  readonly set: string;
  readonly using?: string;
  readonly to?: Expression;
  readonly default?: Expression;
}

export interface CTEDefinition {
  readonly name: string;
  readonly columnList?: readonly string[];   // e.g., (revenue_date, amount)
  readonly materialized?: 'materialized' | 'not_materialized';
  readonly query: QueryExpression;
  readonly leadingComments?: readonly CommentNode[];  // comments before this CTE definition
}

export interface MergeStatement {
  readonly type: 'merge';
  readonly target: { readonly table: string; readonly alias?: string };
  readonly source: { readonly table: string | Expression; readonly alias?: string };
  readonly on: Expression;
  readonly whenClauses: readonly MergeWhenClause[];
  readonly leadingComments: readonly CommentNode[];
}

export interface MergeWhenClause {
  readonly matched: boolean;
  readonly condition?: Expression;
  readonly action: 'delete' | 'update' | 'insert';
  readonly setItems?: readonly SetItem[];
  readonly columns?: readonly string[];
  readonly values?: readonly Expression[];
}

export interface ExplainStatement {
  readonly type: 'explain';
  readonly planFor?: boolean;
  readonly analyze?: boolean;
  readonly verbose?: boolean;
  readonly costs?: boolean;
  readonly buffers?: boolean;
  readonly timing?: boolean;
  readonly summary?: boolean;
  readonly settings?: boolean;
  readonly wal?: boolean;
  readonly format?: 'TEXT' | 'XML' | 'JSON' | 'YAML';
  readonly statement: QueryExpression | InsertStatement | UpdateStatement | DeleteStatement;
  readonly leadingComments: readonly CommentNode[];
}

export interface CreateIndexStatement {
  readonly type: 'create_index';
  readonly unique?: boolean;
  readonly clustered?: 'CLUSTERED' | 'NONCLUSTERED';
  readonly concurrently?: boolean;
  readonly ifNotExists?: boolean;
  readonly name?: string;
  readonly table: string;
  readonly only?: boolean;
  readonly using?: string;
  readonly columns: readonly Expression[];
  readonly include?: readonly Expression[];
  readonly where?: Expression;
  readonly options?: string;
  readonly leadingComments: readonly CommentNode[];
}

export interface CreateViewStatement {
  readonly type: 'create_view';
  readonly orReplace?: boolean;
  readonly temporary?: boolean;
  readonly materialized?: boolean;
  readonly ifNotExists?: boolean;
  readonly name: string;
  readonly columnList?: readonly string[];
  readonly toTable?: string;
  readonly toColumns?: readonly string[];
  readonly comment?: string;
  readonly withOptions?: string;
  readonly query: Statement;
  readonly withData?: boolean;
  readonly withClause?: string;
  readonly leadingComments: readonly CommentNode[];
}

export interface CreatePolicyStatement {
  readonly type: 'create_policy';
  readonly name: string;
  readonly table: string;
  readonly permissive?: 'PERMISSIVE' | 'RESTRICTIVE';
  readonly command?: 'ALL' | 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';
  readonly roles?: readonly string[];
  readonly using?: Expression;
  readonly withCheck?: Expression;
  readonly leadingComments: readonly CommentNode[];
}

export interface GrantStatement {
  readonly type: 'grant';
  readonly kind: 'GRANT' | 'REVOKE';
  readonly grantOptionFor?: boolean;
  readonly privileges: readonly string[];
  readonly object?: string;
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
  readonly alias?: {
    readonly name: string;
    readonly columns?: readonly string[];
  };
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
  readonly startsOnOwnLine?: boolean;
  readonly blankLinesBefore?: number;  // number of blank lines preceding this comment (0 = none)
  readonly blankLinesAfter?: number;   // number of blank lines before the next non-comment token
}

// Expressions

export interface IdentifierExpr {
  readonly type: 'identifier';
  readonly value: string;
  readonly quoted: boolean;
  readonly withDescendants?: boolean;
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
  readonly dataType: string;
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
  readonly separator?: Expression;
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
  readonly whenClauses: readonly {
    readonly condition: Expression;
    readonly result: Expression;
    readonly trailingComment?: string;
  }[];
  readonly elseResult?: Expression;
}

export interface BetweenExpr {
  readonly type: 'between';
  readonly expr: Expression;
  readonly low: Expression;
  readonly high: Expression;
  readonly negated: boolean;
}

export type QuantifiedComparisonExpr =
  | QuantifiedComparisonSubqueryExpr
  | QuantifiedComparisonListExpr;

export interface QuantifiedComparisonSubqueryExpr {
  readonly type: 'quantified_comparison';
  readonly kind: 'subquery';
  readonly left: Expression;
  readonly operator: string;
  readonly quantifier: 'ALL' | 'ANY' | 'SOME';
  readonly subquery: SubqueryExpr;
}

export interface QuantifiedComparisonListExpr {
  readonly type: 'quantified_comparison';
  readonly kind: 'list';
  readonly left: Expression;
  readonly operator: string;
  readonly quantifier: 'ALL' | 'ANY' | 'SOME';
  readonly values: readonly Expression[];
}

// InExpr discriminated union: list vs subquery
export type InExpr = InExprList | InExprSubquery;

export interface InExprList {
  readonly type: 'in';
  readonly kind: 'list';
  readonly expr: Expression;
  readonly values: readonly Expression[];
  readonly negated: boolean;
}

export interface InExprSubquery {
  readonly type: 'in';
  readonly kind: 'subquery';
  readonly expr: Expression;
  readonly subquery: SubqueryExpr;
  readonly negated: boolean;
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

export interface TupleExpr {
  readonly type: 'tuple';
  readonly items: readonly Expression[];
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
  readonly baseWindowName?: string;
  readonly partitionBy?: readonly Expression[];
  readonly orderBy?: readonly OrderByItem[];
  readonly frame?: FrameSpec;
  readonly exclude?: string;
}

export interface FrameSpec {
  readonly unit: 'ROWS' | 'RANGE' | 'GROUPS';
  readonly start: FrameBound;
  readonly end?: FrameBound;
}

export interface FrameBound {
  readonly kind: 'UNBOUNDED PRECEDING' | 'UNBOUNDED FOLLOWING' | 'CURRENT ROW' | 'PRECEDING' | 'FOLLOWING';
  readonly value?: Expression;
}

export interface WindowFunctionExpr {
  readonly type: 'window_function';
  readonly func: FunctionCallExpr;
  readonly nullTreatment?: 'IGNORE NULLS' | 'RESPECT NULLS';
  readonly partitionBy?: readonly Expression[];
  readonly orderBy?: readonly OrderByItem[];
  readonly frame?: FrameSpec;
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
  readonly style?: 'from_for' | 'comma';
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

export interface CollateExpr {
  readonly type: 'collate';
  readonly expr: Expression;
  readonly collation: string;
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
  readonly reason?: RawReason;
}

export type RawReason =
  | 'parse_error'
  | 'unsupported'
  | 'comment_only'
  | 'verbatim'
  | 'transaction_control'
  | 'trailing_semicolon_comment'
  | 'slash_terminator';

// Column expression with optional alias and comment
export interface ColumnExpr {
  readonly expr: Expression;
  readonly alias?: string;
  readonly leadingComments?: readonly CommentNode[];
  readonly trailingComment?: CommentNode;
}

export interface FromClause {
  readonly table: Expression;
  readonly alias?: string;
  readonly aliasColumns?: readonly string[];
  readonly indexHint?: string;
  readonly pivotClause?: string;
  readonly lateral?: boolean;
  readonly ordinality?: boolean;
  readonly tablesample?: { readonly method: string; readonly args: readonly Expression[]; readonly repeatable?: Expression };
  readonly trailingComments?: readonly CommentNode[];
}

export interface JoinClause {
  readonly joinType: string; // 'INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', etc.
  readonly table: Expression;
  readonly alias?: string;
  readonly aliasColumns?: readonly string[];
  readonly indexHint?: string;
  readonly pivotClause?: string;
  readonly lateral?: boolean;
  readonly ordinality?: boolean;
  readonly on?: Expression;
  readonly usingClause?: readonly string[];
  readonly usingAlias?: string;
  readonly usingAliasColumns?: readonly string[];
  readonly trailingComment?: CommentNode;
}

export interface WhereClause {
  readonly condition: Expression;
  readonly trailingComment?: CommentNode;
}

export interface GroupByClause {
  readonly setQuantifier?: 'ALL' | 'DISTINCT';
  readonly items: readonly Expression[];
  readonly groupingSets?: readonly { readonly type: 'grouping_sets' | 'rollup' | 'cube'; readonly sets: readonly (readonly Expression[])[] }[];
  readonly withRollup?: boolean;
}

export interface HavingClause {
  readonly condition: Expression;
}

export interface OrderByClause {
  readonly items: readonly OrderByItem[];
}

export interface OrderByItem {
  readonly expr: Expression;
  readonly usingOperator?: string;
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
  readonly assignmentOperator?: '=' | '+=' | '-=' | '*=' | '/=' | '%=' | '&=' | '^=' | '|=';
  readonly methodCall?: boolean;
}

export interface ValuesList {
  readonly values: readonly Expression[];
  readonly leadingComments?: readonly CommentNode[];
  readonly trailingComments?: readonly CommentNode[];
}

export interface TableElement {
  readonly elementType: 'column' | 'primary_key' | 'constraint' | 'foreign_key' | 'comment';
  readonly raw: string;
  readonly name?: string;
  readonly dataType?: string;
  readonly constraints?: string;
  readonly columnConstraints?: readonly ColumnConstraint[];
  readonly trailingComment?: string;
  readonly constraintName?: string;
  readonly constraintBody?: string;
  readonly constraintType?: 'check' | 'raw';
  readonly checkExpr?: Expression;
  readonly fkColumns?: string;
  readonly fkRefTable?: string;
  readonly fkRefColumns?: string;
  readonly fkActions?: string;
}

export type ColumnConstraint =
  | ColumnConstraintNotNull
  | ColumnConstraintNull
  | ColumnConstraintDefault
  | ColumnConstraintCheck
  | ColumnConstraintReferences
  | ColumnConstraintGeneratedIdentity
  | ColumnConstraintPrimaryKey
  | ColumnConstraintUnique
  | ColumnConstraintRaw;

export interface ColumnConstraintNotNull {
  readonly type: 'not_null';
  readonly name?: string;
}

export interface ColumnConstraintNull {
  readonly type: 'null';
  readonly name?: string;
}

export interface ColumnConstraintDefault {
  readonly type: 'default';
  readonly name?: string;
  readonly expr: Expression;
}

export interface ColumnConstraintCheck {
  readonly type: 'check';
  readonly name?: string;
  readonly expr: Expression;
}

export interface ColumnConstraintReferences {
  readonly type: 'references';
  readonly name?: string;
  readonly table: string;
  readonly columns?: readonly string[];
  readonly matchType?: 'SIMPLE' | 'FULL' | 'PARTIAL';
  readonly actions?: readonly ReferentialAction[];
  readonly deferrable?: 'DEFERRABLE' | 'NOT DEFERRABLE';
  readonly initially?: 'DEFERRED' | 'IMMEDIATE';
}

export interface ReferentialAction {
  readonly event: 'DELETE' | 'UPDATE';
  readonly action: 'RESTRICT' | 'CASCADE' | 'SET NULL' | 'SET DEFAULT' | 'NO ACTION';
}

export interface ColumnConstraintGeneratedIdentity {
  readonly type: 'generated_identity';
  readonly name?: string;
  readonly always: boolean;
  readonly onNull?: boolean;
  readonly options?: string;
}

export interface ColumnConstraintPrimaryKey {
  readonly type: 'primary_key';
  readonly name?: string;
}

export interface ColumnConstraintUnique {
  readonly type: 'unique';
  readonly name?: string;
}

export interface ColumnConstraintRaw {
  readonly type: 'raw';
  readonly name?: string;
  readonly text: string;
}
