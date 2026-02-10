// SQL reserved keywords for recognition and uppercasing.
//
// Design: KEYWORD_LIST contains SQL reserved words and clause keywords.
// FUNCTION_KEYWORD_LIST contains keywords that act as function names (followed
// by parens). Some words could appear in both lists; to avoid duplication, each
// word appears in exactly one list. KEYWORDS is the union of both sets, used by
// the tokenizer to recognize all keywords.
const KEYWORD_LIST = [
  'ABSOLUTE', 'ACTION', 'ADD', 'ALTER', 'ALWAYS', 'AND', 'APPLY', 'ARRAY', 'AS', 'ASC',
  'AFTER',
  'AUTO_INCREMENT',
  'BEFORE',
  'BEGIN', 'BERNOULLI', 'BETWEEN', 'BIGINT', 'BIGSERIAL', 'BINARY', 'BIT', 'BLOB', 'BOOL', 'BOOLEAN', 'BOTH', 'BYTEA', 'BY',
  'CASCADE', 'CASE', 'CHAR', 'CHARACTER', 'CHARSET', 'CHECK', 'CIDR', 'CLOB', 'CLOSE',
  'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'CONCURRENTLY', 'CONFLICT', 'CONSTRAINT', 'COPY', 'CREATE', 'CROSS', 'CUBE', 'CURRENT',
  'CURRENT_DATE', 'CURRENT_ROW', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
  'CURRENT_USER', 'CURSOR',
  'DATA', 'DATABASE', 'DATE', 'DATETIME', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DELETE', 'DOMAIN',
  'DESC', 'DISTINCT', 'DO', 'DOUBLE', 'DROP',
  'EACH',
  'ELSE', 'END', 'ENGINE', 'ESCAPE', 'EXCEPT', 'EXCLUDE', 'EXCLUDED', 'EXECUTE', 'EXTENSION',
  'ENCRYPTED',
  'FALSE', 'FETCH', 'FILE', 'FILTER', 'FLOAT', 'FLOAT4', 'FLOAT8', 'FOLLOWING', 'FOR', 'FOREIGN', 'FORMAT', 'FROM', 'FULL',
  'FUNCTION',
  'GENERATED', 'GO', 'GRANT', 'GROUP',
  'HAVING',
  'IDENTITY', 'ILIKE', 'IN', 'INET', 'INDEX', 'INNER', 'INSERT', 'INT', 'INT2', 'INT4', 'INT8', 'INTEGER', 'INTERSECT',
  'INTERVAL', 'INTO', 'IS',
  'JOIN', 'JSON', 'JSONB',
  'KEY',
  'LATERAL', 'LEADING', 'LIKE', 'LIMIT',
  'LOGIN',
  'MACADDR', 'MATCHED', 'MATERIALIZED', 'MERGE', 'MONEY',
  'MEDIUMTEXT',
  'MODIFY',
  'NATIONAL', 'NATURAL', 'NCHAR', 'NEXT', 'NO', 'NOLOCK', 'NOT', 'NULL', 'NUMERIC', 'NVARCHAR',
  'OFF',
  'OF', 'OFFSET', 'ON', 'ONLY', 'OPEN', 'OR', 'ORDER', 'OUTER', 'OVER', 'OVERLAY',
  'PASSWORD',
  'PARTITION', 'PIVOT', 'PLACING', 'PRECEDING', 'PRECISION', 'PRIMARY', 'PROCEDURE',
  'PRIVILEGES',
  'PUBLICATION',
  'RANGE', 'REAL', 'RECURSIVE', 'REFERENCES', 'RELEASE', 'REPEATABLE', 'RESTART', 'RETURN', 'RETURNING', 'RETURNS', 'REVOKE', 'ROLLBACK', 'ROLLUP',
  'ROLE',
  'ROWS',
  'SESSION',
  'SAVEPOINT', 'SCHEMA', 'SELECT', 'SERIAL', 'SET', 'SHARE', 'SHOW', 'SIMILAR', 'SKIP', 'SMALLINT', 'SOME',
  'SEQUENCE',
  'STAGE',
  'START',
  'STORED',
  'STRAIGHT_JOIN',
  'SYSTEM',
  'TABLE', 'TABLES', 'TABLESAMPLE', 'TEMPORARY', 'TEXT', 'THEN', 'TIES', 'TIME', 'TIMESTAMP', 'TIMESTAMPTZ', 'TINYINT', 'TINYTEXT', 'TO', 'TOP', 'TRAILING', 'TRANSACTION', 'TRIGGER', 'TYPE',
  'TRUE', 'TRUNCATE', 'TSQUERY', 'TSVECTOR',
  'UNBOUNDED', 'UNION', 'UNIQUE', 'UNPIVOT', 'UNSIGNED', 'UPDATE', 'URL', 'USE', 'USAGE', 'USER', 'USING', 'UUID',
  'VALUE', 'VALUES', 'VARCHAR', 'VARYING', 'VIEW',
  'WHEN', 'WHERE', 'WINDOW', 'WITH', 'WITHIN', 'WITHOUT', 'ZONE',
  'LONGTEXT',
  'NOWAIT', 'LOCKED',
  'ENUM',
] as const;

// Function-like keywords (followed by parens) -- uppercased but not clause keywords.
const FUNCTION_KEYWORD_LIST = [
  'ABS', 'ALL', 'ANY', 'ARRAY_AGG', 'ARRAY_LENGTH', 'AVG', 'CAST', 'CEIL', 'CEILING',
  'COALESCE', 'CONCAT', 'CONVERT', 'COUNT', 'CUME_DIST', 'CURRVAL', 'DATEADD', 'DATE_PART', 'DATE_TRUNC',
  'CURDATE', 'CURTIME', 'DAY', 'DAYOFWEEK', 'DATE_FORMAT', 'DENSE_RANK', 'EXISTS', 'EXTRACT', 'FIRST_VALUE', 'FLOOR',
  'GREATEST', 'GROUP_CONCAT', 'GROUPING', 'HOUR', 'JSONB_AGG', 'JSONB_ARRAY_ELEMENTS', 'JSONB_BUILD_OBJECT',
  'IF', 'IFNULL',
  'JSONB_EACH', 'JSONB_PATH_QUERY_ARRAY', 'LAG', 'LAST_VALUE', 'LEAD', 'LEAST',
  'LEFT', 'LENGTH', 'LOWER', 'LTRIM', 'MAX', 'MIN', 'MINUTE', 'MOD', 'MODE', 'MONTH', 'NEXTVAL', 'NOTHING',
  'NTH_VALUE', 'NTILE', 'NOW', 'NULLIF', 'PERCENT_RANK', 'PERCENTILE_CONT', 'PERCENTILE_DISC',
  'POSITION', 'RANK', 'REPLACE', 'RIGHT', 'ROW', 'ROW_NUMBER', 'ROUND', 'SECOND', 'SETVAL',
  'SET_CONFIG', 'SIGN', 'STDDEV', 'STRING_AGG', 'SUBSTRING', 'SUM', 'TO_CHAR', 'TRIM', 'TRY_CAST', 'UNNEST', 'UPPER', 'YEAR',
  'RTRIM',
  'CORR', 'SAFE_CAST',
] as const;

// KEYWORDS is the union of both lists, used by the tokenizer for recognition.
export const KEYWORDS = new Set<string>([...KEYWORD_LIST, ...FUNCTION_KEYWORD_LIST]);

// Keywords that are function-like (followed by parens) -- should be uppercased
// but not treated as clause keywords.
export const FUNCTION_KEYWORDS = new Set<string>(FUNCTION_KEYWORD_LIST);

export function isKeyword(word: string): boolean {
  return KEYWORDS.has(word.toUpperCase());
}
