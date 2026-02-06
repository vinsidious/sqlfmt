// SQL reserved keywords for recognition and uppercasing
export const KEYWORDS = new Set([
  'ABSOLUTE', 'ACTION', 'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC',
  'BETWEEN', 'BIGINT', 'BINARY', 'BIT', 'BLOB', 'BOOLEAN', 'BOTH', 'BY',
  'CASCADE', 'CASE', 'CAST', 'CHAR', 'CHARACTER', 'CHECK', 'CLOB', 'CLOSE',
  'COALESCE', 'COLUMN', 'COMMIT', 'CONSTRAINT', 'CREATE', 'CROSS', 'CURRENT',
  'CURRENT_DATE', 'CURRENT_ROW', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
  'CURRENT_USER', 'CURSOR',
  'DATABASE', 'DATE', 'DAY', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DELETE',
  'DESC', 'DISTINCT', 'DOUBLE', 'DROP',
  'ELSE', 'END', 'ESCAPE', 'EXCEPT', 'EXECUTE', 'EXISTS', 'EXTRACT',
  'FALSE', 'FETCH', 'FLOAT', 'FOLLOWING', 'FOR', 'FOREIGN', 'FROM', 'FULL',
  'FUNCTION',
  'GRANT', 'GROUP',
  'HAVING', 'HOUR',
  'IF', 'IN', 'INDEX', 'INNER', 'INSERT', 'INT', 'INTEGER', 'INTERSECT',
  'INTERVAL', 'INTO', 'IS',
  'JOIN',
  'KEY',
  'LEADING', 'LEFT', 'LIKE', 'LIMIT',
  'MAX', 'MIN', 'MINUTE', 'MONTH',
  'NATIONAL', 'NATURAL', 'NCHAR', 'NO', 'NOT', 'NULL', 'NULLIF', 'NUMERIC',
  'OF', 'OFFSET', 'ON', 'ONLY', 'OPEN', 'OR', 'ORDER', 'OUTER', 'OVER',
  'PARTITION', 'PRECEDING', 'PRIMARY', 'PROCEDURE',
  'RANGE', 'REAL', 'RECURSIVE', 'REFERENCES', 'REPLACE', 'RIGHT', 'ROLLBACK',
  'ROW', 'ROW_NUMBER', 'ROWS',
  'SECOND', 'SELECT', 'SET', 'SMALLINT', 'SOME', 'SUM',
  'TABLE', 'THEN', 'TIME', 'TIMESTAMP', 'TO', 'TRAILING', 'TRIGGER', 'TRIM',
  'TRUE', 'TRUNCATE',
  'UNBOUNDED', 'UNION', 'UNIQUE', 'UPDATE', 'UPPER', 'USER', 'USING',
  'VALUES', 'VARCHAR', 'VARYING', 'VIEW',
  'WHEN', 'WHERE', 'WITH',
  'YEAR',
  // Aggregate/window functions treated as keywords for uppercasing
  'AVG', 'COUNT', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
  'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE',
  'ABS', 'CEIL', 'CEILING', 'FLOOR', 'ROUND', 'SIGN',
  'LOWER', 'LENGTH', 'SUBSTRING', 'CONCAT', 'POSITION', 'REPLACE',
  'DATE_TRUNC', 'TO_CHAR', 'EXTRACT',
]);

// Keywords that are function-like (followed by parens) — should be uppercased
// but not treated as clause keywords
export const FUNCTION_KEYWORDS = new Set([
  'AVG', 'COUNT', 'MAX', 'MIN', 'SUM',
  'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
  'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE',
  'COALESCE', 'NULLIF', 'CAST', 'EXTRACT', 'TRIM',
  'ABS', 'CEIL', 'CEILING', 'FLOOR', 'ROUND', 'SIGN',
  'LOWER', 'UPPER', 'LENGTH', 'SUBSTRING', 'CONCAT', 'POSITION', 'REPLACE',
  'DATE_TRUNC', 'TO_CHAR', 'EXTRACT',
  'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
  'EXISTS',
]);

export function isKeyword(word: string): boolean {
  return KEYWORDS.has(word.toUpperCase());
}

export function toUpperKeyword(word: string): string {
  if (isKeyword(word)) return word.toUpperCase();
  return word;
}
