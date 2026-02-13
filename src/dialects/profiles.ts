import { FUNCTION_KEYWORD_LIST, KEYWORD_LIST } from '../keywords';
import type { DialectProfile, DialectStatementHandler } from './types';

const BASE_CLAUSE_KEYWORDS = new Set([
  'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
  'UNION', 'INTERSECT', 'EXCEPT', 'ON', 'SET', 'VALUES',
  'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'JOIN',
  'INTO', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER',
  'DROP', 'WITH', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR',
  'RETURNING', 'FETCH', 'WINDOW', 'LATERAL', 'FOR', 'USING', 'ESCAPE',
  'PIVOT', 'UNPIVOT', 'GO', 'OPTION',
  'START', 'CONNECT', 'BY', 'PRIOR', 'NOCYCLE',
]);

const BASE_STATEMENT_STARTERS = new Set([
  'SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'MERGE',
  'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
  'GRANT', 'REVOKE', 'COMMENT', 'CALL',
  'EXPLAIN',
  'PRAGMA', 'SHOW', 'FLUSH',
  'LOCK', 'UNLOCK',
  'BACKUP', 'BULK', 'CLUSTER',
  'PRINT',
  'SET', 'RESET', 'ANALYZE', 'ANALYSE', 'VACUUM',
  'REINDEX',
  'DECLARE', 'PREPARE', 'EXECUTE', 'EXEC', 'DEALLOCATE',
  'USE', 'DO', 'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE',
  'START', 'VALUES', 'COPY', 'DELIMITER',
  'GO', 'DBCC', 'ACCEPT', 'DESC', 'DESCRIBE', 'REM', 'DEFINE', 'PROMPT', 'SP_RENAME',
]);

const BASE_KEYWORDS = new Set<string>([
  ...KEYWORD_LIST,
  ...FUNCTION_KEYWORD_LIST,
]);

const BASE_FUNCTION_KEYWORDS = new Set<string>(FUNCTION_KEYWORD_LIST);

// Per-dialect statement handlers
const ANSI_STATEMENT_HANDLERS: Readonly<Record<string, DialectStatementHandler>> = {};

const POSTGRES_STATEMENT_HANDLERS: Readonly<Record<string, DialectStatementHandler>> = {
  CLUSTER: { kind: 'verbatim_unsupported' },
};

const MYSQL_STATEMENT_HANDLERS: Readonly<Record<string, DialectStatementHandler>> = {
  DELIMITER: { kind: 'delimiter_script' },
};

const TSQL_STATEMENT_HANDLERS: Readonly<Record<string, DialectStatementHandler>> = {
  GO: { kind: 'single_line_unsupported' },
  BACKUP: { kind: 'verbatim_unsupported' },
  BULK: { kind: 'verbatim_unsupported' },
  DBCC: { kind: 'verbatim_unsupported' },
};

/**
 * Freeze a Set at runtime and prevent mutation through prototype calls.
 */
function freezeSet<T>(set: Set<T>): ReadonlySet<T> {
  const frozen = new Proxy(set, {
    get(target, property) {
      if (property === 'add') {
        return () => { throw new TypeError('Cannot add to a frozen Set'); };
      }
      if (property === 'delete') {
        return () => { throw new TypeError('Cannot delete from a frozen Set'); };
      }
      if (property === 'clear') {
        return () => { throw new TypeError('Cannot clear a frozen Set'); };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  Object.freeze(frozen);
  return frozen;
}

function makeProfile(
  name: DialectProfile['name'],
  options: {
    extraKeywords?: readonly string[];
    removeKeywords?: readonly string[];
    extraClauseKeywords?: readonly string[];
    removeClauseKeywords?: readonly string[];
    extraStatementStarters?: readonly string[];
    removeStatementStarters?: readonly string[];
    extraFunctionKeywords?: readonly string[];
    removeFunctionKeywords?: readonly string[];
    statementHandlers?: Readonly<Record<string, DialectStatementHandler>>;
  },
): DialectProfile {
  const keywords = new Set(BASE_KEYWORDS);
  for (const kw of options.extraKeywords ?? []) keywords.add(kw.toUpperCase());
  for (const kw of options.removeKeywords ?? []) keywords.delete(kw.toUpperCase());

  const functionKeywords = new Set(BASE_FUNCTION_KEYWORDS);
  for (const kw of options.extraFunctionKeywords ?? []) {
    functionKeywords.add(kw.toUpperCase());
    keywords.add(kw.toUpperCase()); // Also add to keywords so the tokenizer recognizes them
  }
  for (const kw of options.removeFunctionKeywords ?? []) functionKeywords.delete(kw.toUpperCase());

  const clauseKeywords = new Set(BASE_CLAUSE_KEYWORDS);
  for (const kw of options.extraClauseKeywords ?? []) clauseKeywords.add(kw.toUpperCase());
  for (const kw of options.removeClauseKeywords ?? []) clauseKeywords.delete(kw.toUpperCase());

  const statementStarters = new Set(BASE_STATEMENT_STARTERS);
  for (const kw of options.extraStatementStarters ?? []) statementStarters.add(kw.toUpperCase());
  for (const kw of options.removeStatementStarters ?? []) statementStarters.delete(kw.toUpperCase());

  const profile: DialectProfile = {
    name,
    keywords: freezeSet(keywords),
    functionKeywords: freezeSet(functionKeywords),
    clauseKeywords: freezeSet(clauseKeywords),
    statementStarters: freezeSet(statementStarters),
    statementHandlers: options.statementHandlers ?? ANSI_STATEMENT_HANDLERS,
  };
  return Object.freeze(profile);
}

export const ANSI_PROFILE: DialectProfile = makeProfile('ansi', {
  statementHandlers: ANSI_STATEMENT_HANDLERS,
  removeKeywords: [
    'AUTO_INCREMENT', 'BIGSERIAL', 'SERIAL', 'BYTEA', 'CIDR', 'INET', 'MACADDR',
    'JSONB', 'TSQUERY', 'TSVECTOR', 'TIMESTAMPTZ', 'INT2', 'INT4', 'INT8',
    'FLOAT4', 'FLOAT8', 'BOOL', 'ILIKE', 'CONCURRENTLY', 'NOLOCK', 'TOP',
    'PRINT', 'STRAIGHT_JOIN', 'MEDIUMTEXT', 'LONGTEXT', 'TINYTEXT', 'UNSIGNED',
    'ENGINE', 'CHARSET', 'MODIFY', 'MONEY', 'PIVOT', 'UNPIVOT', 'PUBLICATION',
    'EXTENSION', 'SIMILAR', 'RETURNING', 'TINYINT', 'ENUM', 'LATERAL',
  ],
  removeFunctionKeywords: [
    'GROUP_CONCAT', 'IFNULL', 'IF', 'CURDATE', 'CURTIME', 'DATE_FORMAT', 'DAYOFWEEK',
    'JSONB_AGG', 'JSONB_ARRAY_ELEMENTS', 'JSONB_BUILD_OBJECT', 'JSONB_EACH',
    'JSONB_PATH_QUERY_ARRAY', 'CURRVAL', 'NEXTVAL', 'SETVAL', 'SET_CONFIG',
    'ARRAY_AGG', 'ARRAY_LENGTH', 'UNNEST', 'TO_CHAR', 'DATE_PART', 'DATE_TRUNC',
    'DATEADD', 'TRY_CAST', 'SAFE_CAST',
  ],
  removeClauseKeywords: ['RETURNING', 'LIMIT', 'PIVOT', 'UNPIVOT', 'GO', 'OPTION', 'LATERAL'],
  removeStatementStarters: [
    'PRAGMA', 'FLUSH', 'LOCK', 'UNLOCK', 'ANALYSE', 'VACUUM', 'REINDEX',
    'GO', 'DBCC', 'DELIMITER', 'BACKUP', 'BULK', 'CLUSTER', 'PRINT',
    'EXEC', 'SP_RENAME', 'ACCEPT',
  ],
});

export const POSTGRES_PROFILE: DialectProfile = makeProfile('postgres', {
  statementHandlers: POSTGRES_STATEMENT_HANDLERS,
  extraKeywords: ['ANALYSE', 'VACUUM', 'REINDEX', 'NOTIFY', 'LISTEN', 'UNLISTEN'],
  extraFunctionKeywords: [
    'GENERATE_SERIES', 'REGEXP_REPLACE', 'REGEXP_MATCHES', 'SPLIT_PART',
    'ARRAY_TO_STRING', 'STRING_TO_ARRAY',
  ],
  removeKeywords: [
    'AUTO_INCREMENT', 'NOLOCK', 'TOP', 'PRINT', 'STRAIGHT_JOIN', 'MEDIUMTEXT',
    'LONGTEXT', 'TINYTEXT', 'UNSIGNED', 'ENGINE', 'CHARSET', 'MODIFY',
    'PIVOT', 'UNPIVOT',
  ],
  removeFunctionKeywords: [
    'GROUP_CONCAT', 'IFNULL', 'IF', 'CURDATE', 'CURTIME', 'DATE_FORMAT', 'DAYOFWEEK',
    'DATEADD', 'TRY_CAST', 'SAFE_CAST',
  ],
  removeClauseKeywords: ['PIVOT', 'UNPIVOT', 'GO', 'OPTION'],
  removeStatementStarters: [
    'PRAGMA', 'GO', 'DBCC', 'DELIMITER', 'BACKUP', 'BULK', 'PRINT',
    'EXEC', 'SP_RENAME', 'ACCEPT',
  ],
});

export const MYSQL_PROFILE: DialectProfile = makeProfile('mysql', {
  statementHandlers: MYSQL_STATEMENT_HANDLERS,
  extraKeywords: ['DUPLICATE', 'REPLACE'],
  extraFunctionKeywords: [
    'LOCATE', 'INSTR', 'LCASE', 'UCASE', 'MATCH', 'VALUES',
  ],
  removeKeywords: [
    'BIGSERIAL', 'SERIAL', 'BYTEA', 'CIDR', 'INET', 'MACADDR', 'JSONB',
    'TSQUERY', 'TSVECTOR', 'TIMESTAMPTZ', 'INT2', 'INT4', 'INT8', 'FLOAT4',
    'FLOAT8', 'BOOL', 'ILIKE', 'SIMILAR', 'CONCURRENTLY', 'RETURNING',
    'NOLOCK', 'TOP', 'PRINT', 'PIVOT', 'UNPIVOT', 'PUBLICATION', 'EXTENSION', 'MONEY',
  ],
  removeFunctionKeywords: [
    'JSONB_AGG', 'JSONB_ARRAY_ELEMENTS', 'JSONB_BUILD_OBJECT', 'JSONB_EACH',
    'JSONB_PATH_QUERY_ARRAY', 'CURRVAL', 'NEXTVAL', 'SETVAL', 'SET_CONFIG',
    'ARRAY_AGG', 'ARRAY_LENGTH', 'UNNEST', 'TO_CHAR', 'DATE_PART', 'DATE_TRUNC',
    'DATEADD', 'TRY_CAST', 'SAFE_CAST', 'STRING_AGG', 'CORR',
  ],
  removeClauseKeywords: ['RETURNING', 'PIVOT', 'UNPIVOT', 'GO', 'OPTION'],
  removeStatementStarters: [
    'PRAGMA', 'GO', 'DBCC', 'BACKUP', 'BULK', 'CLUSTER', 'PRINT',
    'EXEC', 'ANALYSE', 'VACUUM', 'REINDEX', 'SP_RENAME', 'ACCEPT',
  ],
});

export const TSQL_PROFILE: DialectProfile = makeProfile('tsql', {
  statementHandlers: TSQL_STATEMENT_HANDLERS,
  extraKeywords: [
    'NONCLUSTERED', 'CLUSTERED', 'ROWVERSION', 'UNIQUEIDENTIFIER', 'NTEXT',
    'IMAGE', 'VARBINARY', 'SMALLMONEY', 'DATETIME2', 'DATETIMEOFFSET', 'SMALLDATETIME',
  ],
  extraFunctionKeywords: [
    'GETDATE', 'DATEDIFF', 'GETUTCDATE', 'ISNULL', 'NEWID', 'SCOPE_IDENTITY',
    'OBJECT_ID', 'CHARINDEX', 'PATINDEX', 'STUFF', 'IIF', 'CHOOSE', 'TRY_CONVERT',
    'OPENJSON', 'JSON_VALUE',
    'SYSDATETIME', 'SYSDATETIMEOFFSET', 'EOMONTH', 'CHECKSUM', 'ABS',
  ],
  removeKeywords: [
    'AUTO_INCREMENT', 'BIGSERIAL', 'SERIAL', 'BYTEA', 'CIDR', 'INET', 'MACADDR',
    'JSONB', 'TSQUERY', 'TSVECTOR', 'TIMESTAMPTZ', 'INT2', 'INT4', 'INT8',
    'FLOAT4', 'FLOAT8', 'BOOL', 'ILIKE', 'SIMILAR', 'CONCURRENTLY', 'RETURNING',
    'STRAIGHT_JOIN', 'MEDIUMTEXT', 'LONGTEXT', 'TINYTEXT', 'UNSIGNED', 'ENGINE',
    'CHARSET', 'MODIFY', 'PUBLICATION', 'EXTENSION',
  ],
  removeFunctionKeywords: [
    'GROUP_CONCAT', 'IFNULL', 'IF', 'CURDATE', 'CURTIME', 'DATE_FORMAT', 'DAYOFWEEK',
    'JSONB_AGG', 'JSONB_ARRAY_ELEMENTS', 'JSONB_BUILD_OBJECT', 'JSONB_EACH',
    'JSONB_PATH_QUERY_ARRAY', 'CURRVAL', 'NEXTVAL', 'SETVAL', 'SET_CONFIG',
    'ARRAY_AGG', 'ARRAY_LENGTH', 'UNNEST', 'TO_CHAR', 'DATE_PART', 'DATE_TRUNC',
    'SAFE_CAST', 'CORR',
  ],
  removeClauseKeywords: ['RETURNING', 'LIMIT', 'LATERAL'],
  removeStatementStarters: [
    'PRAGMA', 'DELIMITER', 'FLUSH', 'LOCK', 'UNLOCK', 'ANALYSE', 'VACUUM',
    'REINDEX', 'CLUSTER', 'ACCEPT',
  ],
});

export const DIALECT_PROFILES: Readonly<Record<DialectProfile['name'], DialectProfile>> = Object.freeze({
  ansi: ANSI_PROFILE,
  postgres: POSTGRES_PROFILE,
  mysql: MYSQL_PROFILE,
  tsql: TSQL_PROFILE,
});
