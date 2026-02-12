import type { DialectName } from '@/lib/holywell';
import {
  PostgreSQL,
  StandardSQL,
  MySQL,
  MSSQL,
  type SQLDialect,
} from '@codemirror/lang-sql';

export const DEFAULT_DIALECT: DialectName = 'postgres';

export const DIALECT_OPTIONS: readonly { value: DialectName; label: string }[] =
  [
    { value: 'postgres', label: 'PostgreSQL' },
    { value: 'ansi', label: 'ANSI SQL' },
    { value: 'mysql', label: 'MySQL' },
    { value: 'tsql', label: 'SQL Server (T-SQL)' },
  ];

export const CODEMIRROR_DIALECTS: Record<DialectName, SQLDialect> = {
  ansi: StandardSQL,
  postgres: PostgreSQL,
  mysql: MySQL,
  tsql: MSSQL,
};
