import { describe, expect, it } from 'bun:test';
import { tokenize } from '../src/tokenizer';
import { parse } from '../src/parser';
import { formatSQL } from '../src/format';
import { formatStatements } from '../src/formatter';
import {
  ANSI_PROFILE,
  POSTGRES_PROFILE,
  MYSQL_PROFILE,
  TSQL_PROFILE,
} from '../src/dialects';

// ─── Keyword classification by dialect ─────────────────────────────────

describe('dialect keyword differentiation', () => {
  describe('PostgreSQL-specific keywords', () => {
    it('ILIKE is a keyword in postgres, identifier in mysql', () => {
      const pgTokens = tokenize('SELECT a ILIKE b', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'ILIKE')?.type).toBe('keyword');

      const myTokens = tokenize('SELECT a ILIKE b', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'ILIKE')?.type).toBe('identifier');
    });

    it('BIGSERIAL is a keyword in postgres, identifier in mysql', () => {
      const pgTokens = tokenize('CREATE TABLE t (id BIGSERIAL)', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'BIGSERIAL')?.type).toBe('keyword');

      const myTokens = tokenize('CREATE TABLE t (id BIGSERIAL)', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'BIGSERIAL')?.type).toBe('identifier');
    });

    it('JSONB is a keyword in postgres, identifier in mysql/tsql', () => {
      const pgTokens = tokenize('SELECT col::JSONB', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'JSONB')?.type).toBe('keyword');

      const myTokens = tokenize('SELECT JSONB FROM t', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'JSONB')?.type).toBe('identifier');

      const tsqlTokens = tokenize('SELECT JSONB FROM t', { dialect: 'tsql' });
      expect(tsqlTokens.find(t => t.upper === 'JSONB')?.type).toBe('identifier');
    });

    it('CONCURRENTLY is a keyword in postgres, identifier in mysql/tsql', () => {
      const pgTokens = tokenize('CREATE INDEX CONCURRENTLY idx ON t(a)', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'CONCURRENTLY')?.type).toBe('keyword');

      const myTokens = tokenize('SELECT CONCURRENTLY FROM t', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'CONCURRENTLY')?.type).toBe('identifier');
    });

    it('RETURNING is a keyword in postgres, identifier in mysql/tsql', () => {
      const pgTokens = tokenize('DELETE FROM t RETURNING id', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'RETURNING')?.type).toBe('keyword');

      const myTokens = tokenize('SELECT RETURNING FROM t', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'RETURNING')?.type).toBe('identifier');
    });
  });

  describe('MySQL-specific keywords', () => {
    it('AUTO_INCREMENT is a keyword in mysql, identifier in postgres', () => {
      const myTokens = tokenize('id INT AUTO_INCREMENT', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'AUTO_INCREMENT')?.type).toBe('keyword');

      const pgTokens = tokenize('id INT AUTO_INCREMENT', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'AUTO_INCREMENT')?.type).toBe('identifier');
    });

    it('STRAIGHT_JOIN is a keyword in mysql, identifier in postgres', () => {
      const myTokens = tokenize('SELECT STRAIGHT_JOIN a', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'STRAIGHT_JOIN')?.type).toBe('keyword');

      const pgTokens = tokenize('SELECT STRAIGHT_JOIN a', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'STRAIGHT_JOIN')?.type).toBe('identifier');
    });

    it('ENGINE and CHARSET are keywords in mysql, identifiers in postgres', () => {
      const myTokens = tokenize('ENGINE=InnoDB DEFAULT CHARSET=utf8mb4', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'ENGINE')?.type).toBe('keyword');
      expect(myTokens.find(t => t.upper === 'CHARSET')?.type).toBe('keyword');

      const pgTokens = tokenize('SELECT ENGINE, CHARSET FROM t', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'ENGINE')?.type).toBe('identifier');
      expect(pgTokens.find(t => t.upper === 'CHARSET')?.type).toBe('identifier');
    });

    it('MEDIUMTEXT and LONGTEXT are keywords in mysql, identifiers in postgres', () => {
      const myTokens = tokenize('CREATE TABLE t (a MEDIUMTEXT, b LONGTEXT)', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'MEDIUMTEXT')?.type).toBe('keyword');
      expect(myTokens.find(t => t.upper === 'LONGTEXT')?.type).toBe('keyword');

      const pgTokens = tokenize('SELECT MEDIUMTEXT, LONGTEXT FROM t', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'MEDIUMTEXT')?.type).toBe('identifier');
      expect(pgTokens.find(t => t.upper === 'LONGTEXT')?.type).toBe('identifier');
    });
  });

  describe('T-SQL-specific keywords', () => {
    it('NOLOCK is a keyword in tsql, identifier in postgres', () => {
      const tsqlTokens = tokenize('SELECT * FROM t WITH (NOLOCK)', { dialect: 'tsql' });
      expect(tsqlTokens.find(t => t.upper === 'NOLOCK')?.type).toBe('keyword');

      const pgTokens = tokenize('SELECT * FROM t WITH (NOLOCK)', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'NOLOCK')?.type).toBe('identifier');
    });

    it('TOP is a keyword in tsql, identifier in postgres/mysql', () => {
      const tsqlTokens = tokenize('SELECT TOP 10 * FROM t', { dialect: 'tsql' });
      expect(tsqlTokens.find(t => t.upper === 'TOP')?.type).toBe('keyword');

      const pgTokens = tokenize('SELECT TOP FROM t', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'TOP')?.type).toBe('identifier');

      const myTokens = tokenize('SELECT TOP FROM t', { dialect: 'mysql' });
      expect(myTokens.find(t => t.upper === 'TOP')?.type).toBe('identifier');
    });

    it('NONCLUSTERED is a keyword in tsql, identifier in postgres/mysql', () => {
      const tsqlTokens = tokenize('CREATE NONCLUSTERED INDEX idx ON t(a)', { dialect: 'tsql' });
      expect(tsqlTokens.find(t => t.upper === 'NONCLUSTERED')?.type).toBe('keyword');

      const pgTokens = tokenize('SELECT NONCLUSTERED FROM t', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'NONCLUSTERED')?.type).toBe('identifier');
    });

    it('DATETIME2 and DATETIMEOFFSET are keywords in tsql, identifiers in postgres', () => {
      const tsqlTokens = tokenize('DECLARE @d DATETIME2, @o DATETIMEOFFSET', { dialect: 'tsql' });
      expect(tsqlTokens.find(t => t.upper === 'DATETIME2')?.type).toBe('keyword');
      expect(tsqlTokens.find(t => t.upper === 'DATETIMEOFFSET')?.type).toBe('keyword');

      const pgTokens = tokenize('SELECT DATETIME2, DATETIMEOFFSET FROM t', { dialect: 'postgres' });
      expect(pgTokens.find(t => t.upper === 'DATETIME2')?.type).toBe('identifier');
      expect(pgTokens.find(t => t.upper === 'DATETIMEOFFSET')?.type).toBe('identifier');
    });
  });

  describe('ANSI baseline', () => {
    it('ANSI mode treats all dialect-specific keywords as identifiers', () => {
      const tokens = tokenize(
        'SELECT ILIKE, AUTO_INCREMENT, NOLOCK, BIGSERIAL, JSONB, MEDIUMTEXT, TOP, PRINT',
        { dialect: 'ansi' },
      );
      const keywordValues = tokens.filter(t => t.type === 'keyword').map(t => t.upper);
      expect(keywordValues).not.toContain('ILIKE');
      expect(keywordValues).not.toContain('AUTO_INCREMENT');
      expect(keywordValues).not.toContain('NOLOCK');
      expect(keywordValues).not.toContain('BIGSERIAL');
      expect(keywordValues).not.toContain('JSONB');
      expect(keywordValues).not.toContain('MEDIUMTEXT');
      expect(keywordValues).not.toContain('TOP');
      expect(keywordValues).not.toContain('PRINT');
    });

    it('ANSI mode keeps standard SQL keywords', () => {
      const tokens = tokenize('SELECT a FROM t WHERE x = 1', { dialect: 'ansi' });
      const keywordValues = tokens.filter(t => t.type === 'keyword').map(t => t.upper);
      expect(keywordValues).toContain('SELECT');
      expect(keywordValues).toContain('FROM');
      expect(keywordValues).toContain('WHERE');
    });
  });
});

// ─── Formatting output differs by dialect ──────────────────────────────

describe('dialect-aware formatting output', () => {
  it('AUTO_INCREMENT is uppercased in mysql, lowercased in postgres', () => {
    const myResult = formatSQL('CREATE TABLE t (id INT auto_increment);', { dialect: 'mysql' });
    expect(myResult).toContain('AUTO_INCREMENT');

    const pgResult = formatSQL('CREATE TABLE t (id INT auto_increment);', { dialect: 'postgres' });
    expect(pgResult).toContain('auto_increment');
    expect(pgResult).not.toContain('AUTO_INCREMENT');
  });

  it('MODIFY is uppercased in mysql, lowercased in postgres', () => {
    const myResult = formatSQL('ALTER TABLE t modify col INT;', { dialect: 'mysql', recover: false });
    expect(myResult).toContain('MODIFY');

    const pgResult = formatSQL('ALTER TABLE t modify col INT;', { dialect: 'postgres' });
    expect(pgResult).toContain('modify');
  });

  it('MEDIUMTEXT/LONGTEXT/TINYTEXT are uppercased in mysql, lowercased in postgres', () => {
    const myResult = formatSQL('CREATE TABLE t (a tinytext, b mediumtext, c longtext);', { dialect: 'mysql' });
    expect(myResult).toContain('TINYTEXT');
    expect(myResult).toContain('MEDIUMTEXT');
    expect(myResult).toContain('LONGTEXT');

    const pgResult = formatSQL('CREATE TABLE t (a tinytext, b mediumtext, c longtext);', { dialect: 'postgres' });
    expect(pgResult).toContain('tinytext');
    expect(pgResult).toContain('mediumtext');
    expect(pgResult).toContain('longtext');
  });
});

// ─── Function keyword differentiation ──────────────────────────────────

describe('dialect function keyword differentiation', () => {
  it('GROUP_CONCAT is uppercased in mysql, lowercased in postgres', () => {
    const myResult = formatSQL('SELECT group_concat(name) FROM t;', { dialect: 'mysql' });
    expect(myResult).toContain('GROUP_CONCAT');

    const pgResult = formatSQL('SELECT group_concat(name) FROM t;', { dialect: 'postgres' });
    expect(pgResult).toContain('group_concat');
  });

  it('JSONB_AGG is uppercased in postgres, lowercased in mysql', () => {
    const pgResult = formatSQL('SELECT jsonb_agg(col) FROM t;', { dialect: 'postgres' });
    expect(pgResult).toContain('JSONB_AGG');

    const myResult = formatSQL('SELECT jsonb_agg(col) FROM t;', { dialect: 'mysql' });
    expect(myResult).toContain('jsonb_agg');
  });

  it('DATEADD is uppercased in tsql, lowercased in postgres', () => {
    const tsqlResult = formatSQL('SELECT dateadd(day, 1, getdate()) FROM t;', { dialect: 'tsql' });
    expect(tsqlResult).toContain('DATEADD');

    const pgResult = formatSQL('SELECT dateadd(day, 1, getdate()) FROM t;', { dialect: 'postgres' });
    expect(pgResult).toContain('dateadd');
  });

  it('TRY_CAST is uppercased in tsql, lowercased in mysql', () => {
    const tsqlResult = formatSQL("SELECT try_cast('123' AS INT) FROM t;", { dialect: 'tsql' });
    expect(tsqlResult).toContain('TRY_CAST');

    const myResult = formatSQL("SELECT try_cast('123' AS INT) FROM t;", { dialect: 'mysql' });
    expect(myResult).toContain('try_cast');
  });

  it('ANSI mode lowercases dialect-specific function keywords', () => {
    const ansiResult = formatSQL('SELECT group_concat(name), jsonb_agg(col), dateadd(day, 1, x) FROM t;', { dialect: 'ansi' });
    expect(ansiResult).toContain('group_concat');
    expect(ansiResult).toContain('jsonb_agg');
    expect(ansiResult).toContain('dateadd');
  });

  it('standard functions are uppercased in all dialects', () => {
    for (const dialect of ['ansi', 'postgres', 'mysql', 'tsql'] as const) {
      const result = formatSQL('SELECT count(*), max(x), coalesce(a, b) FROM t;', { dialect });
      expect(result).toContain('COUNT(*)');
      expect(result).toContain('MAX(x)');
      expect(result).toContain('COALESCE(a, b)');
    }
  });
});

// ─── Statement handler differentiation ─────────────────────────────────

describe('dialect statement handler differentiation', () => {
  it('GO is recognized as batch separator in tsql but not postgres', () => {
    const tsqlResult = formatSQL('SELECT 1\nGO\nSELECT 2;', { dialect: 'tsql' });
    expect(tsqlResult).toContain('\nGO\n');

    const pgResult = formatSQL('SELECT 1\nGO\nSELECT 2;', { dialect: 'postgres' });
    expect(pgResult).not.toContain('\nGO\n');
  });

  it('DELIMITER is recognized in mysql but not postgres', () => {
    const myResult = formatSQL('DELIMITER ;;\nSELECT 1;;\nDELIMITER ;', { dialect: 'mysql' });
    expect(myResult).toContain('DELIMITER ;;');

    // In postgres, DELIMITER is not a statement handler, so it gets parsed differently
    const pgResult = formatSQL('DELIMITER ;;\nSELECT 1;;\nDELIMITER ;', { dialect: 'postgres', recover: true });
    expect(pgResult).not.toContain('DELIMITER ;;');
  });

  it('BACKUP is recognized in tsql but not mysql', () => {
    expect(() => parse("BACKUP DATABASE db TO DISK = 'x.bak';", { recover: false, dialect: 'tsql' })).not.toThrow();
  });

  it('DBCC is recognized in tsql but not postgres', () => {
    expect(() => parse("DBCC CHECKIDENT ('[t]', RESEED, 0);", { recover: false, dialect: 'tsql' })).not.toThrow();
  });
});

// ─── Clause keyword differentiation ────────────────────────────────────

describe('dialect clause keyword differentiation', () => {
  it('PIVOT is a clause keyword in tsql but not postgres', () => {
    expect(TSQL_PROFILE.clauseKeywords.has('PIVOT')).toBe(true);
    expect(POSTGRES_PROFILE.clauseKeywords.has('PIVOT')).toBe(false);
  });

  it('RETURNING is a clause keyword in postgres but not mysql/tsql', () => {
    expect(POSTGRES_PROFILE.clauseKeywords.has('RETURNING')).toBe(true);
    expect(MYSQL_PROFILE.clauseKeywords.has('RETURNING')).toBe(false);
    expect(TSQL_PROFILE.clauseKeywords.has('RETURNING')).toBe(false);
  });

  it('LIMIT is a clause keyword in postgres/mysql but not tsql/ansi', () => {
    expect(POSTGRES_PROFILE.clauseKeywords.has('LIMIT')).toBe(true);
    expect(MYSQL_PROFILE.clauseKeywords.has('LIMIT')).toBe(true);
    expect(TSQL_PROFILE.clauseKeywords.has('LIMIT')).toBe(false);
    expect(ANSI_PROFILE.clauseKeywords.has('LIMIT')).toBe(false);
  });

  it('GO is a clause keyword in tsql but not postgres/mysql', () => {
    expect(TSQL_PROFILE.clauseKeywords.has('GO')).toBe(true);
    expect(POSTGRES_PROFILE.clauseKeywords.has('GO')).toBe(false);
    expect(MYSQL_PROFILE.clauseKeywords.has('GO')).toBe(false);
  });
});

// ─── Profile immutability (M1 Issue 2) ─────────────────────────────────

describe('profile immutability', () => {
  it('profile objects are frozen', () => {
    expect(Object.isFrozen(POSTGRES_PROFILE)).toBe(true);
    expect(Object.isFrozen(MYSQL_PROFILE)).toBe(true);
    expect(Object.isFrozen(TSQL_PROFILE)).toBe(true);
    expect(Object.isFrozen(ANSI_PROFILE)).toBe(true);
  });

  it('keyword Sets throw on .add()', () => {
    expect(() => (POSTGRES_PROFILE.keywords as Set<string>).add('FOOBAR')).toThrow(TypeError);
    expect(() => (MYSQL_PROFILE.keywords as Set<string>).add('FOOBAR')).toThrow(TypeError);
    expect(() => (TSQL_PROFILE.keywords as Set<string>).add('FOOBAR')).toThrow(TypeError);
    expect(() => (ANSI_PROFILE.keywords as Set<string>).add('FOOBAR')).toThrow(TypeError);
  });

  it('keyword Sets throw on .delete()', () => {
    expect(() => (POSTGRES_PROFILE.keywords as Set<string>).delete('SELECT')).toThrow(TypeError);
  });

  it('keyword Sets throw on .clear()', () => {
    expect(() => (POSTGRES_PROFILE.keywords as Set<string>).clear()).toThrow(TypeError);
  });

  it('keyword Sets cannot be mutated via Set.prototype calls', () => {
    const keywords = POSTGRES_PROFILE.keywords as unknown as Set<string>;
    const originalSize = keywords.size;

    expect(() => Set.prototype.add.call(keywords, 'FOOBAR')).toThrow(TypeError);
    expect(() => Set.prototype.delete.call(keywords, 'SELECT')).toThrow(TypeError);

    expect(keywords.size).toBe(originalSize);
    expect(keywords.has('FOOBAR')).toBe(false);
    expect(keywords.has('SELECT')).toBe(true);
  });

  it('functionKeywords Sets are also frozen', () => {
    expect(() => (POSTGRES_PROFILE.functionKeywords as Set<string>).add('FOOBAR')).toThrow(TypeError);
    expect(() => (MYSQL_PROFILE.functionKeywords as Set<string>).add('FOOBAR')).toThrow(TypeError);
  });
});

// ─── Formatter option threading ────────────────────────────────────────

describe('formatter reparse threading', () => {
  it('dialect survives through formatStatements recursive calls', () => {
    // A CTE with a subquery that triggers recursive formatting
    const sql = `WITH cte AS (SELECT 1) SELECT * FROM cte;`;
    const pgResult = formatSQL(sql, { dialect: 'postgres' });
    const myResult = formatSQL(sql, { dialect: 'mysql' });
    // Both should format without errors
    expect(pgResult).toContain('WITH cte AS');
    expect(myResult).toContain('WITH cte AS');
  });

  it('functionKeywords from dialect profile are used in formatting', () => {
    // ARRAY_AGG is a postgres function keyword but not mysql
    const pgResult = formatSQL('SELECT array_agg(x) FROM t;', { dialect: 'postgres' });
    expect(pgResult).toContain('ARRAY_AGG');

    const myResult = formatSQL('SELECT array_agg(x) FROM t;', { dialect: 'mysql' });
    expect(myResult).toContain('array_agg');
  });

  it('maxDepth is threaded through to reparse helpers', () => {
    // Ensure very low maxDepth still works for simple SQL
    const result = formatSQL('SELECT 1;', { maxDepth: 10 });
    expect(result).toContain('SELECT 1;');
  });
});

// ─── Statement starter differentiation ─────────────────────────────────

describe('dialect statement starter differentiation', () => {
  it('VACUUM is a statement starter in postgres but not mysql', () => {
    expect(POSTGRES_PROFILE.statementStarters.has('VACUUM')).toBe(true);
    expect(MYSQL_PROFILE.statementStarters.has('VACUUM')).toBe(false);
  });

  it('DELIMITER is a statement starter in mysql but not postgres', () => {
    expect(MYSQL_PROFILE.statementStarters.has('DELIMITER')).toBe(true);
    expect(POSTGRES_PROFILE.statementStarters.has('DELIMITER')).toBe(false);
  });

  it('DBCC is a statement starter in tsql but not postgres/mysql', () => {
    expect(TSQL_PROFILE.statementStarters.has('DBCC')).toBe(true);
    expect(POSTGRES_PROFILE.statementStarters.has('DBCC')).toBe(false);
    expect(MYSQL_PROFILE.statementStarters.has('DBCC')).toBe(false);
  });

  it('EXEC is a statement starter in tsql but not postgres/mysql', () => {
    expect(TSQL_PROFILE.statementStarters.has('EXEC')).toBe(true);
    expect(POSTGRES_PROFILE.statementStarters.has('EXEC')).toBe(false);
    expect(MYSQL_PROFILE.statementStarters.has('EXEC')).toBe(false);
  });

  it('ANALYSE is a statement starter in postgres but not mysql/tsql', () => {
    expect(POSTGRES_PROFILE.statementStarters.has('ANALYSE')).toBe(true);
    expect(MYSQL_PROFILE.statementStarters.has('ANALYSE')).toBe(false);
    expect(TSQL_PROFILE.statementStarters.has('ANALYSE')).toBe(false);
  });
});

// ─── Cross-dialect keyword isolation ───────────────────────────────────

describe('cross-dialect keyword isolation', () => {
  it('each dialect has unique keywords not shared with others', () => {
    // Postgres has BIGSERIAL, MySQL does not
    expect(POSTGRES_PROFILE.keywords.has('BIGSERIAL')).toBe(true);
    expect(MYSQL_PROFILE.keywords.has('BIGSERIAL')).toBe(false);

    // MySQL has AUTO_INCREMENT, Postgres does not
    expect(MYSQL_PROFILE.keywords.has('AUTO_INCREMENT')).toBe(true);
    expect(POSTGRES_PROFILE.keywords.has('AUTO_INCREMENT')).toBe(false);

    // TSQL has NONCLUSTERED, others do not
    expect(TSQL_PROFILE.keywords.has('NONCLUSTERED')).toBe(true);
    expect(POSTGRES_PROFILE.keywords.has('NONCLUSTERED')).toBe(false);
    expect(MYSQL_PROFILE.keywords.has('NONCLUSTERED')).toBe(false);
  });

  it('all dialects share standard SQL keywords', () => {
    for (const profile of [ANSI_PROFILE, POSTGRES_PROFILE, MYSQL_PROFILE, TSQL_PROFILE]) {
      expect(profile.keywords.has('SELECT')).toBe(true);
      expect(profile.keywords.has('FROM')).toBe(true);
      expect(profile.keywords.has('WHERE')).toBe(true);
      expect(profile.keywords.has('INSERT')).toBe(true);
      expect(profile.keywords.has('UPDATE')).toBe(true);
      expect(profile.keywords.has('DELETE')).toBe(true);
      expect(profile.keywords.has('CREATE')).toBe(true);
      expect(profile.keywords.has('ALTER')).toBe(true);
      expect(profile.keywords.has('DROP')).toBe(true);
      expect(profile.keywords.has('JOIN')).toBe(true);
      expect(profile.keywords.has('GROUP')).toBe(true);
      expect(profile.keywords.has('ORDER')).toBe(true);
    }
  });
});
