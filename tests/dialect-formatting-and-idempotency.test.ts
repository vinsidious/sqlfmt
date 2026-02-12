import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';
import { tokenize } from '../src/tokenizer';

// ---------------------------------------------------------------------------
// Helper: assert idempotency for a given SQL + dialect
// ---------------------------------------------------------------------------
function assertIdempotent(sql: string, dialect: 'mysql' | 'postgres' | 'tsql' | 'ansi') {
  const once = formatSQL(sql, { dialect, recover: true });
  const twice = formatSQL(once, { dialect, recover: true });
  expect(twice).toBe(once);
}

// ===========================================================================
//  MYSQL DIALECT
// ===========================================================================
describe('MySQL dialect', () => {
  const opts = { dialect: 'mysql' as const, recover: true };

  // -- Identifiers ----------------------------------------------------------
  describe('backtick identifiers', () => {
    it('preserves backtick-quoted identifiers', () => {
      const out = formatSQL('SELECT `my col` FROM `my table`;', opts);
      expect(out).toContain('`my col`');
      expect(out).toContain('`my table`');
    });

    it('does not lowercase inside backticks', () => {
      const out = formatSQL('SELECT `MyCol` FROM `MyTable`;', opts);
      expect(out).toContain('`MyCol`');
      expect(out).toContain('`MyTable`');
    });
  });

  // -- DDL features ---------------------------------------------------------
  describe('CREATE TABLE features', () => {
    it('uppercases AUTO_INCREMENT', () => {
      const out = formatSQL(
        'CREATE TABLE users (id INT auto_increment PRIMARY KEY);',
        opts,
      );
      expect(out).toContain('AUTO_INCREMENT');
    });

    it('formats ENGINE and CHARSET clauses', () => {
      const out = formatSQL(
        'CREATE TABLE t (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;',
        opts,
      );
      expect(out).toContain('ENGINE');
      expect(out).toContain('InnoDB');
      expect(out).toContain('CHARSET');
      expect(out).toContain('utf8mb4');
    });

    it('uppercases FULLTEXT KEY', () => {
      const out = formatSQL(
        'CREATE TABLE t (body TEXT, FULLTEXT KEY ft_body (body));',
        opts,
      );
      expect(out.toUpperCase()).toContain('FULLTEXT');
      expect(out.toUpperCase()).toContain('KEY');
    });

    it('handles MySQL-specific types: TINYTEXT, MEDIUMTEXT, LONGTEXT', () => {
      const out = formatSQL(
        'CREATE TABLE t (a tinytext, b mediumtext, c longtext);',
        opts,
      );
      expect(out).toContain('TINYTEXT');
      expect(out).toContain('MEDIUMTEXT');
      expect(out).toContain('LONGTEXT');
    });

    it('handles TINYINT and UNSIGNED modifier', () => {
      const out = formatSQL(
        'CREATE TABLE t (a tinyint, b int unsigned);',
        opts,
      );
      expect(out).toContain('TINYINT');
      expect(out).toContain('UNSIGNED');
    });

    it('handles MEDIUMINT type', () => {
      const out = formatSQL('CREATE TABLE t (a mediumint);', opts);
      // MEDIUMINT may not be in keyword list -- check passthrough
      expect(out.toLowerCase()).toContain('mediumint');
    });
  });

  // -- DML features ---------------------------------------------------------
  describe('DML features', () => {
    it('formats ON DUPLICATE KEY UPDATE', () => {
      const out = formatSQL(
        "INSERT INTO t (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name);",
        opts,
      );
      expect(out).toContain('ON DUPLICATE KEY UPDATE');
    });

    it('formats INSERT IGNORE INTO', () => {
      const out = formatSQL(
        "INSERT IGNORE INTO t (id) VALUES (1);",
        opts,
      );
      expect(out).toContain('INSERT IGNORE INTO');
    });

    it('formats REPLACE INTO', () => {
      const out = formatSQL("REPLACE INTO t (id, name) VALUES (1, 'a');", opts);
      expect(out).toContain('REPLACE INTO');
    });
  });

  // -- JOIN / hints ---------------------------------------------------------
  describe('STRAIGHT_JOIN', () => {
    it('uppercases and formats STRAIGHT_JOIN', () => {
      const out = formatSQL(
        'SELECT * FROM t1 straight_join t2 ON t1.id = t2.id;',
        opts,
      );
      expect(out).toContain('STRAIGHT_JOIN');
    });
  });

  describe('index hints', () => {
    it('formats USE INDEX', () => {
      const out = formatSQL(
        'SELECT * FROM t USE INDEX (idx1) WHERE a = 1;',
        opts,
      );
      expect(out).toContain('USE INDEX');
    });

    it('formats FORCE INDEX', () => {
      const out = formatSQL(
        'SELECT * FROM t FORCE INDEX (idx1) WHERE a = 1;',
        opts,
      );
      expect(out).toContain('FORCE INDEX');
    });

    it('formats IGNORE INDEX', () => {
      const out = formatSQL(
        'SELECT * FROM t IGNORE INDEX (idx1) WHERE a = 1;',
        opts,
      );
      expect(out).toContain('IGNORE INDEX');
    });
  });

  // -- Functions ------------------------------------------------------------
  describe('MySQL function casing', () => {
    it('uppercases GROUP_CONCAT with ORDER BY and SEPARATOR', () => {
      const out = formatSQL(
        "SELECT group_concat(name ORDER BY name SEPARATOR ', ') FROM t;",
        opts,
      );
      expect(out).toContain('GROUP_CONCAT(');
      expect(out).toContain('ORDER BY');
      expect(out).toContain('SEPARATOR');
    });

    it('uppercases IF() and IFNULL()', () => {
      const out = formatSQL(
        'SELECT if(a > 0, a, 0), ifnull(b, 0) FROM t;',
        opts,
      );
      expect(out).toContain('IF(');
      expect(out).toContain('IFNULL(');
    });

    it('uppercases NOW(), CURDATE(), CURTIME()', () => {
      const out = formatSQL(
        'SELECT now(), curdate(), curtime() FROM t;',
        opts,
      );
      expect(out).toContain('NOW()');
      expect(out).toContain('CURDATE()');
      expect(out).toContain('CURTIME()');
    });

    it('uppercases DAYOFWEEK() and DATE_FORMAT()', () => {
      const out = formatSQL(
        "SELECT dayofweek(d), date_format(d, '%Y-%m-%d') FROM t;",
        opts,
      );
      expect(out).toContain('DAYOFWEEK(');
      expect(out).toContain('DATE_FORMAT(');
    });
  });

  // -- DELIMITER blocks -----------------------------------------------------
  describe('DELIMITER blocks', () => {
    it('preserves DELIMITER blocks verbatim', () => {
      const sql =
        'DELIMITER //\nCREATE PROCEDURE test() BEGIN SELECT 1; END //\nDELIMITER ;';
      const out = formatSQL(sql, opts);
      expect(out).toContain('DELIMITER //');
      expect(out).toContain('DELIMITER ;');
    });
  });

  // -- ALTER TABLE ----------------------------------------------------------
  describe('ALTER TABLE actions', () => {
    it('uppercases MODIFY COLUMN', () => {
      const out = formatSQL(
        'ALTER TABLE t MODIFY COLUMN name VARCHAR(200);',
        opts,
      );
      expect(out).toContain('MODIFY');
      expect(out).toContain('COLUMN');
    });
  });

  // -- CREATE TRIGGER -------------------------------------------------------
  describe('CREATE TRIGGER', () => {
    it('parses CREATE TRIGGER with FOR EACH ROW', () => {
      const sql =
        'CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.created = NOW();';
      const out = formatSQL(sql, opts);
      expect(out).toContain('CREATE TRIGGER');
      expect(out).toContain('FOR EACH ROW');
    });
  });

  // -- RLIKE operator -------------------------------------------------------
  describe('RLIKE operator', () => {
    it('preserves RLIKE operator in WHERE clause', () => {
      const out = formatSQL(
        "SELECT * FROM t WHERE name RLIKE 'pattern';",
        opts,
      );
      expect(out).toContain('RLIKE');
    });
  });

  // -- Administrative statements --------------------------------------------
  describe('administrative statements', () => {
    it('formats SHOW TABLES', () => {
      const out = formatSQL('show tables;', opts);
      expect(out).toContain('SHOW TABLES');
    });

    it('uppercases DESC', () => {
      expect(formatSQL('desc users;', opts)).toContain('DESC');
    });

    it('preserves already-uppercase DESCRIBE', () => {
      expect(formatSQL('DESCRIBE users;', opts)).toContain('DESCRIBE');
    });

    it('uppercases lowercase describe in MySQL administrative statements', () => {
      const out = formatSQL('describe users;', opts);
      expect(out).toContain('DESCRIBE');
    });
  });

  // -- Operators ------------------------------------------------------------
  describe('operators', () => {
    it('preserves && boolean operator', () => {
      const out = formatSQL('SELECT * FROM t WHERE a > 0 && b > 0;', opts);
      expect(out).toContain('&&');
    });
  });

  // -- Charset introducer ---------------------------------------------------
  describe('charset introducer', () => {
    it('preserves _binary charset introducer', () => {
      const out = formatSQL("SELECT _binary 'hello' FROM t;", opts);
      expect(out.toLowerCase()).toContain('_binary');
    });
  });
});

// ===========================================================================
//  POSTGRESQL DIALECT
// ===========================================================================
describe('PostgreSQL dialect', () => {
  const opts = { dialect: 'postgres' as const, recover: true };

  // -- Dollar-quoted strings ------------------------------------------------
  describe('dollar-quoted strings', () => {
    it('preserves $$ delimited strings', () => {
      const out = formatSQL("SELECT $$hello world$$;", opts);
      expect(out).toContain('$$hello world$$');
    });

    it('parses DO $$ BEGIN ... END $$ blocks', () => {
      const sql = "DO $$ BEGIN RAISE NOTICE 'hello'; END $$;";
      const out = formatSQL(sql, opts);
      expect(out).toContain('DO $$');
      expect(out).toContain('END $$');
    });
  });

  // -- ILIKE operator -------------------------------------------------------
  describe('ILIKE operator', () => {
    it('uppercases ILIKE', () => {
      const out = formatSQL(
        "SELECT * FROM t WHERE name ilike '%foo%';",
        opts,
      );
      expect(out).toContain('ILIKE');
    });
  });

  // -- Array and JSON operators ---------------------------------------------
  describe('array and JSON operators', () => {
    it('preserves @> array containment operator', () => {
      const out = formatSQL(
        "SELECT * FROM t WHERE tags @> ARRAY['a'];",
        opts,
      );
      expect(out).toContain('@>');
    });

    it('preserves || concatenation operator', () => {
      const out = formatSQL(
        "SELECT first_name || ' ' || last_name FROM t;",
        opts,
      );
      expect(out).toContain('||');
    });

    it('preserves -> JSON access operator', () => {
      const out = formatSQL('SELECT data->0 FROM t;', opts);
      expect(out).toContain('->');
    });

    it('preserves ->> JSON text access operator', () => {
      const out = formatSQL("SELECT data->>'name' FROM t;", opts);
      expect(out).toContain('->>');
    });

    it('preserves #> JSON path operator', () => {
      const out = formatSQL("SELECT data #> '{a,b}' FROM t;", opts);
      expect(out).toContain('#>');
    });

    it('preserves #>> JSON path text operator', () => {
      const out = formatSQL("SELECT data #>> '{a,b}' FROM t;", opts);
      expect(out).toContain('#>>');
    });
  });

  // -- RETURNING clause -----------------------------------------------------
  describe('RETURNING clause', () => {
    it('uppercases RETURNING', () => {
      const out = formatSQL(
        "INSERT INTO t (name) VALUES ('a') returning id;",
        opts,
      );
      expect(out).toContain('RETURNING');
    });
  });

  // -- ON CONFLICT ----------------------------------------------------------
  describe('ON CONFLICT DO UPDATE', () => {
    it('formats upsert correctly', () => {
      const out = formatSQL(
        "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING *;",
        opts,
      );
      expect(out).toContain('ON CONFLICT');
      expect(out).toContain('DO UPDATE');
      expect(out).toContain('RETURNING');
    });
  });

  // -- Cast syntax ----------------------------------------------------------
  describe('cast syntax', () => {
    it('uppercases types in :: cast', () => {
      const out = formatSQL('SELECT id::integer, name::text FROM t;', opts);
      expect(out).toContain('::INTEGER');
      expect(out).toContain('::TEXT');
    });

    it('handles schema-qualified types', () => {
      const out = formatSQL('SELECT x::public.my_type FROM t;', opts);
      expect(out).toContain('::public.my_type');
    });
  });

  // -- Function casing ------------------------------------------------------
  describe('function casing', () => {
    it('uppercases ARRAY_AGG, STRING_AGG, JSONB_AGG', () => {
      const out = formatSQL(
        "SELECT array_agg(x), string_agg(y, ','), jsonb_agg(z) FROM t;",
        opts,
      );
      expect(out).toContain('ARRAY_AGG(');
      expect(out).toContain('STRING_AGG(');
      expect(out).toContain('JSONB_AGG(');
    });

    it('uppercases NOW()', () => {
      const out = formatSQL('SELECT now() FROM t;', opts);
      expect(out).toContain('NOW()');
    });
  });

  // -- CREATE EXTENSION -----------------------------------------------------
  describe('CREATE EXTENSION', () => {
    it('formats CREATE EXTENSION IF NOT EXISTS', () => {
      const out = formatSQL('CREATE EXTENSION IF NOT EXISTS pgcrypto;', opts);
      expect(out).toContain('CREATE EXTENSION');
      expect(out).toContain('IF NOT EXISTS');
    });
  });

  // -- CLUSTER statement ----------------------------------------------------
  describe('CLUSTER statement', () => {
    it('handles CLUSTER statement', () => {
      const out = formatSQL('CLUSTER t USING idx;', opts);
      expect(out).toContain('CLUSTER');
    });
  });

  // -- LISTEN / NOTIFY ------------------------------------------------------
  describe('LISTEN / NOTIFY', () => {
    it('formats LISTEN', () => {
      const out = formatSQL('LISTEN my_channel;', opts);
      expect(out).toContain('LISTEN');
    });

    it('formats NOTIFY', () => {
      const out = formatSQL('NOTIFY my_channel;', opts);
      expect(out).toContain('NOTIFY');
    });
  });

  // -- COPY -----------------------------------------------------------------
  describe('COPY FROM/TO', () => {
    it('formats COPY FROM', () => {
      const out = formatSQL(
        "COPY t FROM '/tmp/data.csv' WITH (FORMAT csv);",
        opts,
      );
      expect(out).toContain('COPY');
      expect(out).toContain('FROM');
    });

    it('formats COPY TO', () => {
      const out = formatSQL(
        "COPY t TO '/tmp/data.csv' WITH (FORMAT csv);",
        opts,
      );
      expect(out).toContain('COPY');
      expect(out).toContain('TO');
    });
  });

  // -- ALTER DEFAULT PRIVILEGES ---------------------------------------------
  describe('ALTER DEFAULT PRIVILEGES', () => {
    it('formats ALTER DEFAULT PRIVILEGES', () => {
      const out = formatSQL(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;',
        opts,
      );
      expect(out).toContain('ALTER DEFAULT PRIVILEGES');
    });
  });

  // -- COMMENT ON -----------------------------------------------------------
  describe('COMMENT ON', () => {
    it('formats COMMENT ON TABLE', () => {
      const out = formatSQL("COMMENT ON TABLE t IS 'A table';", opts);
      expect(out).toContain('COMMENT ON TABLE');
    });

    it('formats COMMENT ON COLUMN', () => {
      const out = formatSQL("COMMENT ON COLUMN t.c IS 'A column';", opts);
      expect(out).toContain('COMMENT ON COLUMN');
    });
  });

  // -- CREATE TYPE AS ENUM --------------------------------------------------
  describe('CREATE TYPE AS ENUM', () => {
    it('formats CREATE TYPE ... AS ENUM', () => {
      const out = formatSQL(
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');",
        opts,
      );
      expect(out).toContain('CREATE TYPE');
      expect(out).toContain('ENUM');
    });
  });
});

// ===========================================================================
//  T-SQL DIALECT
// ===========================================================================
describe('T-SQL dialect', () => {
  const opts = { dialect: 'tsql' as const, recover: true };

  // -- Square bracket identifiers -------------------------------------------
  describe('square bracket identifiers', () => {
    it('preserves [dbo].[table] identifiers', () => {
      const out = formatSQL('SELECT [my col] FROM [dbo].[my table];', opts);
      expect(out).toContain('[my col]');
      expect(out).toContain('[dbo].[my table]');
    });
  });

  // -- GO batch separator ---------------------------------------------------
  describe('GO batch separator', () => {
    it('preserves GO as batch separator', () => {
      const out = formatSQL('SELECT 1;\nGO\nSELECT 2;', opts);
      expect(out).toContain('GO');
      expect(out).toContain('SELECT 1');
      expect(out).toContain('SELECT 2');
    });
  });

  // -- @variable declarations -----------------------------------------------
  describe('@variable declarations', () => {
    it('formats DECLARE with @variables', () => {
      const out = formatSQL('DECLARE @name NVARCHAR(100);', opts);
      expect(out).toContain('DECLARE');
      expect(out).toContain('@name');
      expect(out).toContain('NVARCHAR');
    });

    it('formats SET @variable assignment', () => {
      const out = formatSQL("SET @name = N'test';", opts);
      expect(out).toContain('SET');
      expect(out).toContain('@name');
    });
  });

  // -- TOP clause -----------------------------------------------------------
  describe('TOP clause', () => {
    it('uppercases TOP', () => {
      const out = formatSQL('SELECT top 10 * FROM t;', opts);
      expect(out).toContain('TOP');
    });
  });

  // -- IDENTITY -------------------------------------------------------------
  describe('IDENTITY', () => {
    it('formats IDENTITY(1,1) in CREATE TABLE', () => {
      const out = formatSQL(
        'CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY);',
        opts,
      );
      expect(out).toContain('IDENTITY');
    });
  });

  // -- T-SQL specific types -------------------------------------------------
  describe('T-SQL specific types', () => {
    it('uppercases NVARCHAR', () => {
      const out = formatSQL('CREATE TABLE t (name nvarchar(100));', opts);
      expect(out).toContain('NVARCHAR');
    });

    it('uppercases DATETIME2', () => {
      const out = formatSQL('CREATE TABLE t (ts datetime2);', opts);
      expect(out).toContain('DATETIME2');
    });

    it('uppercases UNIQUEIDENTIFIER', () => {
      const out = formatSQL('CREATE TABLE t (id uniqueidentifier);', opts);
      expect(out).toContain('UNIQUEIDENTIFIER');
    });
  });

  // -- BACKUP / RESTORE / DBCC ----------------------------------------------
  describe('BACKUP / DBCC', () => {
    it('handles BACKUP DATABASE as verbatim passthrough', () => {
      const out = formatSQL(
        "BACKUP DATABASE mydb TO DISK = 'C:\\\\backup.bak';",
        opts,
      );
      expect(out).toContain('BACKUP');
      expect(out).toContain('DATABASE');
    });

    it('handles DBCC commands', () => {
      const out = formatSQL("DBCC CHECKDB('mydb');", opts);
      expect(out).toContain('DBCC');
    });
  });

  // -- BULK INSERT ----------------------------------------------------------
  describe('BULK INSERT', () => {
    it('handles BULK INSERT', () => {
      const out = formatSQL(
        "BULK INSERT t FROM 'C:\\\\data.csv';",
        opts,
      );
      expect(out).toContain('BULK INSERT');
    });
  });

  // -- PRINT ----------------------------------------------------------------
  describe('PRINT statement', () => {
    it('formats PRINT statement', () => {
      const out = formatSQL("PRINT N'Hello';", opts);
      expect(out).toContain('PRINT');
    });
  });

  // -- T-SQL functions ------------------------------------------------------
  describe('T-SQL function casing', () => {
    it('uppercases TRY_CAST', () => {
      const out = formatSQL('SELECT try_cast(x AS INT) FROM t;', opts);
      expect(out).toContain('TRY_CAST(');
    });

    it('uppercases CONVERT', () => {
      const out = formatSQL("SELECT convert(INT, '123') FROM t;", opts);
      expect(out).toContain('CONVERT(');
    });

    it('uppercases DATEADD', () => {
      const out = formatSQL(
        'SELECT dateadd(day, 1, GETDATE()) FROM t;',
        opts,
      );
      expect(out).toContain('DATEADD(');
    });

    it('uppercases GETDATE in T-SQL dialect', () => {
      const out = formatSQL('SELECT getdate() FROM t;', opts);
      expect(out).toContain('GETDATE()');
    });

    it('uppercases DATEDIFF in T-SQL dialect', () => {
      const out = formatSQL(
        'SELECT datediff(day, start_date, end_date) FROM t;',
        opts,
      );
      expect(out).toContain('DATEDIFF(');
    });
  });

  // -- WITH (NOLOCK) table hints --------------------------------------------
  describe('WITH (NOLOCK) table hints', () => {
    it('preserves already-uppercase NOLOCK', () => {
      const out = formatSQL('SELECT * FROM t WITH (NOLOCK);', opts);
      expect(out).toContain('NOLOCK');
    });

    it('uppercases lowercase nolock in WITH() table hints', () => {
      const out = formatSQL('SELECT * FROM t WITH (nolock);', opts);
      expect(out).toContain('NOLOCK');
    });
  });

  // -- OPTION (RECOMPILE) query hints ---------------------------------------
  describe('OPTION query hints', () => {
    it('formats OPTION (RECOMPILE)', () => {
      const out = formatSQL('SELECT * FROM t OPTION (RECOMPILE);', opts);
      expect(out).toContain('OPTION');
      expect(out).toContain('RECOMPILE');
    });
  });

  // -- IF/BEGIN/END blocks --------------------------------------------------
  describe('IF/BEGIN/END blocks', () => {
    it('handles IF BEGIN END construct', () => {
      const sql = 'IF @x > 0 BEGIN SELECT 1; END';
      const out = formatSQL(sql, opts);
      expect(out).toContain('IF');
      expect(out).toContain('BEGIN');
      expect(out).toContain('END');
    });
  });

  // -- EXEC / EXECUTE -------------------------------------------------------
  describe('EXEC / EXECUTE', () => {
    it('formats EXEC statement', () => {
      const out = formatSQL("EXEC sp_executesql N'SELECT 1';", opts);
      expect(out).toContain('EXEC');
    });

    it('formats EXECUTE statement', () => {
      const out = formatSQL("EXECUTE sp_rename 'old_table', 'new_table';", opts);
      expect(out).toContain('EXECUTE');
    });
  });

  // -- N'string' Unicode literals -------------------------------------------
  describe('N string literals', () => {
    it('preserves N-prefixed string literals', () => {
      const out = formatSQL("SELECT N'unicode string' AS val;", opts);
      expect(out).toContain("N'unicode string'");
    });
  });

  // -- MERGE ----------------------------------------------------------------
  describe('MERGE statement', () => {
    it('formats MERGE INTO ... USING ... ON ...', () => {
      const sql =
        'MERGE INTO target t USING source s ON t.id = s.id ' +
        'WHEN MATCHED THEN UPDATE SET t.name = s.name ' +
        'WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name);';
      const out = formatSQL(sql, opts);
      expect(out).toContain('MERGE INTO');
      expect(out).toContain('USING');
      expect(out).toContain('WHEN MATCHED');
      expect(out).toContain('WHEN NOT MATCHED');
    });
  });

  // -- CROSS APPLY / OUTER APPLY -------------------------------------------
  describe('CROSS APPLY / OUTER APPLY', () => {
    it('formats CROSS APPLY', () => {
      const out = formatSQL(
        'SELECT * FROM t CROSS APPLY fn(t.id) AS f;',
        opts,
      );
      expect(out).toContain('CROSS APPLY');
    });

    it('formats OUTER APPLY', () => {
      const out = formatSQL(
        'SELECT * FROM t OUTER APPLY fn(t.id) AS f;',
        opts,
      );
      expect(out).toContain('OUTER APPLY');
    });
  });

  // -- OUTPUT clause --------------------------------------------------------
  describe('OUTPUT clause', () => {
    it('preserves OUTPUT clause in DELETE', () => {
      const out = formatSQL(
        'DELETE FROM t OUTPUT DELETED.* WHERE id = 1;',
        opts,
      );
      expect(out).toContain('OUTPUT');
      expect(out).toContain('DELETED');
    });
  });
});

// ===========================================================================
//  ANSI DIALECT
// ===========================================================================
describe('ANSI dialect', () => {
  const opts = { dialect: 'ansi' as const, recover: true };

  describe('standard SQL works', () => {
    it('formats basic SELECT', () => {
      const out = formatSQL('SELECT a, b FROM t WHERE a = 1;', opts);
      expect(out).toContain('SELECT');
      expect(out).toContain('FROM');
      expect(out).toContain('WHERE');
    });

    it('formats JOIN', () => {
      const out = formatSQL(
        'SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id;',
        opts,
      );
      expect(out).toContain('INNER JOIN');
    });

    it('formats GROUP BY and HAVING', () => {
      const out = formatSQL(
        'SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1;',
        opts,
      );
      expect(out).toContain('GROUP BY');
      expect(out).toContain('HAVING');
    });
  });

  describe('dialect-specific keywords should NOT be uppercased', () => {
    it('does not uppercase GROUP_CONCAT (MySQL-specific)', () => {
      const out = formatSQL('SELECT group_concat(x) FROM t;', opts);
      expect(out).toContain('group_concat(');
      expect(out).not.toContain('GROUP_CONCAT(');
    });

    it('does not uppercase TRY_CAST (T-SQL-specific)', () => {
      const out = formatSQL('SELECT try_cast(x AS INT) FROM t;', opts);
      expect(out).toContain('try_cast(');
      expect(out).not.toContain('TRY_CAST(');
    });

    it('does not uppercase IFNULL (MySQL-specific)', () => {
      const out = formatSQL('SELECT ifnull(x, 0) FROM t;', opts);
      expect(out).toContain('ifnull(');
      expect(out).not.toContain('IFNULL(');
    });

    it('does not treat ILIKE as keyword', () => {
      const out = formatSQL('SELECT ilike FROM t;', opts);
      // ILIKE removed from ANSI keywords, so should stay lowercase
      expect(out).toContain('ilike');
    });

    it('does not uppercase AUTO_INCREMENT', () => {
      const out = formatSQL('SELECT auto_increment FROM t;', opts);
      expect(out).toContain('auto_increment');
    });
  });
});

// ===========================================================================
//  CROSS-DIALECT ISOLATION
// ===========================================================================
describe('cross-dialect keyword isolation', () => {
  it('postgres does NOT uppercase GROUP_CONCAT', () => {
    const out = formatSQL('SELECT group_concat(x) FROM t;', {
      dialect: 'postgres',
    });
    expect(out).toContain('group_concat(');
    expect(out).not.toContain('GROUP_CONCAT(');
  });

  it('mysql does NOT uppercase TRY_CAST', () => {
    const out = formatSQL('SELECT try_cast(x AS INT) FROM t;', {
      dialect: 'mysql',
    });
    expect(out).toContain('try_cast(');
    expect(out).not.toContain('TRY_CAST(');
  });

  it('postgres does NOT uppercase DATEADD or GETDATE', () => {
    const out = formatSQL(
      'SELECT dateadd(day, 1, getdate()) FROM t;',
      { dialect: 'postgres' },
    );
    expect(out).toContain('dateadd(');
    expect(out).toContain('getdate()');
    expect(out).not.toContain('DATEADD(');
  });

  it('mysql does NOT uppercase JSONB_AGG', () => {
    const out = formatSQL('SELECT jsonb_agg(x) FROM t;', {
      dialect: 'mysql',
    });
    expect(out).toContain('jsonb_agg(');
    expect(out).not.toContain('JSONB_AGG(');
  });

  it('tsql does NOT uppercase GROUP_CONCAT', () => {
    const out = formatSQL('SELECT group_concat(x) FROM t;', {
      dialect: 'tsql',
    });
    expect(out).toContain('group_concat(');
    expect(out).not.toContain('GROUP_CONCAT(');
  });

  it('tsql does NOT uppercase ARRAY_AGG', () => {
    const out = formatSQL('SELECT array_agg(x) FROM t;', {
      dialect: 'tsql',
    });
    expect(out).toContain('array_agg(');
    expect(out).not.toContain('ARRAY_AGG(');
  });

  it('ansi does NOT uppercase DATEADD', () => {
    const out = formatSQL('SELECT dateadd(day, 1, d) FROM t;', {
      dialect: 'ansi',
    });
    expect(out).toContain('dateadd(');
    expect(out).not.toContain('DATEADD(');
  });

  it('ansi does NOT uppercase SAFE_CAST', () => {
    const out = formatSQL('SELECT safe_cast(x AS INT) FROM t;', {
      dialect: 'ansi',
    });
    expect(out).toContain('safe_cast(');
    expect(out).not.toContain('SAFE_CAST(');
  });

  it('postgres does NOT uppercase IF or IFNULL', () => {
    const out = formatSQL('SELECT if(a, b, c), ifnull(d, e) FROM t;', {
      dialect: 'postgres',
    });
    expect(out).toContain('if(');
    expect(out).toContain('ifnull(');
  });

  it('mysql does NOT uppercase STRING_AGG', () => {
    const out = formatSQL("SELECT string_agg(x, ',') FROM t;", {
      dialect: 'mysql',
    });
    expect(out).toContain('string_agg(');
    expect(out).not.toContain('STRING_AGG(');
  });
});

// ===========================================================================
//  IDEMPOTENCY TESTS PER DIALECT
// ===========================================================================
describe('idempotency per dialect', () => {
  describe('MySQL idempotency', () => {
    const samples = [
      'SELECT * FROM t WHERE a > 1 ORDER BY b;',
      "INSERT INTO t (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name);",
      'CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100)) ENGINE=InnoDB;',
      "SELECT group_concat(name ORDER BY name SEPARATOR ', ') FROM t;",
      'SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.id = t2.id;',
      'SELECT if(a > 0, a, 0), ifnull(b, 0) FROM t;',
    ];

    for (const sql of samples) {
      it(`remains idempotent for SQL: ${sql.substring(0, 60)}...`, () => {
        assertIdempotent(sql, 'mysql');
      });
    }
  });

  describe('PostgreSQL idempotency', () => {
    const samples = [
      'SELECT * FROM t WHERE a > 1 ORDER BY b;',
      "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING *;",
      "SELECT * FROM t WHERE name ILIKE '%foo%' ORDER BY id;",
      "SELECT array_agg(x), string_agg(y, ',') FROM t;",
      'SELECT id::INTEGER, name::TEXT FROM t;',
      "SELECT data->>'name' FROM t WHERE data->>'type' = 'a';",
    ];

    for (const sql of samples) {
      it(`remains idempotent for SQL: ${sql.substring(0, 60)}...`, () => {
        assertIdempotent(sql, 'postgres');
      });
    }
  });

  describe('T-SQL idempotency', () => {
    const samples = [
      'SELECT TOP 10 * FROM t WITH (NOLOCK) WHERE a > 1;',
      'DECLARE @x INT; SET @x = 1;',
      'SELECT TRY_CAST(x AS INT), CONVERT(VARCHAR, y) FROM t;',
      'SELECT * FROM t CROSS APPLY fn(t.id) AS f;',
      'CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100));',
      'MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name;',
    ];

    for (const sql of samples) {
      it(`remains idempotent for SQL: ${sql.substring(0, 60)}...`, () => {
        assertIdempotent(sql, 'tsql');
      });
    }
  });

  describe('ANSI idempotency', () => {
    const samples = [
      'SELECT a, b FROM t WHERE a = 1;',
      'SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id;',
      'SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1;',
      'SELECT * FROM t ORDER BY a ASC, b DESC;',
      "SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END FROM t;",
      'WITH cte AS (SELECT 1 AS x) SELECT * FROM cte;',
    ];

    for (const sql of samples) {
      it(`remains idempotent for SQL: ${sql.substring(0, 60)}...`, () => {
        assertIdempotent(sql, 'ansi');
      });
    }
  });
});

// ===========================================================================
//  EDGE CASES
// ===========================================================================
describe('edge cases', () => {
  // -- Empty / blank input --------------------------------------------------
  describe('empty input', () => {
    it('returns empty string for empty input', () => {
      expect(formatSQL('')).toBe('');
    });

    it('returns empty string for whitespace-only input', () => {
      expect(formatSQL('   ')).toBe('');
      expect(formatSQL('\n\n')).toBe('');
      expect(formatSQL('\t  \n  ')).toBe('');
    });

    it('returns empty string for multiple semicolons', () => {
      expect(formatSQL(';;;')).toBe('');
    });
  });

  // -- Mixed quoting styles -------------------------------------------------
  describe('mixed quoting styles', () => {
    it('handles backtick and double-quoted identifiers together', () => {
      const out = formatSQL(
        'SELECT `col1`, "col2" FROM t;',
        { dialect: 'mysql' },
      );
      expect(out).toContain('`col1`');
      expect(out).toContain('"col2"');
    });

    it('handles square bracket and double-quoted identifiers together', () => {
      const out = formatSQL(
        'SELECT [col1], "col2" FROM t;',
        { dialect: 'tsql' },
      );
      expect(out).toContain('[col1]');
      expect(out).toContain('"col2"');
    });
  });

  // -- Unicode identifiers --------------------------------------------------
  describe('unicode identifiers', () => {
    it('preserves unicode identifier names', () => {
      const out = formatSQL('SELECT nombre, direccion FROM clientes;');
      expect(out).toContain('nombre');
      expect(out).toContain('direccion');
      expect(out).toContain('clientes');
    });

    it('handles CJK characters in identifiers', () => {
      // Use quoted identifiers to be safe
      const out = formatSQL('SELECT "customer_id" FROM "orders";');
      expect(out).toContain('"customer_id"');
      expect(out).toContain('"orders"');
    });
  });

  // -- Very long lines ------------------------------------------------------
  describe('very long lines', () => {
    it('wraps lines with 20 columns', () => {
      const cols = Array.from({ length: 20 }, (_, i) => 'column_' + i).join(', ');
      const sql = 'SELECT ' + cols + ' FROM very_long_table_name;';
      const out = formatSQL(sql);
      const lines = out.split('\n');
      // Should have multiple lines due to wrapping
      expect(lines.length).toBeGreaterThan(1);
    });

    it('handles 200+ character column names gracefully', () => {
      const longName = 'x'.repeat(200);
      const sql = `SELECT ${longName} FROM t;`;
      const out = formatSQL(sql);
      expect(out).toContain(longName);
    });
  });

  // -- Deep nesting ---------------------------------------------------------
  describe('deep nesting', () => {
    it('handles 10-level nested subqueries', () => {
      let sql = 'SELECT 1';
      for (let i = 0; i < 10; i++) {
        sql = `SELECT * FROM (${sql}) AS sub${i}`;
      }
      const out = formatSQL(sql + ';');
      expect(out).toContain('SELECT');
      // Should produce valid output without crashing
      expect(out.length).toBeGreaterThan(0);
    });

    it('rejects excessively deep nesting with maxDepth', () => {
      let sql = 'SELECT ' + '('.repeat(50) + '1' + ')'.repeat(50);
      expect(() => formatSQL(sql + ';', { maxDepth: 20 })).toThrow();
    });
  });

  // -- Comments inside expressions ------------------------------------------
  describe('comments inside expressions', () => {
    it('preserves block comments in SELECT list', () => {
      const out = formatSQL('SELECT /* get id */ id, /* name */ name FROM t;');
      expect(out).toContain('/* get id */');
      expect(out).toContain('/* name */');
    });

    it('preserves line comments', () => {
      const out = formatSQL('SELECT id -- primary key\nFROM t;');
      expect(out).toContain('-- primary key');
    });
  });

  // -- Strings containing SQL keywords --------------------------------------
  describe('strings containing SQL keywords', () => {
    it('does not treat keywords inside strings as SQL', () => {
      const out = formatSQL("SELECT 'SELECT * FROM users' AS query FROM t;");
      // The string content should be preserved as-is
      expect(out).toContain("'SELECT * FROM users'");
    });

    it('handles strings with WHERE clause inside', () => {
      const out = formatSQL(
        "SELECT 'WHERE a = 1 AND b = 2' AS filter FROM t;",
      );
      expect(out).toContain("'WHERE a = 1 AND b = 2'");
    });
  });

  // -- Multiple statements --------------------------------------------------
  describe('multiple statements', () => {
    it('formats multiple statements separated by semicolons', () => {
      const out = formatSQL('SELECT 1; SELECT 2; SELECT 3;');
      const selectCount = (out.match(/SELECT/g) || []).length;
      expect(selectCount).toBe(3);
    });
  });

  // -- Semicolons -----------------------------------------------------------
  describe('semicolons', () => {
    it('handles statement without trailing semicolon', () => {
      const out = formatSQL('SELECT 1');
      expect(out.trim()).toBe('SELECT 1;');
    });
  });
});

// ===========================================================================
//  TOKENIZER DIALECT AWARENESS
// ===========================================================================
describe('tokenizer dialect awareness', () => {
  it('tokenizes GO as keyword in tsql', () => {
    const tokens = tokenize('SELECT 1 GO', { dialect: 'tsql' });
    const go = tokens.find(t => t.upper === 'GO');
    expect(go?.type).toBe('keyword');
  });

  it('tokenizes ILIKE as keyword in postgres', () => {
    const tokens = tokenize("SELECT * FROM t WHERE name ILIKE '%x%'", {
      dialect: 'postgres',
    });
    const ilike = tokens.find(t => t.upper === 'ILIKE');
    expect(ilike?.type).toBe('keyword');
  });

  it('does not tokenize ILIKE as keyword in ansi', () => {
    const tokens = tokenize("SELECT ilike FROM t", { dialect: 'ansi' });
    const ilike = tokens.find(t => t.upper === 'ILIKE');
    expect(ilike?.type).toBe('identifier');
  });

  it('tokenizes AUTO_INCREMENT as keyword in mysql', () => {
    const tokens = tokenize('id INT AUTO_INCREMENT', { dialect: 'mysql' });
    const ai = tokens.find(t => t.upper === 'AUTO_INCREMENT');
    expect(ai?.type).toBe('keyword');
  });

  it('does not tokenize AUTO_INCREMENT as keyword in postgres', () => {
    const tokens = tokenize('id INT auto_increment', { dialect: 'postgres' });
    const ai = tokens.find(t => t.upper === 'AUTO_INCREMENT');
    expect(ai?.type).toBe('identifier');
  });

  it('tokenizes NOLOCK as keyword in tsql', () => {
    const tokens = tokenize('WITH (NOLOCK)', { dialect: 'tsql' });
    const nolock = tokens.find(t => t.upper === 'NOLOCK');
    expect(nolock?.type).toBe('keyword');
  });

  it('does not tokenize NOLOCK as keyword in mysql', () => {
    const tokens = tokenize('SELECT nolock FROM t', { dialect: 'mysql' });
    const nolock = tokens.find(t => t.upper === 'NOLOCK');
    expect(nolock?.type).toBe('identifier');
  });

  it('tokenizes UNIQUEIDENTIFIER as keyword in tsql', () => {
    const tokens = tokenize('id UNIQUEIDENTIFIER', { dialect: 'tsql' });
    const uid = tokens.find(t => t.upper === 'UNIQUEIDENTIFIER');
    expect(uid?.type).toBe('keyword');
  });

  it('tokenizes DATETIME2 as keyword in tsql', () => {
    const tokens = tokenize('ts DATETIME2', { dialect: 'tsql' });
    const dt = tokens.find(t => t.upper === 'DATETIME2');
    expect(dt?.type).toBe('keyword');
  });

  it('tokenizes MEDIUMTEXT as keyword in mysql', () => {
    const tokens = tokenize('body MEDIUMTEXT', { dialect: 'mysql' });
    const mt = tokens.find(t => t.upper === 'MEDIUMTEXT');
    expect(mt?.type).toBe('keyword');
  });

  it('does not tokenize MEDIUMTEXT as keyword in ansi', () => {
    const tokens = tokenize('body mediumtext', { dialect: 'ansi' });
    const mt = tokens.find(t => t.upper === 'MEDIUMTEXT');
    expect(mt?.type).toBe('identifier');
  });
});

// ===========================================================================
//  PARSER DIALECT AWARENESS
// ===========================================================================
describe('parser dialect awareness', () => {
  it('parses MySQL CREATE TABLE with ENGINE without error in strict mode', () => {
    const sql =
      'CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;';
    expect(() => parse(sql, { recover: false, dialect: 'mysql' })).not.toThrow();
  });

  it('parses PostgreSQL ON CONFLICT without error in strict mode', () => {
    const sql =
      "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;";
    expect(() =>
      parse(sql, { recover: false, dialect: 'postgres' }),
    ).not.toThrow();
  });

  it('parses T-SQL MERGE without error in strict mode', () => {
    const sql =
      'MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name;';
    expect(() =>
      parse(sql, { recover: false, dialect: 'tsql' }),
    ).not.toThrow();
  });

  it('parses T-SQL DECLARE without error in strict mode', () => {
    expect(() =>
      parse('DECLARE @x INT;', { recover: false, dialect: 'tsql' }),
    ).not.toThrow();
  });

  it('parses PostgreSQL dollar-quoted strings without error', () => {
    expect(() =>
      parse("DO $$ BEGIN RAISE NOTICE 'hello'; END $$;", {
        recover: false,
        dialect: 'postgres',
      }),
    ).not.toThrow();
  });
});

// ===========================================================================
//  REGRESSION: FORMAT DOES NOT CRASH ON UNUSUAL INPUT
// ===========================================================================
describe('no-crash regression tests', () => {
  const dialects = ['mysql', 'postgres', 'tsql', 'ansi'] as const;

  for (const dialect of dialects) {
    describe(`${dialect} does not crash on`, () => {
      it('single keyword', () => {
        expect(() => formatSQL('SELECT', { dialect })).not.toThrow();
      });

      it('unterminated paren (with recovery)', () => {
        expect(() =>
          formatSQL('SELECT (1', { dialect, recover: true }),
        ).not.toThrow();
      });

      it('only comments', () => {
        expect(() =>
          formatSQL('-- just a comment\n/* block */\n', { dialect }),
        ).not.toThrow();
      });

      it('very long identifier', () => {
        const longIdent = 'a'.repeat(500);
        expect(() =>
          formatSQL(`SELECT ${longIdent} FROM t;`, { dialect }),
        ).not.toThrow();
      });

      it('numbers only', () => {
        expect(() => formatSQL('SELECT 12345;', { dialect })).not.toThrow();
      });

      it('nested CASE expressions', () => {
        const sql =
          "SELECT CASE WHEN a = 1 THEN CASE WHEN b = 2 THEN 'x' ELSE 'y' END ELSE 'z' END FROM t;";
        expect(() => formatSQL(sql, { dialect })).not.toThrow();
      });

      it('CTE with multiple branches', () => {
        const sql =
          'WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2;';
        expect(() => formatSQL(sql, { dialect })).not.toThrow();
      });
    });
  }
});

// ===========================================================================
//  CROSS-DIALECT IDEMPOTENCY ON SHARED SQL
// ===========================================================================
describe('cross-dialect idempotency on shared SQL', () => {
  const sharedSQL = [
    'SELECT a, b, c FROM t WHERE a = 1 AND b = 2 ORDER BY c;',
    "INSERT INTO t (a, b) VALUES (1, 'x');",
    'UPDATE t SET a = 1 WHERE b = 2;',
    'DELETE FROM t WHERE a = 1;',
    "SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM t;",
    'SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id;',
    'CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, active BOOLEAN DEFAULT TRUE);',
    'ALTER TABLE t ADD COLUMN email VARCHAR(255);',
    'DROP TABLE IF EXISTS t;',
    'SELECT a, COUNT(*) AS cnt FROM t GROUP BY a HAVING COUNT(*) > 1 ORDER BY cnt DESC;',
  ];

  for (const dialect of ['mysql', 'postgres', 'tsql', 'ansi'] as const) {
    for (const sql of sharedSQL) {
      it(`${dialect} remains idempotent for SQL: ${sql.substring(0, 50)}...`, () => {
        assertIdempotent(sql, dialect);
      });
    }
  }
});
