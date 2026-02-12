import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('SQL regression behaviors', () => {
  it('remains idempotent for comments leading into WITH blocks', () => {
    const sql = 'SELECT 1;\n\n-- mixed srsName\n\nWITH g AS (SELECT 1) SELECT * FROM g;';
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('supports psql meta-commands in recovery mode and strict mode', () => {
    const sql = 'SELECT * FROM users;\n\\d users\nSELECT * FROM orders;';
    const out = formatSQL(sql);
    expect(out).toContain('\\d users');
    expect(out).toContain('SELECT *');
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('handles inline psql meta-commands appended to SQL lines', () => {
    const sql = "SELECT stats_reset as prev_stats_reset FROM pg_stat_subscription_stats WHERE subname = 'regress_testsub' \\gset\nSELECT :'prev_stats_reset' < stats_reset FROM pg_stat_subscription_stats WHERE subname = 'regress_testsub';";
    const out = formatSQL(sql);
    expect(out).toContain('\\gset');
    expect(out).toContain("SELECT :'prev_stats_reset' < stats_reset");
  });

  it('supports COPY FROM stdin blocks terminated by \\.', () => {
    const sql = 'COPY test_table FROM stdin;\n1\tAlice\t100\n2\tBob\t200\n\\.\nSELECT 1;';
    const out = formatSQL(sql);
    expect(out).toContain('COPY test_table FROM stdin;');
    expect(out).toContain('1\tAlice\t100');
    expect(out).toContain('\\.');
    expect(out).toContain('SELECT 1;');
  });

  it('allows overriding tokenizer token-count limit', () => {
    const sql = 'SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;';
    expect(() => parse(sql, { recover: false, maxTokenCount: 5 } as never)).toThrow();
    expect(() => parse(sql, { recover: false, maxTokenCount: 200 } as never)).not.toThrow();
  });

  it('supports escaped semicolons (\\;)', () => {
    const out = formatSQL('SELECT 1\\; SELECT 2\\; SELECT 3;');
    expect(out).toContain('SELECT 1;');
    expect(out).toContain('SELECT 2;');
    expect(out).toContain('SELECT 3;');
  });

  it('supports SET and RESET in strict mode', () => {
    expect(() => parse('SET statement_timeout = 0;', { recover: false })).not.toThrow();
    expect(() => parse('RESET enable_partitionwise_join;', { recover: false })).not.toThrow();
  });

  it('supports ANALYZE and VACUUM in strict mode', () => {
    expect(() => parse('ANALYZE prt1;', { recover: false })).not.toThrow();
    expect(() => parse('VACUUM;', { recover: false })).not.toThrow();
  });

  it('supports DECLARE CURSOR in strict mode', () => {
    expect(() => parse('DECLARE xc CURSOR WITH HOLD FOR SELECT * FROM t ORDER BY 1;', { recover: false })).not.toThrow();
  });

  it('supports PREPARE/EXECUTE/DEALLOCATE in strict mode', () => {
    expect(() => parse('PREPARE data_sel AS SELECT generate_series(1,3);', { recover: false })).not.toThrow();
    expect(() => parse('EXECUTE data_sel;', { recover: false })).not.toThrow();
    expect(() => parse('DEALLOCATE data_sel;', { recover: false })).not.toThrow();
  });

  it('supports CTAS in strict mode', () => {
    expect(() => parse('CREATE TABLE t2 AS SELECT count(*) FROM t1;', { recover: false })).not.toThrow();
  });

  it('supports USE in strict mode', () => {
    expect(() => parse('USE `Chinook`;', { recover: false })).not.toThrow();
  });

  it('supports DO blocks in strict mode', () => {
    expect(() => parse("DO $$ BEGIN EXECUTE 'SELECT 1'; END$$;", { recover: false })).not.toThrow();
  });

  it('supports parenthesized UNION expressions', () => {
    expect(() => parse('SELECT 1 UNION (SELECT 2 UNION ALL SELECT 2) ORDER BY 1;', { recover: false })).not.toThrow();
  });

  it('supports ORDER BY ... USING operator syntax', () => {
    expect(() => parse('SELECT * FROM t ORDER BY two USING <, string4 USING <;', { recover: false })).not.toThrow();
    const out = formatSQL('SELECT * FROM t ORDER BY two USING <, string4 USING <;');
    expect(out).toContain('ORDER BY two USING <, string4 USING <');
  });

  it('supports WITH ORDINALITY in FROM clauses', () => {
    const out = formatSQL('SELECT * FROM rngfunct(1) WITH ORDINALITY AS z(a,b,ord);');
    expect(out).toContain('WITH ORDINALITY AS z(a, b, ord)');
  });

  it('supports CREATE TABLE WITH (storage_parameters)', () => {
    expect(() => parse('CREATE TABLE target (tid integer) WITH (autovacuum_enabled=off);', { recover: false })).not.toThrow();
  });

  it('supports named WINDOW references in OVER (...)', () => {
    const sql = 'SELECT sum(x) OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t WINDOW w AS (ORDER BY y);';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain(`OVER (
           w
           RANGE BETWEEN UNBOUNDED PRECEDING
                     AND CURRENT ROW
       )`);
    expect(out).toContain('WINDOW w AS (ORDER BY y)');
  });

  it('supports MySQL DELIMITER scripts without parse failure', () => {
    const sql = 'DELIMITER ;;\nCREATE TRIGGER `ins_film` AFTER INSERT ON `film` FOR EACH ROW BEGIN\n  INSERT INTO film_text VALUES (NEW.film_id, NEW.title);\nEND;;\nDELIMITER ;';
    expect(() => parse(sql, { recover: false, dialect: 'mysql' })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'mysql' });
    expect(out).toContain('DELIMITER ;;');
    expect(out).toContain('END;;');
    expect(out).toContain('DELIMITER ;');
  });

  it('supports MySQL DELIMITER $$ routines with dollar-text literals', () => {
    const sql = "DELIMITER $$\nCREATE PROCEDURE p()\nBEGIN\n  SELECT '$$';\nEND$$\nDELIMITER ;\nSELECT 1;";
    expect(() => parse(sql, { recover: false, dialect: 'mysql' })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'mysql' });
    expect(out).toContain("SELECT '$$';");
    expect(out).toContain('END$$');
    expect(out).toContain('SELECT 1;');
  });

  it('supports top-level T-SQL IF/BEGIN/END blocks in strict mode', () => {
    const sql = "IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'Chinook') BEGIN DROP DATABASE [Chinook]; END";
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('supports tuple-literal composite casts', () => {
    expect(() => parse('SELECT (1.1,2.2)::complex;', { recover: false })).not.toThrow();
    const out = formatSQL('SELECT (1.1,2.2)::complex;');
    expect(out).toContain('(1.1, 2.2)::complex');
  });

  it('supports N-prefixed strings in multi-row INSERT', () => {
    expect(() => parse("INSERT INTO Genre (GenreId, Name) VALUES (1, N'Rock'), (2, N'Jazz');", { recover: false })).not.toThrow();
  });

  it('does not inject spaces around colon in object names', () => {
    expect(formatSQL('DROP VIEW revenue:s;').trim()).toBe('DROP VIEW revenue:s;');
  });

  it('adds blank lines around UNION operators', () => {
    const out = formatSQL('SELECT 1 AS two UNION SELECT 2.2 ORDER BY 1;');
    expect(out).toMatch(/\n\s*\n\s*UNION\n\s*\n/);
  });

  it('parses UESCAPE clauses in strict mode', () => {
    expect(() => parse("SELECT U&'wrong: +0061' UESCAPE +;", { recover: false })).not.toThrow();
  });
});
