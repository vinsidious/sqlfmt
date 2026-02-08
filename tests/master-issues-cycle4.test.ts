import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('master issues cycle 4 regressions (2026-02-08)', () => {
  it('#1 line comments never swallow subsequent SQL', () => {
    const sql = `ALTER TABLE t ADD CONSTRAINT pk_t -- comment
PRIMARY KEY (id -- inline
);
CREATE TABLE z (id INT);`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(once).toContain('-- comment');
    expect(once).toContain('PRIMARY KEY');
    expect(once).toContain('CREATE TABLE z');
    expect(once).toContain('\nPRIMARY KEY');
    expect(once).toContain('\nCREATE TABLE z');
    expect(twice).toBe(once);
  });

  it('#2 supports emoji/unicode outside BMP in string literals', () => {
    const sql = "INSERT INTO countries (code, flag) VALUES ('AG', '🇦🇬');";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain("'🇦🇬'");
  });

  it('#3 supports Unicode curly quote characters without tokenizer crash', () => {
    const sql = "SELECT ’smart quote literal’;";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain("'smart quote literal'");
  });

  it('#4 recovers from incomplete WHERE clause with partial output', () => {
    const sql = 'SELECT 1;\nSELECT * FROM t WHERE\nSELECT 2;';
    const out = formatSQL(sql);
    expect(out).toContain('SELECT 1;');
    expect(out).toContain('SELECT * FROM t WHERE');
    expect(out).toContain('SELECT 2;');
  });

  it('#5 keeps IDENTITY(1,1) argument comma inside type specifier', () => {
    const sql = 'CREATE TABLE r (region_id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(10));';
    const out = formatSQL(sql);
    expect(out).toMatch(/IDENTITY\(1,\s*1\)/);
    expect(out).not.toContain('IDENTITY(1,\n');
  });

  it('#6 preserves MySQL user@host token without space padding', () => {
    const sql = "GRANT ALL PRIVILEGES ON db.* TO 'fred'@'localhost';";
    const out = formatSQL(sql);
    expect(out).toContain("'fred'@'localhost'");
    expect(out).not.toContain("'fred' @ 'localhost'");
  });

  it('#7 uppercases MySQL tinyint/unsigned keywords in formatted output', () => {
    const out = formatSQL('CREATE TABLE t (a tinyint unsigned NOT NULL);');
    expect(out).toContain('TINYINT UNSIGNED');
  });

  it('#8 uppercases ON in CREATE TRIGGER context', () => {
    const out = formatSQL('CREATE TRIGGER trg AFTER UPDATE on `data_table` FOR EACH ROW SET @x = 1;');
    expect(out).toContain('UPDATE ON `data_table`');
  });

  it('#9 supports // comments without breaking subsequent SQL parsing', () => {
    const sql = '// comment\nSELECT 1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('// comment');
    expect(out).toContain('SELECT 1;');
  });

  it('#10 supports Oracle INSERT ALL ... WHEN ... THEN ... INTO in strict mode', () => {
    const sql = "INSERT ALL WHEN score >= 90 THEN INTO a_grade (id) VALUES (1) INTO all_grades (id) VALUES (1) SELECT 1 FROM dual;";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('INSERT ALL');
    expect(out).toContain('WHEN score >= 90 THEN');
  });

  it('#11 supports DROP TABLE ... CASCADE CONSTRAINTS in strict mode', () => {
    expect(() => parse('DROP TABLE t CASCADE CONSTRAINTS;', { recover: false })).not.toThrow();
    expect(formatSQL('DROP TABLE t CASCADE CONSTRAINTS;')).toContain('CASCADE CONSTRAINTS');
  });

  it('#12 supports MySQL hex literals in INSERT VALUES', () => {
    expect(() => parse('INSERT INTO t VALUES (0x0200122C);', { recover: false })).not.toThrow();
    const out = formatSQL('INSERT INTO t VALUES (0x0200122C);');
    expect(out).toContain('0x0200122C');
  });

  it('#13 supports PostgreSQL regex operator and substring(... FROM ...)', () => {
    const sql = "SELECT substring(email FROM '@(.*)$') FROM users WHERE email ~ '@';";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain("SUBSTRING(email FROM '@(.*)$')");
    expect(out).toContain("email ~ '@'");
  });

  it('#14 supports NATURAL JOIN with parenthesized join expressions', () => {
    const sql = 'SELECT * FROM (t1 NATURAL JOIN t2) NATURAL JOIN t3;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('#15 supports CREATE TEMPORARY TABLE ... AS SELECT in strict mode', () => {
    const sql = 'CREATE TEMPORARY TABLE tmp_users AS SELECT id FROM users;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('CREATE TEMPORARY TABLE tmp_users AS');
    expect(out).toContain('SELECT id');
  });

  it('#16 supports MySQL INSERT table(cols) shorthand without INTO', () => {
    const sql = 'INSERT most_popular (video_id, score) VALUES (1, 100);';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('#17 supports CREATE VIEW with parenthesized FROM-join clause', () => {
    const sql = 'CREATE VIEW v AS SELECT * FROM (a JOIN b ON a.id = b.id);';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('#18 supports T-SQL SELECT TOP N syntax', () => {
    const sql = 'SELECT TOP 10 * FROM users;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('SELECT TOP 10 *');
  });

  it('#19 supports sp_rename procedure calls in strict mode', () => {
    const sql = "sp_rename @objname = 'dbo.TableA.ColA', @newname = 'ColB';";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('sp_rename');
    expect(out).toContain('@objname');
  });

  it('#20 supports KWDB-style TIMESTAMPTZ typed literals and duration suffixes', () => {
    const sql = "SELECT TIMESTAMPTZ'2020-01-01 00:00:00+00' + 10y - 12mon + 4w - 32d + 24h + 60m + 60s + 1000ms;";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain("TIMESTAMPTZ'2020-01-01 00:00:00+00'");
    expect(out).toContain('10y');
    expect(out).toContain('1000ms');
  });

  it('#21 infers statement boundaries when semicolons are missing', () => {
    const sql = 'SELECT * FROM buy\nSELECT * FROM flower';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('FROM buy;');
    expect(out).toContain('FROM flower;');
  });
});
