import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('MySQL CREATE TABLE options and SHOW COLUMNS', () => {
  it('keeps table options attached to CREATE TABLE statements', () => {
    const sql = "CREATE TABLE t (id INT PRIMARY KEY) COMMENT 'test table' ENGINE = InnoDB;";
    const out = formatSQL(sql);
    expect(out).toContain(") COMMENT 'test table' ENGINE = InnoDB;");
    expect(out).not.toContain(";\n\nCOMMENT 'test table'");
  });

  it('keeps backslash line continuations in CREATE TABLE option blocks', () => {
    const sql = "CREATE TABLE t (id INT) ENGINE=InnoDB \\\n COMMENT='test';";
    const out = formatSQL(sql);
    expect(out).toContain('ENGINE = InnoDB \\ COMMENT =');
    expect(out).not.toContain('\\;\n\nCOMMENT');
  });

  it('does not wrap COMMENT string literals across multiple lines', () => {
    const sql = "CREATE TABLE t (col1 VARCHAR(255) NOT NULL COMMENT 'A long comment about this column');";
    const out = formatSQL(sql, { maxLineLength: 60 });
    expect(out).toContain("'A long comment about this column'");
    expect(out).not.toContain('A long comment about\n');
  });

  it('formats SHOW COLUMNS statements with statement keywords normalized', () => {
    const out = formatSQL('show columns from ch1;');
    expect(out.trim()).toBe('SHOW COLUMNS FROM ch1;');
  });

  it('keeps unsigned modifiers visually attached to integer types', () => {
    const sql = 'CREATE TABLE t (id INT UNSIGNED NOT NULL, note VARCHAR(255) NOT NULL);';
    const out = formatSQL(sql);
    expect(out).toContain('id   INT UNSIGNED NOT NULL');
    expect(out).not.toMatch(/INT\s{2,}UNSIGNED/);
  });
});
