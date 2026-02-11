import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Comment spacing and inline association', () => {
  it('keeps a blank line between comment blocks and the following statement', () => {
    const sql = `--
-- Table structure
--

CREATE TABLE t (id INT);`;

    const out = formatSQL(sql);
    expect(out).toContain('/* */');
    expect(out).toContain('/* Table structure */');
    expect(out).toContain('/* */\n\nCREATE TABLE t');
  });

  it('keeps statement-ending line comments on the same line', () => {
    const out = formatSQL('SELECT 1; -- my inline comment');
    expect(out).toContain('SELECT 1; -- my inline comment');
    expect(out).not.toContain('SELECT 1;\n\n-- my inline comment');
  });

  it('keeps trailing column comments attached to the column line', () => {
    const sql = `CREATE TABLE t (
    a INT,
    b TEXT -- trailing comment
);`;

    const out = formatSQL(sql);
    expect(out).toContain('b TEXT -- trailing comment');
    expect(out).not.toContain('b TEXT\n    -- trailing comment');
  });

  it('keeps boolean operators singular when a line comment ends the previous predicate', () => {
    const sql = `SELECT *
FROM t
WHERE ReadingOrder = 'ULD_AF' -- Only Return Values for this ReadingOrder
AND [Read] BETWEEN DATEADD(MONTH,-6,GETDATE()) AND GETDATE();`;

    const out = formatSQL(sql);
    expect(out).toContain("WHERE ReadingOrder = 'ULD_AF' -- Only Return Values for this ReadingOrder");
    expect(out).toContain('AND [Read] BETWEEN DATEADD(MONTH, - 6, GETDATE()) AND GETDATE()');
    expect(out).not.toMatch(/\n\s*AND\s*--[^\n]*\nAND\b/);
    expect(out).not.toMatch(/\n\s*OR\s*--[^\n]*\nOR\b/);
  });
});
