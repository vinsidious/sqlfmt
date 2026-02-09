import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('INSERT VALUES and standalone comment boundaries', () => {
  it('keeps comments between INSERT column lists and VALUES in the same statement', () => {
    const sql = "INSERT INTO t (c1, c2)\n-- comment\nVALUES (1, 'a');";
    const out = formatSQL(sql);
    expect(out).toContain('INSERT INTO t (c1, c2)');
    expect(out).toContain('-- comment');
    expect(out).toContain("VALUES (1, 'a');");
    expect(out).not.toContain('INSERT INTO t (c1, c2);');
  });

  it('preserves tuple-level block comments in INSERT VALUES lists', () => {
    const sql = 'INSERT INTO t (a) VALUES (1) /* first */, (2) /* second */;';
    const out = formatSQL(sql);
    expect(out).toContain('VALUES (1) /* first */,');
    expect(out).toContain('(2) /* second */;');
  });

  it('keeps standalone comments between statements on their own line without semicolons', () => {
    const sql = 'SELECT a FROM t1\n\n-- section divider\n\nSELECT b FROM t2';
    const out = formatSQL(sql);
    expect(out).toContain('\n-- section divider\nSELECT b');
    expect(out).not.toContain('FROM t1 -- section divider');
  });
});
