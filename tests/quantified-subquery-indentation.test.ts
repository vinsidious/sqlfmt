import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Quantified comparison subquery indentation', () => {
  it('indents inner SELECT clauses under ALL-subquery comparisons', () => {
    const sql = 'SELECT * FROM Employee WHERE salary > ALL (SELECT salary FROM Employee WHERE deptno = 30);';
    const out = formatSQL(sql);

    expect(out).toContain('WHERE salary > ALL (SELECT salary');
    expect(out).toMatch(/ALL \(SELECT salary\n\s{10,}FROM Employee\n\s{10,}WHERE deptno = 30\)/);
    expect(out).not.toContain('ALL (SELECT salary\n  FROM Employee\n WHERE deptno = 30)');
  });
});
