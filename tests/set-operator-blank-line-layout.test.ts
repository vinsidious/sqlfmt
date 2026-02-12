import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Set operator blank-line layout', () => {
  it('keeps blank lines above and below set operators', () => {
    const sql = `SELECT a FROM t UNION ALL SELECT b FROM t2 INTERSECT SELECT c FROM t3 EXCEPT SELECT d FROM t4;`;
    const out = formatSQL(sql);

    expect(out).toMatch(/\n\n\s+UNION ALL\n\n/);
    expect(out).toMatch(/\n\n\s*INTERSECT\n\n/);
    expect(out).toMatch(/\n\n\s*EXCEPT\n\n/);
  });
});
