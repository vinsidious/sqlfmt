import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE CHECK constraint layout', () => {
  it('aligns unnamed CHECK constraints at base indent', () => {
    const sql = `CREATE TABLE t (
  id INT PRIMARY KEY,
  role VARCHAR(20) NOT NULL,
  CHECK (role IN ('a', 'b', 'c'))
);`;
    const out = formatSQL(sql);
    expect(out).toBe(`CREATE TABLE t (
    id   INT         PRIMARY KEY,
    role VARCHAR(20) NOT NULL,
    CHECK(role IN ('a', 'b', 'c'))
);
`);
  });
});
