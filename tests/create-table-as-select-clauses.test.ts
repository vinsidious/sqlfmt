import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE AS SELECT with pre-AS clauses', () => {
  it('keeps clustering clauses attached to CTAS before the SELECT body', () => {
    const sql = `CREATE TABLE t (id INT64, col STRING) CLUSTER BY id AS
SELECT * FROM src;`;

    expect(formatSQL(sql)).toBe(`CREATE TABLE t (
    id  INT64,
    col STRING
) CLUSTER BY id AS
SELECT *
  FROM src;
`);
  });
});
