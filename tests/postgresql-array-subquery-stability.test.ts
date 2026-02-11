import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('PostgreSQL array subquery formatting stability', () => {
  it('keeps ARRAY subquery formatting stable across repeated runs', () => {
    const sql = `select array(select sum(x+y) s
            from generate_series(1,3) y group by y order by s)
  from generate_series(1,3) x;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
  });
});
