import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Window frame OVER clause layout', () => {
  it('formats framed window clauses as multiline OVER blocks', () => {
    const sql = `SELECT o.id,
       o.placed_at,
       o.total,
       SUM(o.total) OVER (ORDER BY o.placed_at RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW) AS week_rolling_sum,
       COUNT(*) OVER (ORDER BY o.placed_at RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS week_orders_excl_current,
       SUM(o.total) OVER (ORDER BY o.placed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) AS cumulative_excl_ties
  FROM orders AS o
 WHERE o.status = 'delivered';`;

    const expected = `SELECT o.id,
       o.placed_at,
       o.total,
       SUM(o.total) OVER (
           ORDER BY o.placed_at
           RANGE BETWEEN INTERVAL '7 days' PRECEDING
                     AND CURRENT ROW
       ) AS week_rolling_sum,
       COUNT(*) OVER (
           ORDER BY o.placed_at
           RANGE BETWEEN INTERVAL '7 days' PRECEDING
                     AND CURRENT ROW
         EXCLUDE CURRENT ROW
       ) AS week_orders_excl_current,
       SUM(o.total) OVER (
           ORDER BY o.placed_at
            ROWS BETWEEN UNBOUNDED PRECEDING
                     AND CURRENT ROW
         EXCLUDE TIES
       ) AS cumulative_excl_ties
  FROM orders AS o
 WHERE o.status = 'delivered';`;

    expect(formatSQL(sql).trimEnd()).toBe(expected);
  });
});
