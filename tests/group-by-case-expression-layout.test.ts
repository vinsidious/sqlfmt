import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('GROUP BY case expression layout', () => {
  it('wraps long GROUP BY clauses and formats CASE expressions as blocks', () => {
    const sql = `SELECT CASE
       WHEN EXTRACT(MONTH FROM order_date) BETWEEN 1 AND 3 THEN 'Q1'
       WHEN EXTRACT(MONTH FROM order_date) BETWEEN 4 AND 6 THEN 'Q2'
       WHEN EXTRACT(MONTH FROM order_date) BETWEEN 7 AND 9 THEN 'Q3'
       ELSE 'Q4'
       END AS quarter,
       EXTRACT(YEAR FROM order_date) AS order_year,
       COUNT(*) AS order_count,
       SUM(total_amount) AS quarterly_revenue,
       AVG(total_amount) AS avg_order_value
  FROM orders
 GROUP BY EXTRACT(YEAR FROM order_date), CASE WHEN EXTRACT(MONTH FROM order_date) BETWEEN 1 AND 3 THEN 'Q1' WHEN EXTRACT(MONTH FROM order_date) BETWEEN 4 AND 6 THEN 'Q2' WHEN EXTRACT(MONTH FROM order_date) BETWEEN 7 AND 9 THEN 'Q3' ELSE 'Q4' END
 ORDER BY order_year, quarter;`;

    const expected = `SELECT CASE
       WHEN EXTRACT(MONTH FROM order_date) BETWEEN 1 AND 3 THEN 'Q1'
       WHEN EXTRACT(MONTH FROM order_date) BETWEEN 4 AND 6 THEN 'Q2'
       WHEN EXTRACT(MONTH FROM order_date) BETWEEN 7 AND 9 THEN 'Q3'
       ELSE 'Q4'
       END AS quarter,
       EXTRACT(YEAR FROM order_date) AS order_year,
       COUNT(*) AS order_count,
       SUM(total_amount) AS quarterly_revenue,
       AVG(total_amount) AS avg_order_value
  FROM orders
 GROUP BY EXTRACT(YEAR FROM order_date),
          CASE
          WHEN EXTRACT(MONTH FROM order_date) BETWEEN 1 AND 3 THEN 'Q1'
          WHEN EXTRACT(MONTH FROM order_date) BETWEEN 4 AND 6 THEN 'Q2'
          WHEN EXTRACT(MONTH FROM order_date) BETWEEN 7 AND 9 THEN 'Q3'
          ELSE 'Q4'
          END
 ORDER BY order_year, quarter;`;

    expect(formatSQL(sql).trimEnd()).toBe(expected);
  });
});
