import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Semicolonless section comment boundaries', () => {
  it('keeps standalone section comments detached from preceding predicates', () => {
    const sql = `SELECT product
  FROM sales
 WHERE product = 'x'
   AND quantity >= 4

-- Q.3 Write a SQL query to calculate the total sales...

SELECT category,
       SUM(price)
  FROM sales`;

    const out = formatSQL(sql);

    expect(out).toContain('-- Q.3 Write a SQL query to calculate the total sales...\n\nSELECT category');
    expect(out).not.toContain("AND quantity >= 4  -- Q.3 Write a SQL query to calculate the total sales...");
  });
});

