import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Mixed JOIN type alignment', () => {
  it('aligns LEFT JOIN with plain JOIN clauses without inserting a blank separator', () => {
    const sql = `SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
LEFT JOIN discounts d ON o.discount_id = d.id;`;

    expect(formatSQL(sql)).toBe(`SELECT o.id, c.name
  FROM orders AS o
  JOIN customers AS c
    ON o.customer_id = c.id
  JOIN products AS p
    ON o.product_id = p.id
       LEFT JOIN discounts AS d
       ON o.discount_id = d.id;
`);
  });
});
