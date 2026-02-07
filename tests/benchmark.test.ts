import { describe, it, expect } from 'bun:test';
import { formatSQL } from '../src/format';

// Generate a variety of SQL statements for benchmark testing.
// Mixes SELECT, INSERT, UPDATE, DELETE with varying complexity.
function generateBenchmarkSQL(statementCount: number): string {
  const statements: string[] = [];

  for (let i = 0; i < statementCount; i++) {
    const kind = i % 4;
    switch (kind) {
      case 0:
        // SELECT with JOIN and WHERE
        statements.push(
          `SELECT t${i}.id, t${i}.name, t${i}.value, t${i}.created_at ` +
          `FROM table_${i} AS t${i} ` +
          `INNER JOIN ref_${i} AS r${i} ON t${i}.ref_id = r${i}.id ` +
          `WHERE t${i}.status = 'active' AND t${i}.value > ${i} ` +
          `ORDER BY t${i}.created_at DESC LIMIT 100;`
        );
        break;
      case 1:
        // INSERT with multiple VALUES
        statements.push(
          `INSERT INTO table_${i} (col_a, col_b, col_c, col_d) ` +
          `VALUES (${i}, 'value_${i}', ${i * 1.5}, NOW()), ` +
          `(${i + 1}, 'value_${i + 1}', ${(i + 1) * 1.5}, NOW());`
        );
        break;
      case 2:
        // UPDATE with multiple SET and WHERE
        statements.push(
          `UPDATE table_${i} ` +
          `SET col_a = ${i * 2}, col_b = 'updated_${i}', ` +
          `col_c = col_c + 1, updated_at = NOW() ` +
          `WHERE id = ${i} AND status <> 'archived';`
        );
        break;
      case 3:
        // DELETE with subquery
        statements.push(
          `DELETE FROM table_${i} ` +
          `WHERE id IN (SELECT id FROM expired_${i} WHERE expire_date < NOW()) ` +
          `AND status = 'pending';`
        );
        break;
    }
  }

  return statements.join('\n');
}

// Generate more complex statements with CTEs, CASE, window functions
function generateComplexSQL(statementCount: number): string {
  const statements: string[] = [];

  for (let i = 0; i < statementCount; i++) {
    const kind = i % 3;
    switch (kind) {
      case 0:
        // CTE with window function
        statements.push(
          `WITH cte_${i} AS (` +
          `SELECT id, name, department, salary, ` +
          `ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn ` +
          `FROM employees_${i} WHERE status = 'active') ` +
          `SELECT id, name, department, salary FROM cte_${i} WHERE rn <= 5;`
        );
        break;
      case 1:
        // SELECT with CASE and GROUP BY
        statements.push(
          `SELECT department, ` +
          `COUNT(*) AS total, ` +
          `SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_count, ` +
          `AVG(salary) AS avg_salary ` +
          `FROM employees_${i} ` +
          `GROUP BY department ` +
          `HAVING COUNT(*) > 5 ` +
          `ORDER BY total DESC;`
        );
        break;
      case 2:
        // UNION query
        statements.push(
          `SELECT id, name, 'source_a' AS origin FROM table_a_${i} WHERE active = TRUE ` +
          `UNION ALL ` +
          `SELECT id, name, 'source_b' AS origin FROM table_b_${i} WHERE active = TRUE ` +
          `ORDER BY name;`
        );
        break;
    }
  }

  return statements.join('\n');
}

describe('Benchmark: formatting performance', () => {
  it('formats 1000 simple statements in reasonable time', () => {
    const sql = generateBenchmarkSQL(1000);
    const start = performance.now();
    const result = formatSQL(sql);
    const elapsed = performance.now() - start;

    console.log(`  1000 simple statements: ${elapsed.toFixed(1)}ms`);
    expect(result.length).toBeGreaterThan(0);
    expect(elapsed).toBeLessThan(10000); // 10 seconds
  });

  it('formats 10000 simple statements in reasonable time', () => {
    const sql = generateBenchmarkSQL(10000);
    const start = performance.now();
    const result = formatSQL(sql);
    const elapsed = performance.now() - start;

    console.log(`  10000 simple statements: ${elapsed.toFixed(1)}ms`);
    expect(result.length).toBeGreaterThan(0);
    expect(elapsed).toBeLessThan(30000); // 30 seconds
  }, 60000);

  it('formats 1000 complex statements (CTEs, CASE, window) in reasonable time', () => {
    const sql = generateComplexSQL(1000);
    const start = performance.now();
    const result = formatSQL(sql);
    const elapsed = performance.now() - start;

    console.log(`  1000 complex statements: ${elapsed.toFixed(1)}ms`);
    expect(result.length).toBeGreaterThan(0);
    expect(elapsed).toBeLessThan(10000); // 10 seconds
  });

  it('produces idempotent output for benchmark SQL', () => {
    const sql = generateBenchmarkSQL(100);
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });
});
