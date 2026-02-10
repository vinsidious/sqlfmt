import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Parenthesized subquery alignment', () => {
  it('aligns subquery clauses inside parenthesized comparison expressions', () => {
    const sql = "SELECT name FROM world WHERE (continent = 'Europe') AND (gdp/population > (SELECT gdp/population FROM world WHERE name = 'United Kingdom'));";

    const out = formatSQL(sql).trimEnd();

    expect(out).toContain("AND (gdp / population > (SELECT gdp / population\n");
    expect(out).toMatch(/gdp \/ population > \(SELECT gdp \/ population\n\s+FROM world\n\s+WHERE name = 'United Kingdom'\)\);$/);
    expect(out).not.toContain("(SELECT gdp / population\n  FROM world\n WHERE name = 'United Kingdom')");
  });
});
