import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('NOT IN subquery indentation', () => {
  it('indents NOT IN subquery FROM clauses under the nested SELECT', () => {
    const out = formatSQL('SELECT * FROM x WHERE NOT pcp IN (SELECT head FROM department);');

    expect(out).toMatch(/NOT pcp IN \(SELECT head\n\s{10,}FROM department\);/);
    expect(out).not.toContain('\n  FROM department);');
  });
});
