import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Oracle delete statements without FROM keyword', () => {
  it('parses delete shorthand in strict mode', () => {
    const sql = 'DELETE member;';

    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('normalizes delete shorthand to explicit FROM form', () => {
    const sql = 'DELETE member;';

    const out = formatSQL(sql);

    expect(out).toBe('DELETE\n  FROM member;\n');
  });
});
