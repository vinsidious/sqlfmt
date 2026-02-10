import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('FULLTEXT identifier column handling', () => {
  it('parses FULLTEXT as an unquoted column identifier when followed by a data type', () => {
    const sql = 'CREATE TABLE test (fulltext INT NOT NULL);';

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('fulltext INT NOT NULL');
  });
});
