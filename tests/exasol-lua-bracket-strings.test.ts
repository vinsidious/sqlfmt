import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { tokenize } from '../src/tokenizer';

describe('Exasol Lua Bracket Strings', () => {
  it('tokenizes Lua-style bracket strings with concatenation markers', () => {
    const sql = `where s.name ]]..SCHEMA_STR..[[ and t.name ]]..TABLE_STR..' '`;
    expect(() => tokenize(sql)).not.toThrow();
  });

  it('formats text containing Lua-style bracket string concatenation markers', () => {
    const sql = `where s.name ]]..SCHEMA_STR..[[ and t.name ]]..TABLE_STR..' '`;
    const out = formatSQL(sql);
    expect(out).toContain('SCHEMA_STR');
    expect(out).toContain('TABLE_STR');
  });
});
