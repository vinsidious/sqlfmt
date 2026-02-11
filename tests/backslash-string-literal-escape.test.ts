import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';
import { tokenize } from '../src/tokenizer';

describe('Backslash-escaped string literals', () => {
  it('tokenizes and formats adjacent literals after escaped backslashes', () => {
    const sql = "SELECT '\\\\' , 'x';";

    expect(() => tokenize(sql)).not.toThrow();
    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql);
    expect(out).toContain("'\\\\'");
    expect(out).toContain("'x'");
  });

  it('tokenizes escaped-backslash clauses used by load statements', () => {
    const sql = "CREATE PIPELINE test AS LOAD DATA LINK x 'y' INTO TABLE t FIELDS TERMINATED BY ',' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n';";

    expect(() => tokenize(sql)).not.toThrow();
    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql);
    expect(out).toContain("ESCAPED BY '\\\\'");
  });
});
