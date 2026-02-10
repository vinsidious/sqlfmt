import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Oracle nested table storage clauses', () => {
  it('parses nested table storage clause in strict mode', () => {
    const sql = `CREATE TABLE users (
  userid VARCHAR2(10) NOT NULL,
  usernumber phonenumberlist,
  PRIMARY KEY (userid)
) NESTED TABLE usernumber STORE AS userphonenumber_nested;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('formats nested table storage clause after table elements', () => {
    const sql = `CREATE TABLE users (
  userid VARCHAR2(10) NOT NULL,
  usernumber phonenumberlist,
  PRIMARY KEY (userid)
) NESTED TABLE usernumber STORE AS userphonenumber_nested;`;

    const out = formatSQL(sql);

    expect(out).toContain('NESTED TABLE usernumber STORE AS userphonenumber_nested');
  });
});
