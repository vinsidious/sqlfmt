import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Oracle type declarations', () => {
  it('parses collection and object type declarations in strict mode', () => {
    const sql = `CREATE OR REPLACE TYPE PhoneNumberList AS TABLE OF VARCHAR2(20);
/
CREATE OR REPLACE TYPE PersonType AS OBJECT (
  first_name VARCHAR2(50),
  last_name VARCHAR2(50),
  birth_date DATE
);
/`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('preserves type declaration structure and SQL*Plus run terminators', () => {
    const sql = `CREATE OR REPLACE TYPE PhoneNumberList AS TABLE OF VARCHAR2(20);
/
CREATE OR REPLACE TYPE PersonType AS OBJECT (
  first_name VARCHAR2(50),
  last_name VARCHAR2(50),
  birth_date DATE
);
/`;

    const out = formatSQL(sql);

    expect(out).toContain('CREATE OR REPLACE TYPE PhoneNumberList AS TABLE OF VARCHAR2(20);');
    expect(out).toContain('CREATE OR REPLACE TYPE PersonType AS OBJECT');
    expect(out).toContain('first_name VARCHAR2(50)');
    expect(out).toContain('birth_date DATE');
    expect(out).toContain(');\n/');
  });
});
