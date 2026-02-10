import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Write statement identifier casing', () => {
  it('lowercases unquoted identifiers in CREATE, INSERT, ALTER, and DROP statements', () => {
    const sql = `CREATE TABLE MY_TABLE (MY_COL1 INT NOT NULL);
INSERT INTO MY_TABLE (MY_COL1) VALUES (1);
ALTER TABLE MY_TABLE DROP COLUMN MY_COL1;
DROP TABLE MY_TABLE;`;

    const out = formatSQL(sql).trimEnd();

    expect(out).toBe(`CREATE TABLE my_table (
    my_col1 INT NOT NULL
);

INSERT INTO my_table (my_col1)
VALUES (1);

ALTER TABLE my_table
        DROP COLUMN my_col1;

DROP TABLE my_table;`);
  });
});
