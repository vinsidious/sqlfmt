import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE named constraint alignment', () => {
  it('aligns named CHECK constraints with other named table constraints', () => {
    const sql = `CREATE TABLE test_table (
  col1 int NOT NULL,
  col2 varchar(50),
  CONSTRAINT pk_test PRIMARY KEY (col1),
  CONSTRAINT fk_test FOREIGN KEY (col2) REFERENCES other_table (id),
  CONSTRAINT ck_test CHECK (col1 > 0)
);`;

    const out = formatSQL(sql);
    expect(out).toBe(`CREATE TABLE test_table (
    col1 INT         NOT NULL,
    col2 VARCHAR(50),
    CONSTRAINT pk_test
        PRIMARY KEY (col1),
    CONSTRAINT fk_test
        FOREIGN KEY (col2)
        REFERENCES other_table (id),
    CONSTRAINT ck_test
        CHECK(col1 > 0)
);
`);
  });
});
