import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('BigQuery procedure body formatting', () => {
  it('formats statements inside CREATE OR REPLACE PROCEDURE blocks', () => {
    const sql = `CREATE OR REPLACE PROCEDURE udf.create_customer()
BEGIN
  DECLARE id STRING;
  SET id = GENERATE_UUID();
  INSERT INTO udf.customers (id)
    VALUES(id);
  SELECT FORMAT("Created customer %s", id);
END`;

    const out = formatSQL(sql);
    expect(out).toContain('DECLARE id STRING;');
    expect(out).toContain('SET id = GENERATE_UUID();');
    expect(out).toContain('INSERT INTO udf.customers (id)\n    VALUES (id);');
    expect(out).toContain('SELECT format("Created customer %s", id);');
  });
});
