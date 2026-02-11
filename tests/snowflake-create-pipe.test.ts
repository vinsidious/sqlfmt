import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Snowflake CREATE PIPE formatting', () => {
  it('normalizes CREATE PIPE clause keyword casing', () => {
    const sql = `CREATE OR REPLACE pipe MANAGE_DB.pipes.employee_pipe
auto_ingest = TRUE
AS
COPY INTO OUR_FIRST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.csv_folder  ;`;

    const out = formatSQL(sql);
    expect(out).toContain('CREATE OR REPLACE PIPE MANAGE_DB.pipes.employee_pipe');
    expect(out).toContain('AUTO_INGEST = TRUE');
    expect(out).toContain('COPY INTO OUR_FIRST_DB.PUBLIC.employees');
    expect(out).toContain('FROM @MANAGE_DB.external_stages.csv_folder;');
    expect(out).not.toContain('folder  ;');
  });
});
