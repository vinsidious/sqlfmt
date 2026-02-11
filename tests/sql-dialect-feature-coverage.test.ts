import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

function formatWithoutRecoveries(sql: string): string {
  const recoveries: string[] = [];
  const out = formatSQL(sql, {
    onRecover: err => recoveries.push(err.message),
  });
  expect(recoveries).toEqual([]);
  return out;
}

function expectIdempotentWithoutRecoveries(sql: string): string {
  const once = formatWithoutRecoveries(sql);
  const twice = formatWithoutRecoveries(once);
  expect(twice).toBe(once);
  return once;
}

function expectStrictParseAndNoRecoveries(sql: string): string {
  expect(() => parse(sql, { recover: false })).not.toThrow();
  return formatWithoutRecoveries(sql);
}

describe('sql dialect feature coverage', () => {
  it('MySQL # comments remain stable across formatting passes', () => {
    const sql = `# V1
# https://www.cnblogs.com/onePunchCoder/p/11619433.html
SELECT
  t.team_id,
  t.team_name
FROM teams t;`;

    const out = expectIdempotentWithoutRecoveries(sql);
    expect(out).toContain('# V1');
    expect(out).toContain('# https://www.cnblogs.com/onePunchCoder/p/11619433.html');
  });

  it('line comments with semicolons do not duplicate across passes', () => {
    const sql = `SELECT * FROM USUARIOS;
--select * from Categorias;
GO
SELECT * FROM Categorias;`;

    const out = expectIdempotentWithoutRecoveries(sql);
    expect(out).toContain('--select * from Categorias;');
    expect(out).not.toContain('--select * from Categorias;;');
  });

  it('psql meta-commands with inline \\gset are idempotent', () => {
    const sql = `\\set dbuser :dbuser
SELECT CASE WHEN :'dbuser' = ':dbuser' THEN 'zulip' ELSE :'dbuser' END AS dbuser \\gset
\\set dbname :dbname
SELECT CASE WHEN :'dbname' = ':dbname' THEN 'zulip' ELSE :'dbname' END AS dbname \\gset

\\connect postgres
DROP DATABASE IF EXISTS :"dbname";`;

    const once = formatSQL(sql, { recover: true });
    const twice = formatSQL(once, { recover: true });
    expect(twice).toBe(once);
    const out = once;
    expect(out).toContain('\\set dbuser :dbuser');
    expect(out).toContain('\\gset');
  });

  it('comment between CTE AS and opening parenthesis parses natively', () => {
    const sql = `WITH cteEndDates (person_id, end_date) AS -- the magic
(
  SELECT person_id,
         DATEADD(day, -1 * 0, event_date) AS end_date
  FROM #cohort_rows
)
SELECT * FROM cteEndDates;`;

    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out).toContain('/* the magic */');
    expect(out).toContain('WITH cteEndDates');
  });

  it('MERGE allows comment between target alias and USING', () => {
    const sql = `MERGE INTO SALES_FINAL_TABLE F -- Target table to merge changes from source table
USING SALES_STREAM S
ON f.id = s.id
WHEN MATCHED THEN
  UPDATE SET f.product = s.product;`;

    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out).toContain('MERGE INTO SALES_FINAL_TABLE AS F');
    expect(out).toContain('USING SALES_STREAM AS S');
  });

  it('aggregate FILTER (WHERE ...) parses without recovery', () => {
    const sql = 'SELECT SUM(unique1) FILTER (WHERE unique1 > 100) FROM tenk1;';
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('FILTER (WHERE UNIQUE1 > 100)');
  });

  it('ordered-set aggregate WITHIN GROUP parses without recovery', () => {
    const sql = 'SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) FROM empsalary;';
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('WITHIN GROUP (ORDER BY SALARY)');
  });

  it('SUBSTRING(expr FOR len) SQL-standard form parses without recovery', () => {
    const sql = 'SELECT a, b, c, substring(d for 30), length(d) FROM clstr_tst;';
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('SUBSTRING(D FROM 1 FOR 30)');
  });

  it('MySQL INSERT ... VALUE (singular) parses without recovery', () => {
    const sql = "INSERT INTO ingredient(ingredient_name, count, ingredient_img_url) VALUE ('vodka', 1, 'img');";
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('VALUES');
  });

  it('PostgreSQL typename string-literal shorthand parses without recovery', () => {
    const sql = "SELECT bool 't' AS true;";
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain("BOOL 'T' AS TRUE");
  });

  it('CREATE INDEX ... ON ONLY parses without recovery', () => {
    const sql = 'CREATE INDEX idx_stage_event ON ONLY analytics_stage_events USING btree (stage_event_hash_id, project_id);';
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('ON ONLY ANALYTICS_STAGE_EVENTS');
  });

  it('CREATE INDEX ... INCLUDE (cols) parses without recovery', () => {
    const sql = 'CREATE INDEX idx_merge_requests ON merge_requests USING btree (target_project_id) INCLUDE (id, latest_merge_request_diff_id);';
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('INCLUDE (ID, LATEST_MERGE_REQUEST_DIFF_ID)');
  });

  it('exclusion constraints with EXCLUDE USING ... WITH parse without recovery', () => {
    const sql = `ALTER TABLE room_bookings
ADD CONSTRAINT room_bookings_no_overlap
EXCLUDE USING gist (room_id WITH =, tstzrange(starts_at, ends_at) WITH &&);`;

    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('EXCLUDE USING GIST');
    expect(out).toContain('WITH &&');
  });

  it('GENERATED ALWAYS AS (expr) STORED columns parse without recovery', () => {
    const sql = `CREATE TABLE users (
  id bigint,
  search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, COALESCE(name, ''::text))) STORED
);`;

    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toMatch(/GENERATED ALWAYS AS\s*\(/);
    expect(out.toUpperCase()).toContain('STORED');
  });

  it('CREATE TABLE ... PARTITION BY LIST parses without recovery', () => {
    const sql = `CREATE TABLE p_ci_builds_metadata (
  id bigint,
  partition_id bigint
) PARTITION BY LIST (partition_id);`;

    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('PARTITION BY LIST(PARTITION_ID)');
  });

  it('function signatures with double precision stay stable and recovery-free', () => {
    const sql = `CREATE OR REPLACE FUNCTION ST_Tile(rast raster, width integer, height integer, padwithnodata boolean DEFAULT FALSE, nodatavalue double precision DEFAULT NULL)
RETURNS SETOF raster AS $$
DECLARE
  initvalue double precision;
BEGIN
  RETURN;
END$$ LANGUAGE plpgsql;`;

    const out = expectIdempotentWithoutRecoveries(sql);
    expect(out).toContain('double precision');
  });

  it('SQL Server BACKUP DATABASE statement parses in strict mode', () => {
    const sql = "backup database feb2022 to disk='D:\\SQL\\BACK\\backupfeb2022.bak' with differential;";
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('BACKUP DATABASE FEB2022');
  });

  it('SQL Server BULK INSERT statement parses in strict mode', () => {
    const sql = "BULK INSERT Students FROM 'D:/SQL/students.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',');";
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('BULK INSERT STUDENTS');
    expect(out.toUpperCase()).toContain('WITH (FORMAT');
  });

  it('CREATE VIEW ... WITH READ ONLY parses without recovery', () => {
    const sql = 'CREATE OR REPLACE VIEW emp_details_view AS SELECT employee_id, first_name FROM employees WITH READ ONLY;';
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('WITH READ ONLY');
  });

  it('ClickHouse CREATE MATERIALIZED VIEW ... TO target parses without recovery', () => {
    const sql = `CREATE MATERIALIZED VIEW session_platform_sessions_mv TO session_platform_sessions
(
  session_id UInt64,
  user_id UInt64
)
AS
SELECT session_id, user_id
FROM raw_sessions;`;

    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('CREATE MATERIALIZED VIEW SESSION_PLATFORM_SESSIONS_MV TO SESSION_PLATFORM_SESSIONS');
    expect(out.toUpperCase()).toContain('AS');
  });

  it('H2 MERGE INTO table(cols) VALUES(...) shorthand parses without recovery', () => {
    const sql = "MERGE INTO genre_type (genre_id, name) VALUES (1, 'Comedy');";
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('MERGE INTO GENRE_TYPE (GENRE_ID, NAME) VALUES (1,');
  });

  it('PostgreSQL CLUSTER command parses in strict mode', () => {
    const sql = 'CLUSTER clstr_tst_c ON clstr_tst;';
    const out = expectStrictParseAndNoRecoveries(sql);
    expect(out.toUpperCase()).toContain('CLUSTER CLSTR_TST_C ON CLSTR_TST');
  });
});
