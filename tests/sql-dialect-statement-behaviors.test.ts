import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';
import { tokenize } from '../src/tokenizer';

function expectStrictAndRecoveryFree(sql: string): string {
  const recoveries: string[] = [];
  expect(() =>
    parse(sql, {
      recover: false,
      onRecover: err => recoveries.push(err.message),
    })
  ).not.toThrow();
  expect(recoveries).toEqual([]);
  return formatSQL(sql, { recover: false });
}

describe('SQL Dialect Statement Behaviors', () => {
  it('handles COPY FROM STDIN rows that include backslashes', () => {
    const sql = `COPY public.entity_revision (id, user_id, object_id, object_type, create_timestamp, action, message) FROM stdin;
1\t1\t1\tMapasCulturais\\\\Entities\\\\Agent\t2019-03-07 00:00:00\tcreated\tRegistro criado.
\\.
SELECT 1;`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('COPY public.entity_revision');
    expect(out).toContain('MapasCulturais\\\\Entities\\\\Agent');
    expect(out).toContain('\\.');
    expect(out).toContain('SELECT 1;');
  });

  it('parses SELECT lists with consecutive commented-out column lines', () => {
    const sql = `SELECT
  user_id,
  --col1,
  --col2,
  --col3,
  name
FROM posts;`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('--col1,');
    expect(out).toContain('--col2,');
    expect(out).toContain('--col3,');
    expect(out).toContain('name');
  });

  it('does not inject aliases when commented-out column lines are adjacent', () => {
    const sql = `SELECT
  user_id,
  --col1,
  --col2,
  name
FROM posts;`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).not.toContain('--col2, AS name');
    expect(out).toContain('name');
  });

  it('parses MERGE USING subqueries with aliases', () => {
    const sql = `MERGE INTO t1 t USING (SELECT 1 AS a) s
  ON t.a = s.a
  WHEN MATCHED THEN UPDATE SET b = 1
  WHEN NOT MATCHED THEN INSERT VALUES (s.a, 0);`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('MERGE INTO t1 AS t');
    expect(out).toContain('USING (SELECT 1 AS a) AS s');
  });

  it('parses EXPLAIN wrapping INSERT statements', () => {
    const sql = `EXPLAIN (costs off) INSERT INTO t VALUES ('a', 'b') RETURNING *;`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('EXPLAIN (COSTS OFF)');
    expect(out).toContain('INSERT INTO t');
  });

  it('parses EXPLAIN wrapping UPDATE statements', () => {
    const sql = `EXPLAIN UPDATE t SET a = 1 WHERE b = 2;`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('EXPLAIN');
    expect(out).toContain('UPDATE t');
  });

  it('parses EXPLAIN wrapping DELETE statements', () => {
    const sql = `EXPLAIN DELETE FROM t WHERE a = 1;`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('EXPLAIN');
    expect(out).toContain('DELETE');
  });

  it('parses CREATE POLICY USING clauses containing inline comments', () => {
    const sql = `CREATE POLICY "test" ON t
FOR SELECT USING (
  -- allow tenant and admin
  tenant_id = auth.uid()
  OR is_admin = TRUE
);`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('CREATE POLICY "test"');
    expect(out).toContain('USING (');
    expect(out).toContain('tenant_id = auth.uid()');
    expect(out).toContain('OR is_admin = TRUE');
  });

  it('parses MySQL INTERVAL numeric unit arithmetic expressions', () => {
    const sql = `SELECT * FROM t WHERE purchase_date >= CURDATE() - INTERVAL 30 DAY;`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('INTERVAL 30 DAY');
  });

  it('tokenizes Oracle q-quoted string literals', () => {
    const sql = `INSERT INTO t VALUES ('id', null, q'[Arta'n Dar]', q'[description]', null);`;
    expect(() => tokenize(sql)).not.toThrow();
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain(`q'[Arta'n Dar]'`);
    expect(out).toContain(`q'[description]'`);
  });

  it('parses SQL Server BACKUP statements followed by GO after semicolon', () => {
    const sql = `BACKUP DATABASE BsAll TO DISK = 'C:\\Backup\\BsAll.bak'
WITH INIT, FORMAT, COMPRESSION;
GO
SELECT 1;`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('BACKUP DATABASE BsAll TO DISK');
    expect(out).toContain('\nGO\n');
    expect(out).toContain('SELECT 1;');
  });

  it('parses Oracle INSERT RETURNING ... INTO clauses', () => {
    const sql = `INSERT INTO QUESTAO (id) VALUES (SEQ_QUESTAO.NEXTVAL)
RETURNING ID_QUESTAO INTO v_id_questao;`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toMatch(/RETURNING\s+id_questao\s+INTO\s+v_id_questao/i);
  });

  it('normalizes Oracle CREATE FUNCTION RETURN signatures', () => {
    const sql = `create or replace function fn(date_in in date)
return boolean
as
begin
  return true;
end;`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('CREATE OR REPLACE FUNCTION');
    expect(out).toContain('RETURN BOOLEAN');
  });

  it('parses CREATE VIEW WITH option lists before AS', () => {
    const sql = `CREATE VIEW v1 WITH (security_invoker=true) AS SELECT * FROM t1;`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('CREATE VIEW v1 WITH (security_invoker = TRUE) AS');
  });

  it('parses CREATE VIEW AS VALUES bodies', () => {
    const sql = `CREATE VIEW v1 AS VALUES(1, 2);`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('CREATE VIEW v1 AS');
    expect(out).toContain('VALUES');
  });

  it('parses PostgreSQL ISNULL and NOTNULL postfix predicates', () => {
    const sql = `SELECT * FROM t WHERE b isnull OR c notnull;`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain('b IS NULL');
    expect(out).toContain('c IS NOT NULL');
  });

  it('parses psql quoted variable interpolation forms', () => {
    const sql = `SELECT * FROM t WHERE name = :'db_name' OR schema_name = :\"schema_name\";`;
    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toContain(`:'db_name'`);
    expect(out).toContain(':\"schema_name\"');
  });

  it('keeps ALTER PUBLICATION ... ADD TABLE semantics', () => {
    const sql = 'alter publication supabase_realtime add table profiles;';
    const out = expectStrictAndRecoveryFree(sql);

    expect(out).toMatch(/ALTER PUBLICATION supabase_realtime[\s\S]*ADD TABLE profiles;/i);
    expect(out).not.toContain('ADD COLUMN');
    expect(out).not.toContain('ADD TABLE table');
  });

  it('formats PIVOT as a clause following FROM sources', () => {
    const sql = `SELECT *
FROM (
  SELECT [ReaderId], datename(month, [Read]) AS [Month], [Average]
  FROM dbo.x
) AS [source]
PIVOT(
  AVG([Average])
  FOR [Month] IN([April], [May])
) AS PivotTable;`;

    const out = expectStrictAndRecoveryFree(sql);
    expect(out).toMatch(/\)\s+AS \[source\]\s*\n\s*PIVOT\s*\(/);
    expect(out).toContain('FOR [Month] IN ([April], [May])');
  });
});
