import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('critical regressions', () => {
  it('keeps DROP ... CASCADE as one statement', () => {
    const out = formatSQL('DROP TABLE IF EXISTS temp CASCADE;');
    expect(out.trim()).toBe('DROP TABLE IF EXISTS temp CASCADE;');
  });

  it('parses ALTER TABLE DROP CONSTRAINT as structured action', () => {
    const ast = parse('ALTER TABLE users DROP CONSTRAINT pk_users;', { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('alter_table');
    if (stmt.type !== 'alter_table') return;
    expect(stmt.actions[0].type).toBe('drop_constraint');
  });

  it('parses modulo operator in expressions', () => {
    const out = formatSQL('SELECT a % b FROM test;');
    expect(out).toContain('a % b');
    expect(out).toContain('FROM test');
  });

  it('supports SELECT INTO syntax', () => {
    const out = formatSQL('SELECT * INTO temp_table FROM users;');
    expect(out).toContain('SELECT *');
    expect(out).toContain('INTO temp_table');
    expect(out).toContain('FROM users');
  });

  it('supports UPDATE aliasing', () => {
    const ast = parse("UPDATE users AS u SET status = 'inactive' WHERE u.id = 1;", { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('update');
    if (stmt.type !== 'update') return;
    expect(stmt.alias).toBe('u');
  });

  it('supports DELETE aliasing', () => {
    const ast = parse('DELETE FROM users AS u WHERE u.id = 1;', { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('delete');
    if (stmt.type !== 'delete') return;
    expect(stmt.alias).toBe('u');
  });

  it('supports UPDATE ... FROM with JOIN sources', () => {
    const sql = `
UPDATE orders AS o
SET status = 'shipped'
FROM shipments AS s
INNER JOIN carriers AS c
        ON c.id = s.carrier_id
WHERE s.order_id = o.id;
`;
    const ast = parse(sql, { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('update');
    if (stmt.type !== 'update') return;
    expect(stmt.from).toHaveLength(1);
    expect(stmt.fromJoins).toHaveLength(1);
    expect(stmt.fromJoins?.[0]?.joinType).toBe('INNER JOIN');
  });

  it('supports DELETE ... USING with JOIN sources', () => {
    const sql = `
DELETE FROM users AS u
USING sessions AS s
INNER JOIN devices AS d
        ON d.id = s.device_id
WHERE s.user_id = u.id;
`;
    const ast = parse(sql, { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('delete');
    if (stmt.type !== 'delete') return;
    expect(stmt.using).toHaveLength(1);
    expect(stmt.usingJoins).toHaveLength(1);
    expect(stmt.usingJoins?.[0]?.joinType).toBe('INNER JOIN');
  });

  it('supports INSERT OVERRIDING SYSTEM VALUE', () => {
    const ast = parse("INSERT INTO logs OVERRIDING SYSTEM VALUE VALUES (1, 'test');", { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('insert');
    if (stmt.type !== 'insert') return;
    expect(stmt.overriding).toBe('SYSTEM VALUE');
  });

  it('supports CTEs with INSERT as the main statement', () => {
    const sql = `
WITH data AS (
  SELECT *
  FROM jsonb_to_recordset('[{"outcome":"INELIGIBLE","input_fingerprint":"4908765f3f686102e4c814e6c138a9d029a939705b41b619c9a7628d0611b70f"}]'::jsonb) AS x(
    outcome text,
    input_fingerprint text
  )
)
INSERT INTO tender_outcomes (
  outcome,
  input_fingerprint
)
SELECT
  outcome::"TenderOutcomeType",
  decode(input_fingerprint, 'hex')
FROM data
ON CONFLICT (outcome) DO UPDATE SET
  input_fingerprint = EXCLUDED.input_fingerprint
WHERE tender_outcomes.input_fingerprint IS DISTINCT FROM EXCLUDED.input_fingerprint;
`;

    const ast = parse(sql, { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('cte');
    if (stmt.type !== 'cte') return;
    expect(stmt.mainQuery.type).toBe('insert');
  });

  it('preserves typed column definitions in FROM-function aliases', () => {
    const out = formatSQL(
      "SELECT * FROM jsonb_to_recordset('[{\"a\":1}]'::jsonb) AS x(a int, b text);"
    );

    expect(out).toContain('AS x(a int, b text)');
    expect(out).not.toContain('AS x(a, int');
  });

  it('supports qualified column names in MERGE SET clause', () => {
    const sql = `MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name;`;
    const ast = parse(sql, { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('merge');
    if (stmt.type !== 'merge') return;
    expect(stmt.whenClauses).toHaveLength(1);
    const whenClause = stmt.whenClauses[0];
    if (whenClause.action !== 'update') return;
    expect(whenClause.setItems).toHaveLength(1);
    expect(whenClause.setItems[0].column).toBe('t.name');
  });

  it('supports qualified column names in UPDATE SET clause', () => {
    const sql = `UPDATE users AS u SET u.status = 'active' WHERE u.id = 1;`;
    const ast = parse(sql, { recover: false });
    expect(ast).toHaveLength(1);
    const stmt = ast[0];
    expect(stmt.type).toBe('update');
    if (stmt.type !== 'update') return;
    expect(stmt.setItems).toHaveLength(1);
    expect(stmt.setItems[0].column).toBe('u.status');
  });
});

describe('ddl + comments resilience', () => {
  it('supports DROP CONCURRENTLY + IF EXISTS', () => {
    const out = formatSQL('DROP INDEX CONCURRENTLY IF EXISTS idx_users_email;');
    expect(out.trim()).toBe('DROP INDEX CONCURRENTLY IF EXISTS idx_users_email;');
  });

  it('supports DROP FUNCTION signatures', () => {
    const out = formatSQL('DROP FUNCTION IF EXISTS foo(integer, text);');
    expect(out).toContain('DROP FUNCTION IF EXISTS');
    expect(out).toContain('foo(');
  });

  it('does not split statements on inline comments after FROM', () => {
    const out = formatSQL('SELECT * FROM users -- comment\nWHERE id = 1;');
    const semicolons = out.split(';').length - 1;
    expect(semicolons).toBe(1);
    expect(out).toContain('-- comment');
    expect(out).toContain('WHERE id = 1');
  });

  it('does not split statements on mid-line block comments', () => {
    const out = formatSQL('SELECT 1 /* c */ + 2;');
    const semicolons = out.split(';').length - 1;
    expect(semicolons).toBe(1);
    expect(out).toContain('/* c */');
  });

  it('keeps unsupported CREATE FUNCTION/TRIGGER/TYPE/SEQUENCE statements intact', () => {
    const sql = `
CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE SEQUENCE IF NOT EXISTS seq_test START 1;
`;
    const out = formatSQL(sql);
    expect(out).toContain('CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;');
    expect(out).toContain('CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();');
    expect(out).toContain("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
    expect(out).toContain('CREATE SEQUENCE IF NOT EXISTS seq_test START 1;');
  });

  it('preserves multiline unsupported statements without injecting extra blank lines', () => {
    const sql = `CREATE FUNCTION f() RETURNS text AS $func$
BEGIN
  RETURN $$text $$ inside$$;
END;
$func$ LANGUAGE plpgsql;`;
    const out = formatSQL(sql).trim();
    expect(out).toContain('$func$ LANGUAGE plpgsql;');
    expect(out).not.toContain('$func$\n\nLANGUAGE');
  });
});
