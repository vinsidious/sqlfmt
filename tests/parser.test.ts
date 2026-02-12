import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { Parser, parse, ParseError } from '../src/parser';
import { tokenize } from '../src/tokenizer';
import { formatStatements } from '../src/formatter';

function parseFirst(sql: string) {
  const parser = new Parser(tokenize(sql));
  const nodes = parser.parseStatements();
  expect(nodes.length).toBeGreaterThan(0);
  return nodes[0] as any;
}

describe('parser syntax behaviors', () => {
  it('parses ORDER BY NULLS LAST/FIRST', () => {
    const stmt = parseFirst('SELECT * FROM t ORDER BY a DESC NULLS LAST, b NULLS FIRST;');
    expect(stmt.type).toBe('select');
    expect(stmt.orderBy.items[0].nulls).toBe('LAST');
    expect(stmt.orderBy.items[1].nulls).toBe('FIRST');
  });

  it('preserves trailing comments on ORDER BY items', () => {
    const stmt = parseFirst('SELECT * FROM t ORDER BY a, -- first sort key\nb DESC -- final key\n;');
    expect(stmt.type).toBe('select');
    expect(stmt.orderBy.items[0].trailingComment?.text).toBe('-- first sort key');
    expect(stmt.orderBy.items[1].trailingComment?.text).toBe('-- final key');
  });

  it('parses FOR UPDATE/SHARE locking clauses', () => {
    const forUpdate = parseFirst('SELECT * FROM t FOR UPDATE;');
    expect(forUpdate.lockingClause).toBe('UPDATE');

    const forShare = parseFirst('SELECT * FROM t FOR SHARE NOWAIT;');
    expect(forShare.lockingClause).toBe('SHARE NOWAIT');
  });

  it('preserves trailing comments on JOIN clauses', () => {
    const stmt = parseFirst('SELECT * FROM a JOIN b ON a.id = b.id -- join comment\nWHERE a.id > 0;');
    expect(stmt.type).toBe('select');
    expect(stmt.joins[0].trailingComment?.text).toBe('-- join comment');
    expect(stmt.where).toBeDefined();
  });

  it('parses BETWEEN with trailing AND predicates correctly', () => {
    const stmt = parseFirst('SELECT * FROM t WHERE x BETWEEN 1 AND 10 AND y = 1;');
    expect(stmt.type).toBe('select');
    expect(stmt.where.condition.type).toBe('binary');
    expect(stmt.where.condition.operator).toBe('AND');
    expect(stmt.where.condition.left.type).toBe('between');
  });

  it('parses DELETE ... USING', () => {
    const stmt = parseFirst('DELETE FROM t USING u, v WHERE t.id = u.id;');
    expect(stmt.type).toBe('delete');
    expect(stmt.using).toHaveLength(2);
  });

  it('parses UPDATE ... FROM with JOIN sources', () => {
    const stmt = parseFirst(`
UPDATE t
SET x = 1
FROM u
INNER JOIN v ON v.id = u.id
WHERE t.id = u.id;
`);
    expect(stmt.type).toBe('update');
    expect(stmt.from).toHaveLength(1);
    expect(stmt.fromJoins).toHaveLength(1);
    expect(stmt.fromJoins?.[0]?.joinType).toBe('INNER JOIN');
  });

  it('parses DELETE ... USING with JOIN sources', () => {
    const stmt = parseFirst(`
DELETE FROM t
USING u
INNER JOIN v ON v.id = u.id
WHERE t.id = u.id;
`);
    expect(stmt.type).toBe('delete');
    expect(stmt.using).toHaveLength(1);
    expect(stmt.usingJoins).toHaveLength(1);
    expect(stmt.usingJoins?.[0]?.joinType).toBe('INNER JOIN');
  });

  it('parses INSERT ... DEFAULT VALUES', () => {
    const stmt = parseFirst('INSERT INTO t DEFAULT VALUES;');
    expect(stmt.type).toBe('insert');
    expect(stmt.defaultValues).toBe(true);
  });

  it('parses UNION DISTINCT and INTERSECT ALL operators', () => {
    const stmt = parseFirst('SELECT 1 UNION DISTINCT SELECT 2 INTERSECT ALL SELECT 3;');
    expect(stmt.type).toBe('union');
    expect(stmt.operators).toEqual(['UNION DISTINCT', 'INTERSECT ALL']);
  });

  it('parses INSERT ... SELECT with UNION as query expression', () => {
    const stmt = parseFirst('INSERT INTO t SELECT id FROM a UNION SELECT id FROM b;');
    expect(stmt.type).toBe('insert');
    expect(stmt.selectQuery?.type).toBe('union');
  });

  it('parses CAST with multi-word type names', () => {
    const out = formatSQL('SELECT CAST(x AS DOUBLE PRECISION), CAST(x AS TIMESTAMP WITH TIME ZONE);');
    expect(out).toContain('CAST(x AS DOUBLE PRECISION)');
    expect(out).toContain('CAST(x AS TIMESTAMP WITH TIME ZONE)');
  });

  it('parses TIMESTAMP WITH TIME ZONE in CREATE TABLE', () => {
    const out = formatSQL('CREATE TABLE t (ts TIMESTAMP WITH TIME ZONE);');
    expect(out).toContain('TIMESTAMP WITH TIME ZONE');
  });

  it('parses TIME WITHOUT TIME ZONE in CREATE TABLE', () => {
    const out = formatSQL('CREATE TABLE t (ts TIME WITHOUT TIME ZONE);');
    expect(out).toContain('TIME WITHOUT TIME ZONE');
  });

  it('parses TRUNCATE without TABLE keyword', () => {
    const out = formatSQL('TRUNCATE foo;');
    expect(out.trim()).toBe('TRUNCATE foo;');
  });

  it('parses DROP for non-TABLE object kinds', () => {
    const out = formatSQL('DROP INDEX IF EXISTS idx_users_email;');
    expect(out.trim()).toBe('DROP INDEX IF EXISTS idx_users_email;');
  });

  it('parses ALTER for non-TABLE object kinds', () => {
    const out = formatSQL('ALTER INDEX idx_users_email RENAME TO idx_users_email_new;');
    expect(out.trim()).toBe('ALTER INDEX idx_users_email\n        RENAME TO idx_users_email_new;');
  });

  it('parses RETURNING aliases as structured expressions', () => {
    const stmt = parseFirst('UPDATE inventory SET quantity = quantity - 1 RETURNING quantity AS remaining, updated_at updated_ts;');
    expect(stmt.type).toBe('update');
    expect(stmt.returning[0].type).toBe('aliased');
    expect(stmt.returning[1].type).toBe('aliased');
    expect(stmt.returning[0].expr.type).toBe('identifier');
    expect(stmt.returning[0].alias).toBe('remaining');
  });

  it('parses CREATE INDEX column direction without raw expressions', () => {
    const stmt = parseFirst('CREATE INDEX idx_orders_date ON orders (order_date DESC, customer_id);');
    expect(stmt.type).toBe('create_index');
    expect(stmt.columns[0].type).toBe('ordered_expr');
    expect(stmt.columns[0].direction).toBe('DESC');
    expect(stmt.columns[1].type).toBe('identifier');
  });

  it('parses array subscripts and slices as structured expressions', () => {
    const stmt = parseFirst('SELECT phone_numbers[1] AS primary_phone, phone_numbers[2:3] AS alt_phones FROM employees WHERE phone_numbers[1] IS NOT NULL;');
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('array_subscript');
    expect(stmt.columns[0].expr.isSlice).toBe(false);
    expect(stmt.columns[1].expr.type).toBe('array_subscript');
    expect(stmt.columns[1].expr.isSlice).toBe(true);
    expect(stmt.where.condition.type).toBe('is');
    expect(stmt.where.condition.expr.type).toBe('array_subscript');
  });

  it('parses GRANT/REVOKE into structured fields', () => {
    const grant = parseFirst('GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO app_readwrite;');
    expect(grant.type).toBe('grant');
    expect(grant.kind).toBe('GRANT');
    expect(grant.privileges).toEqual(['SELECT', 'INSERT']);
    expect(grant.object).toBe('ALL TABLES IN SCHEMA public');
    expect(grant.recipients).toEqual(['app_readwrite']);

    const revoke = parseFirst('REVOKE SELECT ON TABLE orders FROM app_readwrite CASCADE;');
    expect(revoke.type).toBe('grant');
    expect(revoke.kind).toBe('REVOKE');
    expect(revoke.recipientKeyword).toBe('FROM');
    expect(revoke.cascade).toBe(true);
  });

  it('parses CHECK constraints using expression parser', () => {
    const stmt = parseFirst('CREATE TABLE checks_demo (CONSTRAINT qty_check CHECK(quantity BETWEEN 1 AND 99));');
    expect(stmt.type).toBe('create_table');
    expect(stmt.elements[0].elementType).toBe('constraint');
    expect(stmt.elements[0].constraintType).toBe('check');
    expect(stmt.elements[0].checkExpr?.type).toBe('between');
  });

  it('formats complex table constraints with foreign-key actions', () => {
    const sql = `CREATE TABLE orders (
      id BIGINT PRIMARY KEY,
      customer_id BIGINT NOT NULL,
      quantity INT NOT NULL,
      CONSTRAINT qty_check CHECK(quantity BETWEEN 1 AND 99),
      CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE SET NULL ON UPDATE CASCADE
    );`;
    const out = formatSQL(sql);
    expect(out).toContain('CONSTRAINT qty_check');
    expect(out).toContain('CHECK(quantity BETWEEN 1 AND 99)');
    expect(out).toContain('CONSTRAINT fk_customer');
    expect(out).toContain('FOREIGN KEY (customer_id)');
    expect(out).toContain('ON DELETE SET NULL');
    expect(out).toContain('ON UPDATE CASCADE');
  });

  it('parses ALTER actions into structured actions list', () => {
    const stmt = parseFirst('ALTER TABLE users ADD COLUMN status TEXT DEFAULT \'active\', RENAME COLUMN status TO state;');
    expect(stmt.type).toBe('alter_table');
    expect(stmt.actions).toHaveLength(2);
    expect(stmt.actions[0].type).toBe('add_column');
    expect(stmt.actions[1].type).toBe('rename_column');
  });

  it('parses LIKE/ILIKE ESCAPE clauses', () => {
    const out = formatSQL("SELECT * FROM t WHERE a LIKE '%!_%' ESCAPE '!' OR b ILIKE '%!_%' ESCAPE '!';");
    expect(out).toContain("LIKE '%!_%' ESCAPE '!'");
    expect(out).toContain("ILIKE '%!_%' ESCAPE '!'");
  });

  it('formats FROM/JOIN aliases in lowercase', () => {
    const out = formatSQL('SELECT T.ID FROM USERS AS T JOIN ORDERS AS O ON T.ID = O.USER_ID;');
    expect(out).toContain('FROM users AS t');
    expect(out).toContain('JOIN orders AS o');
  });

  it('lowercases star qualifiers', () => {
    const out = formatSQL('SELECT T.* FROM TABLE_NAME AS T;');
    expect(out).toContain('SELECT t.*');
  });

  it('does not uppercase user-defined function names', () => {
    const out = formatSQL('SELECT myCustomFunc(a), SUM(a) FROM t;');
    expect(out).toContain('myCustomFunc(a)');
    expect(out).not.toContain('MYCUSTOMFUNC(a)');
    expect(out).toContain('SUM(a)');
  });

  it('rejects non-query statements in CREATE VIEW', () => {
    expect(() => {
      const parser = new Parser(tokenize('CREATE VIEW v AS DELETE FROM t;'), { recover: false });
      parser.parseStatements();
    }).toThrow();
  });

  it('parses CTE with nested parens in body: ( (SELECT 1) )', () => {
    const out = formatSQL('WITH t AS ( (SELECT 1) ) SELECT * FROM t;');
    expect(out).toContain('SELECT 1');
    expect(out).toContain('SELECT *');
  });

  it('parses CTE body when comments appear before SELECT', () => {
    const recovered: string[] = [];
    const nodes = parse(`
WITH x AS (
  -- CTE body comment
  SELECT 1 AS id
)
INSERT INTO t (id)
SELECT id FROM x;
`, {
      recover: true,
      onRecover: (error) => recovered.push(error.message),
    });

    expect(recovered).toHaveLength(0);
    expect(nodes).toHaveLength(1);
    expect(nodes[0].type).toBe('cte');
  });

  it('parses RIGHT/LEFT as function calls in expressions', () => {
    const stmt = parseFirst(`
UPDATE tch_match_tracking t
SET is_matched = TRUE
FROM pos_events p
WHERE RIGHT(t.invoice, 6) = RIGHT(p.transaction_id, 6)
  AND LEFT(t.invoice, 2) = LEFT(p.transaction_id, 2);
`);

    expect(stmt.type).toBe('update');
    expect(stmt.where?.condition?.type).toBe('binary');
  });

  it('parses boolean predicates when comments appear before AND/OR terms', () => {
    const stmt = parseFirst(`
SELECT *
FROM tch
WHERE invoice IS NOT NULL
-- include only unmatched rows
AND NOT EXISTS (SELECT 1 FROM matched m WHERE m.invoice = tch.invoice);
`);
    expect(stmt.type).toBe('select');
    expect(stmt.where?.condition?.type).toBe('binary');
  });

  it('parses CASE expressions with inline comments after THEN results', () => {
    const stmt = parseFirst(`
SELECT CASE
  WHEN score >= 90 THEN 4 -- exact match
  WHEN score >= 70 THEN 3
  ELSE 0
END AS rank
FROM grades;
`);
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('case');
  });

  it('parses searched CASE when comments appear between CASE and first WHEN', () => {
    const stmt = parseFirst(`
SELECT CASE
  -- quality bucket
  WHEN score >= 90 THEN 'A'
  ELSE 'B'
END AS grade
FROM grades;
`);
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('case');
  });

  it('parses JOIN when comments appear between FROM item and JOIN keyword', () => {
    const stmt = parseFirst(`
SELECT *
FROM pos_events pe
-- Join to tender line
JOIN pos_transaction_lines ptl ON pe.id = ptl.event_id;
`);
    expect(stmt.type).toBe('select');
    expect(stmt.joins).toHaveLength(1);
    expect(stmt.joins[0].joinType).toBe('JOIN');
  });
});

describe('parser/code-quality safety checks', () => {
  it('advance throws when consumed past EOF', () => {
    const parser: any = new Parser(tokenize('SELECT 1;'));
    while (!parser.isAtEnd()) parser.advance();
    expect(() => parser.advance()).toThrow();
  });

  it('expect validates token value or upper matches', () => {
    const parser: any = new Parser([
      { type: 'identifier', value: 'foo', upper: 'FOO', position: 0 },
      { type: 'eof', value: '', upper: '', position: 3 },
    ]);

    expect(() => parser.expect('BAR')).toThrow();
  });

  it('formatter throws on unknown node types by default', () => {
    expect(() => formatStatements([{ type: 'mystery' } as any])).toThrow('Unknown node type');
  });

  it('formatter fallback for unknown node types is opt-in', () => {
    const out = formatStatements([{ type: 'mystery' } as any], { fallbackOnError: true });
    expect(out).toContain('formatter fallback');
  });

  it('normalizes custom dialect profiles in Parser constructor', () => {
    const customDialect = {
      name: 'ansi',
      keywords: new Set(['select', 'from']),
      functionKeywords: new Set<string>(),
      clauseKeywords: new Set(['from']),
      statementStarters: new Set(['select']),
    } as const;

    const sql = 'SELECT value FROM items;';
    const fromParse = parse(sql, { dialect: customDialect, recover: false });
    const parser = new Parser(tokenize(sql, { dialect: customDialect }), {
      dialect: customDialect,
      recover: false,
    });

    expect(parser.parseStatements()).toEqual(fromParse);
  });

  it('enforces maximum nesting depth', () => {
    const deep = 'SELECT ' + '('.repeat(240) + '1' + ')'.repeat(240) + ';';
    expect(() => formatSQL(deep)).toThrow();
  });
});

describe('parser recovery callback', () => {
  it('invokes onRecover for malformed statements in recovery mode', () => {
    const recovered: string[] = [];
    const messages: string[] = [];

    const nodes = parse('SELECT 1; SELECT (; SELECT 2;', {
      recover: true,
      onRecover: (error, raw) => {
        messages.push(error.message);
        recovered.push(raw?.text ?? '');
      },
    });

    expect(messages.length).toBe(1);
    expect(messages[0]).toContain('Expected');
    expect(recovered[0]).toContain('SELECT (;');
    expect(nodes.some(node => node.type === 'raw')).toBe(true);
  });

  it('does not call onRecover when recover is disabled', () => {
    let called = false;
    expect(() =>
      parse('SELECT (;', {
        recover: false,
        onRecover: () => {
          called = true;
        },
      })
    ).toThrow(ParseError);
    expect(called).toBe(false);
  });

  it('invokes onDropStatement when raw recovery is unavailable', () => {
    const dropped: string[] = [];
    const parser = new Parser(tokenize('SELECT (;'), {
      recover: true,
      onDropStatement: (error) => dropped.push(error.message),
    }) as any;

    parser.parseRawStatement = function () {
      this.pos = this.tokens.length - 1;
      return null;
    };

    parser.parseStatements();
    expect(dropped.length).toBe(1);
    expect(dropped[0]).toContain('Expected');
  });

  it('does not emit recovery errors for unsupported DO blocks followed by another statement', () => {
    const recovered: string[] = [];
    const nodes = parse(`
DO $$
BEGIN
  RAISE NOTICE 'hello';
END;
$$;
DROP TYPE IF EXISTS public.cloneparms CASCADE;
`, {
      recover: true,
      onRecover: (error) => recovered.push(error.message),
    });

    expect(recovered).toHaveLength(0);
    expect(nodes).toHaveLength(2);
    expect(nodes[0].type).toBe('raw');
    if (nodes[0].type !== 'raw') return;
    expect(nodes[0].reason).toBe('unsupported');
    expect(nodes[1].type).toBe('drop_table');
  });

  it('does not emit recovery errors for unsupported CREATE FUNCTION followed by comments and another statement', () => {
    const recovered: string[] = [];
    const nodes = parse(`
CREATE OR REPLACE FUNCTION public.f()
RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN 1;
END;
$$;
-- between statements
DROP FUNCTION IF EXISTS public.f();
`, {
      recover: true,
      onRecover: (error) => recovered.push(error.message),
    });

    expect(recovered).toHaveLength(0);
    expect(nodes).toHaveLength(2);
    expect(nodes[0].type).toBe('raw');
    if (nodes[0].type !== 'raw') return;
    expect(nodes[0].reason).toBe('unsupported');
    expect(nodes[1].type).toBe('drop_table');
  });

  it('parses SQL Server bracket-identifier CREATE VIEW without recovery warnings', () => {
    const recovered: string[] = [];
    const nodes = parse(`
CREATE VIEW [dbo].[vw_demo] AS
SELECT 1 AS id;
`, {
      recover: true,
      onRecover: (error) => recovered.push(error.message),
    });

    expect(recovered).toHaveLength(0);
    expect(nodes).toHaveLength(1);
    expect(nodes[0].type).toBe('create_view');
    if (nodes[0].type !== 'create_view') return;
    expect(nodes[0].name).toBe('[dbo].[vw_demo]');
  });

  it('parses T-SQL variable assignment with table hints without recovery warnings', () => {
    const recovered: string[] = [];
    const nodes = parse('SELECT @batch_num = Batch_Num FROM Trans_Num WITH (ROWLOCK, XLOCK);', {
      recover: true,
      onRecover: (error) => recovered.push(error.message),
    });

    expect(recovered).toHaveLength(0);
    expect(nodes).toHaveLength(1);
    expect(nodes[0].type).toBe('select');
  });
});

describe('parser grouping sets', () => {
  it('parses empty grouping sets: GROUP BY GROUPING SETS ((), (a))', () => {
    const stmt = parseFirst('SELECT a, COUNT(*) FROM t GROUP BY GROUPING SETS ((), (a));');
    expect(stmt.type).toBe('select');
    expect(stmt.groupBy).toBeDefined();
  });

  it('formats GROUP BY GROUPING SETS correctly', () => {
    const out = formatSQL('SELECT a FROM t GROUP BY GROUPING SETS ((), (a));');
    expect(out).toContain('GROUP BY');
    expect(out).toContain('GROUPING SETS');
  });
});

describe('parser MERGE with multiple WHEN MATCHED', () => {
  it('parses MERGE with multiple WHEN MATCHED clauses and AND conditions', () => {
    const sql = `MERGE INTO target AS t
      USING source AS s ON t.id = s.id
      WHEN MATCHED AND s.status = 'active' THEN UPDATE SET name = s.name
      WHEN MATCHED AND s.status = 'deleted' THEN DELETE
      WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name);`;
    const stmt = parseFirst(sql);
    expect(stmt.type).toBe('merge');
    expect(stmt.whenClauses.length).toBe(3);
    expect(stmt.whenClauses[0].matched).toBe(true);
    expect(stmt.whenClauses[0].action).toBe('update');
    expect(stmt.whenClauses[0].condition).toBeDefined();
    expect(stmt.whenClauses[1].matched).toBe(true);
    expect(stmt.whenClauses[1].action).toBe('delete');
    expect(stmt.whenClauses[1].condition).toBeDefined();
    expect(stmt.whenClauses[2].matched).toBe(false);
    expect(stmt.whenClauses[2].action).toBe('insert');
  });
});

describe('parser INSERT mutual exclusion (VALUES then SELECT)', () => {
  it('throws ParseError when INSERT VALUES is followed by SELECT', () => {
    expect(() => {
      const parser = new Parser(tokenize('INSERT INTO t VALUES (1) SELECT 2;'), { recover: false });
      parser.parseStatements();
    }).toThrow(ParseError);
  });

  it('throws ParseError when INSERT VALUES is followed by WITH', () => {
    expect(() => {
      const parser = new Parser(tokenize('INSERT INTO t VALUES (1) WITH x AS (SELECT 2) SELECT * FROM x;'), { recover: false });
      parser.parseStatements();
    }).toThrow(ParseError);
  });
});

describe('parser type cast with missing closing paren', () => {
  it('throws ParseError for CAST(x AS NUMERIC(10, 2 without closing paren', () => {
    expect(() => {
      const parser = new Parser(tokenize('SELECT CAST(x AS NUMERIC(10, 2;'), { recover: false });
      parser.parseStatements();
    }).toThrow(ParseError);
  });

  it('throws ParseError for incomplete CAST missing outer paren', () => {
    expect(() => {
      const parser = new Parser(tokenize('SELECT CAST(x AS INT;'), { recover: false });
      parser.parseStatements();
    }).toThrow(ParseError);
  });

  it('valid CAST with type params parses successfully', () => {
    const stmt = parseFirst('SELECT CAST(x AS NUMERIC(10, 2)) FROM t;');
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('cast');
  });
});

describe('parser GROUPS window frame', () => {
  it('parses GROUPS window frame', () => {
    const stmt = parseFirst('SELECT SUM(amount) OVER (ORDER BY date GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM sales;');
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('window_function');
    expect(stmt.columns[0].expr.frame?.unit).toBe('GROUPS');
    expect(stmt.columns[0].expr.frame?.start.kind).toBe('PRECEDING');
    expect(stmt.columns[0].expr.frame?.end?.kind).toBe('FOLLOWING');
  });

  it('parses GROUPS UNBOUNDED PRECEDING', () => {
    const stmt = parseFirst('SELECT AVG(x) OVER (ORDER BY y GROUPS UNBOUNDED PRECEDING) FROM t;');
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.frame?.unit).toBe('GROUPS');
    expect(stmt.columns[0].expr.frame?.start.kind).toBe('UNBOUNDED PRECEDING');
  });
});

describe('parser INTERVAL precision syntax', () => {
  it('parses INTERVAL with DAY unit', () => {
    const stmt = parseFirst("SELECT INTERVAL '1' DAY FROM t;");
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('interval');
    expect(stmt.columns[0].expr.value).toContain('DAY');
  });

  it('parses INTERVAL with DAY TO SECOND range', () => {
    const stmt = parseFirst("SELECT INTERVAL '1 12:00:00' DAY TO SECOND FROM t;");
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('interval');
    expect(stmt.columns[0].expr.value).toContain('DAY TO SECOND');
  });

  it('parses INTERVAL without unit (backward compat)', () => {
    const stmt = parseFirst("SELECT INTERVAL '1 day' FROM t;");
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('interval');
    expect(stmt.columns[0].expr.value).toBe("'1 day'");
  });

  it('parses INTERVAL with YEAR TO MONTH', () => {
    const stmt = parseFirst("SELECT INTERVAL '1-6' YEAR TO MONTH FROM t;");
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('interval');
    expect(stmt.columns[0].expr.value).toContain('YEAR TO MONTH');
  });
});

describe('parser InExpr discriminated union', () => {
  it('produces kind=subquery for IN (SELECT ...)', () => {
    const stmt = parseFirst('SELECT * FROM t WHERE id IN (SELECT id FROM s);');
    expect(stmt.where.condition.type).toBe('in');
    expect(stmt.where.condition.kind).toBe('subquery');
    expect(stmt.where.condition.subquery.type).toBe('subquery');
  });

  it('produces kind=list for IN (1, 2, 3)', () => {
    const stmt = parseFirst('SELECT * FROM t WHERE id IN (1, 2, 3);');
    expect(stmt.where.condition.type).toBe('in');
    expect(stmt.where.condition.kind).toBe('list');
    expect(Array.isArray(stmt.where.condition.values)).toBe(true);
  });
});

describe('parser consumeTypeSpecifier helper', () => {
  it('handles pg_cast with array suffix', () => {
    const stmt = parseFirst("SELECT x::INT[] FROM t;");
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('pg_cast');
    expect(stmt.columns[0].expr.targetType).toBe('INT[]');
  });

  it('handles CAST with parameterized type', () => {
    const stmt = parseFirst('SELECT CAST(x AS VARCHAR(255)) FROM t;');
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('cast');
    expect(stmt.columns[0].expr.targetType).toContain('VARCHAR');
  });
});

describe('parser shared parseOptionalAlias', () => {
  it('parses SELECT column alias with AS', () => {
    const stmt = parseFirst('SELECT id AS user_id FROM t;');
    expect(stmt.columns[0].alias).toBe('user_id');
  });

  it('parses SELECT column alias without AS', () => {
    const stmt = parseFirst('SELECT id user_id FROM t;');
    expect(stmt.columns[0].alias).toBe('user_id');
  });

  it('parses RETURNING alias with AS', () => {
    const stmt = parseFirst('UPDATE t SET x = 1 RETURNING x AS result;');
    expect(stmt.returning[0].type).toBe('aliased');
    expect(stmt.returning[0].alias).toBe('result');
  });

  it('parses RETURNING alias without AS', () => {
    const stmt = parseFirst('UPDATE t SET x = 1 RETURNING x result;');
    expect(stmt.returning[0].type).toBe('aliased');
    expect(stmt.returning[0].alias).toBe('result');
  });

  it('parses FROM alias with column list', () => {
    const stmt = parseFirst('SELECT * FROM generate_series(1, 3) AS g(n);');
    expect(stmt.from.alias).toBe('g');
    expect(stmt.from.aliasColumns).toEqual(['n']);
  });
});

describe('parser production-readiness regressions', () => {
  it('parses COLLATE as a collate expression (not alias)', () => {
    const stmt = parseFirst('SELECT name COLLATE "C" FROM users;');
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('collate');
    expect(stmt.columns[0].expr.collation).toBe('"C"');
  });

  it('parses DISTINCT ON expression list', () => {
    const stmt = parseFirst('SELECT DISTINCT ON (id, created_at) * FROM events ORDER BY id, created_at DESC;');
    expect(stmt.type).toBe('select');
    expect(stmt.distinct).toBe(true);
    expect(stmt.distinctOn).toHaveLength(2);
    expect(stmt.columns[0].expr.type).toBe('star');
  });

  it('parses EXPLAIN options and nested query statement', () => {
    const stmt = parseFirst('EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, FORMAT JSON) SELECT * FROM t;');
    expect(stmt.type).toBe('explain');
    expect(stmt.analyze).toBe(true);
    expect(stmt.verbose).toBe(true);
    expect(stmt.costs).toBe(false);
    expect(stmt.format).toBe('JSON');
    expect(stmt.statement.type).toBe('select');
  });

  it('parses EXPLAIN with implicit TRUE for bare option names in parentheses', () => {
    const stmt = parseFirst('EXPLAIN (ANALYZE, BUFFERS) SELECT 1;');
    expect(stmt.type).toBe('explain');
    expect(stmt.analyze).toBe(true);
    expect(stmt.buffers).toBe(true);
    expect(stmt.statement.type).toBe('select');
  });

  it('parses EXPLAIN with all boolean options as bare names', () => {
    const stmt = parseFirst('EXPLAIN (ANALYZE, VERBOSE, COSTS, BUFFERS, TIMING, SUMMARY, SETTINGS, WAL) SELECT 1;');
    expect(stmt.type).toBe('explain');
    expect(stmt.analyze).toBe(true);
    expect(stmt.verbose).toBe(true);
    expect(stmt.costs).toBe(true);
    expect(stmt.buffers).toBe(true);
    expect(stmt.timing).toBe(true);
    expect(stmt.summary).toBe(true);
    expect(stmt.settings).toBe(true);
    expect(stmt.wal).toBe(true);
  });

  it('parses EXPLAIN with explicit FALSE for boolean options', () => {
    const stmt = parseFirst('EXPLAIN (ANALYZE FALSE, BUFFERS OFF, TIMING FALSE) SELECT 1;');
    expect(stmt.type).toBe('explain');
    expect(stmt.analyze).toBe(false);
    expect(stmt.buffers).toBe(false);
    expect(stmt.timing).toBe(false);
  });

  it('parses EXPLAIN with mixed bare and explicit boolean options', () => {
    const stmt = parseFirst('EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, FORMAT JSON) SELECT * FROM users;');
    expect(stmt.type).toBe('explain');
    expect(stmt.analyze).toBe(true);
    expect(stmt.buffers).toBe(true);
    expect(stmt.costs).toBe(false);
    expect(stmt.format).toBe('JSON');
  });

  it('parses recursive CTE SEARCH/CYCLE clauses', () => {
    const stmt = parseFirst('WITH RECURSIVE t(n) AS (SELECT 1) SEARCH DEPTH FIRST BY n SET ord CYCLE n SET cyc USING path SELECT * FROM t;');
    expect(stmt.type).toBe('cte');
    expect(stmt.search?.mode).toBe('DEPTH FIRST');
    expect(stmt.search?.by).toEqual(['n']);
    expect(stmt.search?.set).toBe('ord');
    expect(stmt.cycle?.columns).toEqual(['n']);
    expect(stmt.cycle?.set).toBe('cyc');
    expect(stmt.cycle?.using).toBe('path');
  });

  it('parses CREATE TABLE column constraints into structured nodes', () => {
    const stmt = parseFirst('CREATE TABLE t (id INT NOT NULL DEFAULT 1 REFERENCES parent(id) ON DELETE CASCADE, qty INT CHECK (qty > 0), code TEXT GENERATED ALWAYS AS IDENTITY, email TEXT UNIQUE);');
    expect(stmt.type).toBe('create_table');

    const idCol = stmt.elements[0];
    expect(idCol.elementType).toBe('column');
    expect(idCol.columnConstraints?.map((c: any) => c.type)).toEqual(['not_null', 'default', 'references']);
    expect(idCol.columnConstraints?.[2].actions?.[0]).toEqual({ event: 'DELETE', action: 'CASCADE' });

    const qtyCol = stmt.elements[1];
    expect(qtyCol.columnConstraints?.[0].type).toBe('check');

    const codeCol = stmt.elements[2];
    expect(codeCol.columnConstraints?.[0].type).toBe('generated_identity');

    const emailCol = stmt.elements[3];
    expect(emailCol.columnConstraints?.[0].type).toBe('unique');
  });

  it('parses adjacent string literals as implicit concatenation', () => {
    const stmt = parseFirst("SELECT 'hello' 'world';");
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].expr.type).toBe('binary');
    expect(stmt.columns[0].expr.operator).toBe('||');
  });
});

describe('parser recovery metadata', () => {
  it('tags raw nodes with parse/unsupported/comment-only reasons', () => {
    const parseErr = parse('SELECT (;', { recover: true });
    expect(parseErr[0].type).toBe('raw');
    expect(parseErr[0].reason).toBe('parse_error');

    const unsupported = parse('VACUUM FULL users;', { recover: true });
    expect(unsupported[0].type).toBe('raw');
    expect(unsupported[0].reason).toBe('unsupported');

    const commentOnly = parse('-- just a comment', { recover: true });
    expect(commentOnly[0].type).toBe('raw');
    expect(commentOnly[0].reason).toBe('comment_only');
  });

  it('passes statement index context to onRecover callbacks', () => {
    const contexts: Array<{ statementIndex: number; totalStatements: number }> = [];
    parse('SELECT 1; SELECT (; SELECT 2;', {
      recover: true,
      onRecover: (_error, _raw, context) => contexts.push(context),
    });

    expect(contexts).toHaveLength(1);
    expect(contexts[0]).toEqual({ statementIndex: 2, totalStatements: 3 });
  });

  it('keeps statement ordering after recovery in multi-statement inputs', () => {
    const out = formatSQL('SELECT 1; SELECT (; SELECT 2;', { recover: true });
    expect(out).toContain('SELECT 1;');
    expect(out).toContain('SELECT (;');
    expect(out).toContain('SELECT 2;');
    expect(out.indexOf('SELECT 1;')).toBeLessThan(out.indexOf('SELECT 2;'));
  });

  it('does not consume IS token when not followed by valid IS-expression pattern', () => {
    // This tests the fix for the token consumption bug where tryParseIsComparison
    // would consume the IS token even when it doesn't match any expected pattern.
    // In this case, "IS" is used as a column alias, not an IS comparison operator.
    const stmt = parseFirst('SELECT col AS IS FROM t;');
    expect(stmt.type).toBe('select');
    expect(stmt.columns[0].alias).toBe('IS');
  });

  it('handles valid IS NULL and IS NOT NULL comparisons', () => {
    // Ensure valid IS comparisons still work after the fix
    const stmt = parseFirst('SELECT * FROM t WHERE x IS NULL AND y IS NOT NULL;');
    expect(stmt.type).toBe('select');
    expect(stmt.where.condition.type).toBe('binary');
    expect(stmt.where.condition.left.type).toBe('is');
    expect(stmt.where.condition.left.value).toBe('NULL');
    expect(stmt.where.condition.right.type).toBe('is');
    expect(stmt.where.condition.right.value).toBe('NOT NULL');
  });

  it('handles IS TRUE and IS FALSE comparisons', () => {
    const stmt = parseFirst('SELECT * FROM t WHERE flag IS TRUE AND disabled IS NOT FALSE;');
    expect(stmt.type).toBe('select');
    expect(stmt.where.condition.type).toBe('binary');
    expect(stmt.where.condition.left.type).toBe('is');
    expect(stmt.where.condition.left.value).toBe('TRUE');
    expect(stmt.where.condition.right.type).toBe('is');
    expect(stmt.where.condition.right.value).toBe('NOT FALSE');
  });

  it('handles IS DISTINCT FROM comparisons', () => {
    const stmt = parseFirst('SELECT * FROM t WHERE x IS DISTINCT FROM y;');
    expect(stmt.type).toBe('select');
    expect(stmt.where.condition.type).toBe('is_distinct_from');
    expect(stmt.where.condition.negated).toBe(false);
  });

  it('parses CREATE POLICY with USING clause', () => {
    const stmt = parseFirst(`CREATE POLICY "read_policy" ON my_table FOR SELECT USING (user_id = current_user);`);
    expect(stmt.type).toBe('create_policy');
    expect(stmt.name).toBe('"read_policy"');
    expect(stmt.table).toBe('my_table');
    expect(stmt.command).toBe('SELECT');
    expect(stmt.using).toBeDefined();
    expect(stmt.using.type).toBe('binary');
  });

  it('parses CREATE POLICY with WITH CHECK clause', () => {
    const stmt = parseFirst(`CREATE POLICY "insert_policy" ON my_table FOR INSERT WITH CHECK (true);`);
    expect(stmt.type).toBe('create_policy');
    expect(stmt.name).toBe('"insert_policy"');
    expect(stmt.table).toBe('my_table');
    expect(stmt.command).toBe('INSERT');
    expect(stmt.withCheck).toBeDefined();
    expect(stmt.withCheck.type).toBe('literal');
  });

  it('parses CREATE POLICY with both USING and WITH CHECK', () => {
    const stmt = parseFirst(`CREATE POLICY "update_policy" ON my_table FOR UPDATE USING (user_id = current_user) WITH CHECK (user_id = current_user);`);
    expect(stmt.type).toBe('create_policy');
    expect(stmt.command).toBe('UPDATE');
    expect(stmt.using).toBeDefined();
    expect(stmt.withCheck).toBeDefined();
  });

  it('parses CREATE POLICY with AS RESTRICTIVE', () => {
    const stmt = parseFirst(`CREATE POLICY "strict" ON my_table AS RESTRICTIVE FOR SELECT USING (true);`);
    expect(stmt.type).toBe('create_policy');
    expect(stmt.permissive).toBe('RESTRICTIVE');
    expect(stmt.command).toBe('SELECT');
  });

  it('parses CREATE POLICY with TO roles', () => {
    const stmt = parseFirst(`CREATE POLICY "admin_only" ON my_table FOR ALL TO admin, superuser USING (true);`);
    expect(stmt.type).toBe('create_policy');
    expect(stmt.command).toBe('ALL');
    expect(stmt.roles).toEqual(['admin', 'superuser']);
  });

  it('parses CREATE POLICY with schema-qualified table', () => {
    const stmt = parseFirst(`CREATE POLICY "read" ON public.my_table FOR SELECT USING (true);`);
    expect(stmt.type).toBe('create_policy');
    expect(stmt.table).toBe('public.my_table');
  });

  it('parses CREATE POLICY with function call in USING', () => {
    const stmt = parseFirst(`CREATE POLICY "auth_check" ON waitlist FOR SELECT USING (auth.role() = 'authenticated');`);
    expect(stmt.type).toBe('create_policy');
    expect(stmt.using.type).toBe('binary');
    expect(stmt.using.left.type).toBe('function_call');
  });
});
