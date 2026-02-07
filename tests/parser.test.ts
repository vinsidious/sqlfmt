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

describe('parser syntax coverage', () => {
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
    expect(out).toContain('mycustomfunc(a)');
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

  it('formatter throws on unknown node type', () => {
    expect(() => formatStatements([{ type: 'mystery' } as any])).toThrow();
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
