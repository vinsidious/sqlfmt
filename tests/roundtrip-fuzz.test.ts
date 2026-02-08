import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

function assertRoundtrip(sql: string): void {
  const once = formatSQL(sql);
  const twice = formatSQL(once);
  expect(twice).toBe(once);
}

describe('roundtrip stability', () => {
  const fixtures = [
    "SELECT * INTO temp_users FROM users WHERE id % 2 = 0;",
    "UPDATE users AS u SET status = 'inactive' FROM sessions s WHERE u.id = s.user_id;",
    "DELETE FROM users AS u USING sessions s WHERE u.id = s.user_id RETURNING u.id;",
    "INSERT INTO logs OVERRIDING SYSTEM VALUE VALUES (1, 'ok');",
    'DROP TABLE IF EXISTS temp CASCADE;',
    'DROP FUNCTION IF EXISTS foo(integer, text);',
    'ALTER TABLE users DROP CONSTRAINT pk_users;',
    'BEGIN; COMMIT; ROLLBACK;',
    'CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;',
    "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');",
  ];

  for (const sql of fixtures) {
    it(`roundtrips: ${sql.slice(0, 40)}...`, () => {
      assertRoundtrip(sql);
    });
  }
});

describe('fuzz idempotency', () => {
  it('remains idempotent for randomized SELECT predicates', () => {
    let seed = 0x5eed1234;
    const rand = () => {
      seed = (seed * 1664525 + 1013904223) >>> 0;
      return seed / 0xffffffff;
    };

    const cols = ['id', 'user_id', 'status', 'created_at', 'amount', 'kind'];
    const ops = ['=', '<>', '>', '<', '>=', '<='];
    const vals = ["'active'", "'inactive'", '1', '2', '100', 'TRUE', 'FALSE'];
    const bools = ['AND', 'OR'];

    for (let i = 0; i < 100; i++) {
      const terms = 2 + Math.floor(rand() * 5);
      const parts: string[] = [];
      for (let t = 0; t < terms; t++) {
        const c = cols[Math.floor(rand() * cols.length)];
        const o = ops[Math.floor(rand() * ops.length)];
        const v = vals[Math.floor(rand() * vals.length)];
        parts.push(`${c} ${o} ${v}`);
        if (t < terms - 1) {
          parts.push(bools[Math.floor(rand() * bools.length)]);
        }
      }

      const sql = `SELECT ${cols[Math.floor(rand() * cols.length)]}, ${cols[Math.floor(rand() * cols.length)]} FROM events WHERE ${parts.join(' ')};`;
      assertRoundtrip(sql);
    }
  });
});
