import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('SQLite INSERT OR conflict actions', () => {
  it('parses INSERT OR variants in strict mode', () => {
    const sql = `INSERT OR IGNORE INTO t VALUES (1);
INSERT OR REPLACE INTO t VALUES (2);
INSERT OR ROLLBACK INTO t VALUES (3);
INSERT OR ABORT INTO t VALUES (4);
INSERT OR FAIL INTO t VALUES (5);`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('formats recursive inserts that use OR IGNORE', () => {
    const sql = `WITH RECURSIVE
  edge_seed(cnt, previous_node, next_node) AS (
    SELECT 1, 2, 3
    UNION ALL
    SELECT cnt + 1, previous_node + 1, next_node + 1
      FROM edge_seed
     WHERE cnt < 10
  )
INSERT OR IGNORE INTO edges(previous_node, next_node)
SELECT previous_node, next_node
  FROM edge_seed;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('INSERT OR IGNORE INTO edges (previous_node, next_node)');
  });
});
