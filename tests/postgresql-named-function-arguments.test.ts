import { describe, expect, it } from 'bun:test';
import { formatSQL, parse } from '../src/index';

describe('PostgreSQL named function arguments', () => {
  it('parses function-call arguments written with := syntax', () => {
    const sql = "SELECT create_distributed_table('lineitem', 'l_orderkey', 'hash', shard_count := 2, colocate_with := 'lineitem');";

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql);
    expect(out).toContain('shard_count := 2');
    expect(out).toContain("colocate_with := 'lineitem'");
  });
});
