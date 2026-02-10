import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Transaction control statement recognition', () => {
  it('parses BEGIN and related transaction statements without recovery callbacks', () => {
    const recovered: string[] = [];

    parse('BEGIN; SAVEPOINT sp1; ROLLBACK TO SAVEPOINT sp1; RELEASE SAVEPOINT sp1; COMMIT;', {
      recover: true,
      onRecover: err => recovered.push(err.message),
    });

    expect(recovered).toHaveLength(0);
  });

  it('formats BEGIN as a recognized transaction statement', () => {
    expect(formatSQL('BEGIN;')).toBe('BEGIN;\n');
  });
});
