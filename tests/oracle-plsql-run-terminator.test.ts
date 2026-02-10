import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Oracle PL/SQL run terminator', () => {
  it('preserves standalone slash terminator after anonymous blocks', () => {
    const sql = `BEGIN
  NULL;
END;
/`;

    const out = formatSQL(sql);

    expect(out).toContain('END;\n/');
    expect(out.trimEnd().endsWith('/')).toBe(true);
  });
});

