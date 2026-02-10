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

  it('separates slash-terminated block from following statements', () => {
    const sql = `BEGIN
  NULL;
END;
/
SELECT 1 FROM dual;`;

    const out = formatSQL(sql);

    expect(out).toContain('END;\n/\n\nSELECT 1');
    expect(out).toContain('FROM dual;');
  });
});
