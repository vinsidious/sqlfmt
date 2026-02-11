import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('PL/SQL BEGIN END keyword casing', () => {
  it('applies consistent keyword casing to begin and end in a block', () => {
    const sql = `begin
wwv_flow_api.remove_flow(wwv_flow.g_flow_id);
end;`;

    const out = formatSQL(sql);
    expect(out).toContain('BEGIN');
    expect(out).toContain('\nEND;');
    expect(out).not.toContain('\nend;');
  });
});
