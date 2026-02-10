import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Semicolonless ALTER TABLE statement boundaries', () => {
  it('keeps consecutive ALTER TABLE statements separate without semicolons', () => {
    const sql = `ALTER TABLE CUSTOMER
DROP COLUMN CustomerSince

ALTER TABLE CUSTOMER
ALTER COLUMN CustomerSince datetime

ALTER TABLE CUSTOMER DROP COLUMN CustomerSince`;

    const out = formatSQL(sql);

    expect(out.match(/ALTER TABLE customer/g)?.length ?? 0).toBe(3);
    expect(out).toContain('DROP COLUMN CustomerSince;\n\nALTER TABLE customer');
    expect(out).not.toContain('ALTER COLUMN TABLE CUSTOMER');
  });
});
