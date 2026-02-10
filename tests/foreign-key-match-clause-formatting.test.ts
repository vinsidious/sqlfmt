import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('FOREIGN KEY MATCH clause formatting', () => {
  it('keeps MATCH SIMPLE on one line in ALTER TABLE foreign-key actions', () => {
    const sql = 'ALTER TABLE x ADD CONSTRAINT fk FOREIGN KEY (col) REFERENCES y (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;';
    const out = formatSQL(sql);

    expect(out).toContain('MATCH SIMPLE');
    expect(out).not.toContain('MATCH\n        SIMPLE');
  });
});
