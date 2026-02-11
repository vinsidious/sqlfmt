import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Consecutive line comment layout', () => {
  it('keeps consecutive trailing line comments on separate lines after FROM sources', () => {
    const sql = `select * from Categories


--delete from Categories where ParentId = 3
--delete from Categories where Id = 3

--update Categories set ParentId = null where ParentId = 3

--delete from Categories`;

    const out = formatSQL(sql);

    expect(out).toMatch(/FROM Categories\n\s*--delete from Categories where ParentId = 3\n\s*--delete from Categories where Id = 3/);
    expect(out).toMatch(/--delete from Categories where Id = 3\n\n\s*--update Categories set ParentId = null where ParentId = 3/);
    expect(out).toMatch(/--update Categories set ParentId = null where ParentId = 3\n\n\s*--delete from Categories/);
    expect(out).not.toContain('FROM Categories --delete from Categories where ParentId = 3 --delete from Categories where Id = 3');
  });

  it('keeps blank lines between trailing line-comment groups', () => {
    const sql = `SELECT *
FROM t
-- keep first group
-- keep first group continued

-- keep second group
WHERE id = 1;`;

    const out = formatSQL(sql);

    expect(out).toMatch(/FROM t\n\s*\/\* keep first group \*\/\n\s*\/\* keep first group continued \*\/\n\n\s*\/\* keep second group \*\/\n\s*WHERE id = 1;/);
    expect(out).not.toContain('FROM t -- keep first group -- keep first group continued');
  });
});
