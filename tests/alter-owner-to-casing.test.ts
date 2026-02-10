import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('ALTER ... OWNER TO keyword casing', () => {
  it('uppercases OWNER TO in ALTER DATABASE and ALTER TABLE statements', () => {
    const sql = `alter database db owner to user1;
alter table t owner to user1;`;

    const out = formatSQL(sql).trimEnd();

    expect(out).toBe(`ALTER DATABASE db
        OWNER TO user1;

ALTER TABLE t
        OWNER TO user1;`);
  });
});
