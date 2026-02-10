import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('T-SQL CREATE INDEX and CREATE VIEW across GO batches', () => {
  it('formats CREATE VIEW query clauses after CREATE NONCLUSTERED INDEX and GO', () => {
    const sql = `CREATE NONCLUSTERED INDEX titleind ON titles (title)
GO
CREATE VIEW titleview
AS
select title, au_ord, au_lname
from authors, titles
where authors.au_id = titleauthor.au_id`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain('CREATE NONCLUSTERED INDEX titleind');
    expect(out).toContain('\nGO\n');
    expect(out).toContain('CREATE VIEW titleview AS');
    expect(out).toContain('\nSELECT title, au_ord, au_lname');
    expect(out).toContain('\n WHERE authors.au_id = titleauthor.au_id;');
    expect(out).not.toContain('select title, au_ord, au_lname');
  });
});
