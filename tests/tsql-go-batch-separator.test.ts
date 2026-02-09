import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL GO batch separators', () => {
  it('keeps GO as a standalone separator after ADD DEFAULT ... FOR clauses', () => {
    const sql = `ALTER TABLE [dbo].[tbl] ADD CONSTRAINT [DF_col] DEFAULT (0) FOR [enterID]
GO
ALTER TABLE [dbo].[tbl] ADD CONSTRAINT [DF_col2] DEFAULT (0) FOR [reviewID]
GO`;

    const out = formatSQL(sql);
    expect(out).toMatch(/\bFOR \[enterID\];?\s*\n\s*GO\s*\n\s*ALTER TABLE \[dbo\]\.\[tbl\]/);
    expect(out).not.toContain('FOR [enterID] GO ALTER TABLE');
  });

  it('keeps GO as a standalone separator after SET READ_WRITE', () => {
    const sql = `ALTER DATABASE [RSM] SET READ_WRITE
GO`;

    const out = formatSQL(sql);
    expect(out).toMatch(/\bSET READ_WRITE;?\s*\n\s*GO\b/);
    expect(out).not.toContain('SET READ_WRITE GO');
  });

  it('treats GO as a standalone separator after CHECK CONSTRAINT statements', () => {
    const sql = `ALTER TABLE [dbo].[t1] CHECK CONSTRAINT [FK_1]
GO
ALTER TABLE [dbo].[t2] WITH CHECK ADD CONSTRAINT [FK_2] FOREIGN KEY ([col]) REFERENCES [dbo].[t3] ([id])
GO`;

    const out = formatSQL(sql);
    expect(out).toMatch(/ALTER TABLE \[dbo\]\.\[t1\][\s\S]*CHECK CONSTRAINT \[FK_1\]/);
    expect(out).toContain('\nGO\n');
    expect(out).toMatch(/ALTER TABLE \[dbo\]\.\[t2\][\s\S]*WITH CHECK ADD CONSTRAINT \[FK_2\]/);
    expect(out).not.toContain('CHECK CONSTRAINT [FK_1] GO');
    expect(out).not.toContain('ALTER COLUMN TABLE');
  });
});
