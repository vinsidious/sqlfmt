import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE terminal constraint comma handling', () => {
  it('does not add a comma after the last table constraint', () => {
    const sql = `CREATE TABLE [HumanResources].[EmployeeDepartmentHistory](
    [BusinessEntityID] [int] NOT NULL,
    [DepartmentID] [smallint] NOT NULL,
    [ShiftID] [tinyint] NOT NULL,
    [StartDate] [date] NOT NULL,
    [EndDate] [date] NULL,
    [ModifiedDate] [datetime] NOT NULL
        CONSTRAINT [DF_EmployeeDepartmentHistory_ModifiedDate] DEFAULT (GETDATE()),
    CONSTRAINT [CK_EmployeeDepartmentHistory_EndDate]
        CHECK (([EndDate] >= [StartDate]) OR ([EndDate] IS NULL))
) ON [PRIMARY];`;

    const out = formatSQL(sql);
    expect(out).toContain('CONSTRAINT [CK_EmployeeDepartmentHistory_EndDate]');
    expect(out).not.toMatch(/\)\),\n\) ON \[PRIMARY\];/);
  });
});
