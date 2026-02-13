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

    expect(() => parse(sql, { recover: false, dialect: 'tsql' })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      dialect: 'tsql',
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

  it('NONCLUSTERED INDEX with INCLUDE and WHERE aligns river through header', () => {
    const sql = `CREATE NONCLUSTERED INDEX IX_Products_Category ON dbo.Products (Category, SubCategory) INCLUDE (ProductName, UnitPrice) WHERE IsDiscontinued = 0;`;
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out.trimEnd()).toBe(
      ` CREATE NONCLUSTERED INDEX IX_Products_Category\n` +
      `     ON dbo.Products (Category, SubCategory)\n` +
      `INCLUDE (ProductName, UnitPrice)\n` +
      `  WHERE IsDiscontinued = 0;`
    );
  });

  it('NONCLUSTERED INDEX with INCLUDE only aligns river through header', () => {
    const sql = `CREATE NONCLUSTERED INDEX IX_Employees_Dept_Region ON dbo.Employees (DepartmentName, Region) INCLUDE (FullName, Salary);`;
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out.trimEnd()).toBe(
      ` CREATE NONCLUSTERED INDEX IX_Employees_Dept_Region\n` +
      `     ON dbo.Employees (DepartmentName, Region)\n` +
      `INCLUDE (FullName, Salary);`
    );
  });

  it('NONCLUSTERED INDEX with INCLUDE across GO batches', () => {
    const sql = `CREATE NONCLUSTERED INDEX IX_Products_Category ON dbo.Products (Category, SubCategory) INCLUDE (ProductName, UnitPrice) WHERE IsDiscontinued = 0;
GO
CREATE NONCLUSTERED INDEX IX_Employees_Dept_Region ON dbo.Employees (DepartmentName, Region) INCLUDE (FullName, Salary);
GO`;
    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      dialect: 'tsql',
      onRecover: err => recoveries.push(err.message),
    });
    expect(recoveries).toEqual([]);
    expect(out).toContain(' CREATE NONCLUSTERED INDEX IX_Products_Category');
    expect(out).toContain('\nINCLUDE (ProductName, UnitPrice)');
    expect(out).toContain('\n  WHERE IsDiscontinued = 0;');
    expect(out).toContain('\nGO\n');
    expect(out).toContain(' CREATE NONCLUSTERED INDEX IX_Employees_Dept_Region');
    expect(out).toContain('\nINCLUDE (FullName, Salary);');
  });
});
