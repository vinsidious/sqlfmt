import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL IF controlled statement spacing', () => {
  it('keeps IF and controlled CREATE statement adjacent', () => {
    const sql = `If not Exists (select loginname from master.dbo.syslogins where name = 'test_login_ddladmin')
CREATE LOGIN [test_login_ddladmin] WITH PASSWORD = 'test_login_ddladmin', CHECK_POLICY = OFF;`;

    const out = formatSQL(sql, { dialect: 'tsql', recover: true });
    expect(out).not.toContain(")\n\nCREATE LOGIN");
    expect(out).toContain("If not Exists (select loginname from master.dbo.syslogins where name = 'test_login_ddladmin')\nCREATE LOGIN [test_login_ddladmin]");
  });
});
