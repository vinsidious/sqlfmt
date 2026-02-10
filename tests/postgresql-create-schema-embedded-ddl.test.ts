import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('PostgreSQL CREATE SCHEMA Embedded DDL', () => {
  it('keeps embedded CREATE VIEW statements within one CREATE SCHEMA statement', () => {
    const sql = `CREATE SCHEMA pr
  CREATE VIEW b AS SELECT billofmaterialsid AS id, * FROM production.billofmaterials
  CREATE VIEW c AS SELECT cultureid AS id, * FROM production.culture
  CREATE VIEW d AS SELECT documentnode AS id, * FROM production.document
;`;

    const ast = parse(sql, { recover: false });
    expect(ast).toHaveLength(1);

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('CREATE SCHEMA pr');
    expect(out).toMatch(/CREATE SCHEMA pr\s*\n\s*CREATE VIEW b AS/i);
    expect(out).not.toMatch(/CREATE\s+SCHEMA\s+pr\s*;\s*CREATE\s+VIEW/i);
  });
});
