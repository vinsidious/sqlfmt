import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('CREATE TABLE default expression wrapping', () => {
  it('keeps DEFAULT and its expression together when column alignment wraps', () => {
    const sql = `CREATE TABLE "community" (
    "id" int8 NOT NULL DEFAULT nextval('community_id_seq'::regclass),
    "description" varchar(255) NOT NULL COLLATE "default",
    "name" varchar(255) NOT NULL COLLATE "default"
);`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain("DEFAULT NEXTVAL('community_id_seq'::regclass)");
    expect(out).not.toContain('DEFAULT\n');
  });
});
