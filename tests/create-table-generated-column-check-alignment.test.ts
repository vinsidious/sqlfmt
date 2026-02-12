import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE generated column and table-constraint alignment', () => {
  it('aligns table-level CHECK after a generated column at the table-constraint column', () => {
    const sql = `CREATE TABLE customer_aliases (
    alias      VARCHAR(64) NOT NULL,
    alias_norm VARCHAR(64) GENERATED ALWAYS AS (LOWER(alias)) STORED,
    CHECK(alias RLIKE '^[a-z0-9_]+$')
);`;

    const out = formatSQL(sql, { dialect: 'mysql' });

    expect(out).toBe(`CREATE TABLE customer_aliases (
    alias      VARCHAR(64) NOT NULL,
    alias_norm VARCHAR(64) GENERATED ALWAYS AS (LOWER(alias)) STORED,
               CHECK(alias RLIKE '^[a-z0-9_]+$')
);
`);
  });

  it('keeps CHECK, INDEX, and FOREIGN KEY aligned at the same table-constraint column', () => {
    const sql = `CREATE TABLE alias_links (
    alias_id BIGINT NOT NULL,
    alias VARCHAR(64) NOT NULL,
    CHECK(alias RLIKE '^[a-z0-9_]+$'),
    INDEX idx_alias (alias),
    FOREIGN KEY (alias_id) REFERENCES aliases (id)
);`;

    const out = formatSQL(sql, { dialect: 'mysql' });

    expect(out).toBe(`CREATE TABLE alias_links (
    alias_id BIGINT      NOT NULL,
    alias    VARCHAR(64) NOT NULL,
             CHECK(alias RLIKE '^[a-z0-9_]+$'),
             INDEX idx_alias (alias),
             FOREIGN KEY (alias_id)
             REFERENCES aliases (id)
);
`);
  });
});
