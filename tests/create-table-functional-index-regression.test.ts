import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE functional and multi-valued index formatting', () => {
  it('aligns unnamed index elements at base indent and normalizes standalone line comments to block comments', () => {
    const sql = `CREATE TABLE complex_ddl_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    -- Invisible column (hidden from SELECT *)
    secret_code VARCHAR(32) INVISIBLE,

    doc JSON,

    -- Functional Index: Note the double parentheses ((...))
    -- This is MANDATORY syntax for functional key parts
    INDEX idx_func_extraction ((CAST(doc->>'$.user_id' AS UNSIGNED))),

    -- Multi-valued index (Z-order curve for array searching)
    INDEX idx_tags ((CAST(doc->'$.tags' AS CHAR(20) ARRAY)))
);`;

    const out = formatSQL(sql, { dialect: 'mysql' });
    expect(out).toBe(`CREATE TABLE complex_ddl_test (
    id          INT         AUTO_INCREMENT PRIMARY KEY,
    /* Invisible column (hidden from SELECT *) */
    secret_code VARCHAR(32) INVISIBLE,
    doc         JSON,
    /* Functional Index: Note the double parentheses ((...)) */
    /* This is MANDATORY syntax for functional key parts */
    INDEX idx_func_extraction ((CAST(doc ->> '$.user_id' AS UNSIGNED))),
    /* Multi-valued index (Z-order curve for array searching) */
    INDEX idx_tags ((CAST(doc -> '$.tags' AS CHAR(20) ARRAY)))
);
`);
  });
});
