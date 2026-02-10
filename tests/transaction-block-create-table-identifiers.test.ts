import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Transaction block CREATE TABLE identifier casing', () => {
  it('keeps CREATE TABLE column identifiers stable within BEGIN TRANSACTION blocks', () => {
    const sql = `BEGIN TRANSACTION
CREATE TABLE Pedido_Venda (
    Codigo INT IDENTITY(1,1) NOT NULL,
    Data DATETIME NOT NULL
)
COMMIT`;

    const out = formatSQL(sql);

    expect(out).toMatch(/\bData\s+DATETIME\s+NOT NULL\b/);
    expect(out).not.toContain('DATA DATETIME NOT NULL');
  });
});
