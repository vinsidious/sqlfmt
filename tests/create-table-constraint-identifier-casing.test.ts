import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Create table constraint identifier casing', () => {
  it('normalizes primary key column identifiers consistently with column definitions', () => {
    const sql = `CREATE TABLE CART_ITEM (
    CART_ID INTEGER NOT NULL,
    PRODUCT_ID INTEGER NOT NULL,
    PRIMARY KEY ( CART_ID, PRODUCT_ID )
);`;

    const out = formatSQL(sql);
    expect(out).toContain('cart_id    INTEGER NOT NULL');
    expect(out).toContain('product_id INTEGER NOT NULL');
    expect(out).toContain('PRIMARY KEY (cart_id, product_id)');
  });

  it('normalizes foreign key and references identifiers consistently', () => {
    const sql = `CREATE TABLE CHILD_ITEM (
    CART_ID INTEGER NOT NULL,
    CONSTRAINT FK_CHILD_CART FOREIGN KEY ( CART_ID ) REFERENCES CART_ITEM ( CART_ID )
);`;

    const out = formatSQL(sql);
    expect(out).toContain('FOREIGN KEY (cart_id)');
    expect(out).toContain('REFERENCES cart_item (cart_id)');
  });
});
