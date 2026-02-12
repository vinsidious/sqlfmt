import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('INSERT values and RETURNING layout', () => {
  it('keeps moderately wide insert column and values tuples inline', () => {
    const sql = `INSERT INTO organizations (name, slug, metadata, allowed_ips, internal_network)
VALUES ('Acme Corp', 'acme-corp', '{"plan": "enterprise", "seats": 500}'::JSONB, ARRAY['10.0.0.0/8'::INET, '192.168.1.0/24'::INET], '10.0.0.0/8'::CIDR)
RETURNING id, name, created_at;

INSERT INTO products (
       sku,
       name,
       description,
       category,
       price,
       cost,
       attributes,
       tags
       )
VALUES (
       'WIDGET-001',
       'Premium Widget',
       'A high-quality widget for discerning customers',
       'widgets',
       49.99,
       12.50,
       '{"color": "blue", "material": "titanium", "warranty_months": 24}'::JSONB,
       ARRAY['premium', 'new-arrival', 'featured']
       ),
       (
       'GADGET-002',
       'Standard Gadget',
       'Reliable everyday gadget',
       'gadgets',
       29.99,
       8.00,
       '{"color": "black", "material": "aluminum"}'::JSONB,
       ARRAY['standard', 'bestseller']
       ),
       (
       'GIZMO-003',
       'Compact Gizmo',
       'Space-saving gizmo',
       'gizmos',
       19.99,
       5.50,
       '{"color": "silver", "foldable": true}'::JSONB,
       ARRAY['compact', 'sale']
       )
RETURNING id, sku, name;`;

    const expected = `INSERT INTO organizations (name, slug, metadata, allowed_ips, internal_network)
   VALUES ('Acme Corp', 'acme-corp', '{"plan": "enterprise", "seats": 500}'::JSONB, ARRAY['10.0.0.0/8'::INET, '192.168.1.0/24'::INET], '10.0.0.0/8'::CIDR)
RETURNING id, name, created_at;

INSERT INTO products (sku, name, description, category, price, cost, attributes, tags)
   VALUES ('WIDGET-001', 'Premium Widget', 'A high-quality widget for discerning customers', 'widgets', 49.99, 12.50, '{"color": "blue", "material": "titanium", "warranty_months": 24}'::JSONB, ARRAY['premium', 'new-arrival', 'featured']),
          ('GADGET-002', 'Standard Gadget', 'Reliable everyday gadget', 'gadgets', 29.99, 8.00, '{"color": "black", "material": "aluminum"}'::JSONB, ARRAY['standard', 'bestseller']),
          ('GIZMO-003', 'Compact Gizmo', 'Space-saving gizmo', 'gizmos', 19.99, 5.50, '{"color": "silver", "foldable": true}'::JSONB, ARRAY['compact', 'sale'])
RETURNING id, sku, name;`;

    expect(formatSQL(sql).trimEnd()).toBe(expected);
  });
});
