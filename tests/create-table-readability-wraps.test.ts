import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE readability-driven wrapping', () => {
  it('keeps constraints aligned when wide data types are present', () => {
    const sql = `CREATE TABLE orders (
    id               BIGSERIAL    PRIMARY KEY,
    org_id           UUID         NOT NULL REFERENCES organizations (id),
    user_id          BIGINT       NOT NULL REFERENCES users (id),
    status           order_status NOT NULL DEFAULT 'pending',
    currency         currency_code NOT NULL DEFAULT 'USD',
    subtotal         NUMERIC(12, 2) NOT NULL,
    tax              NUMERIC(12, 2) NOT NULL DEFAULT 0,
    total            NUMERIC(12, 2) GENERATED ALWAYS AS (subtotal + tax) STORED,
    shipping_address JSONB        NOT NULL,
    billing_address  JSONB,
    notes            TEXT,
    line_items       JSONB        NOT NULL DEFAULT '[]',
    tracking_number  TEXT,
    placed_at        TIMESTAMPTZ  DEFAULT NOW(),
    shipped_at       TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ
);`;

    const out = formatSQL(sql);

    expect(out).toBe(`CREATE TABLE orders (
    id               BIGSERIAL       PRIMARY KEY,
    org_id           UUID            NOT NULL REFERENCES organizations (id),
    user_id          BIGINT          NOT NULL REFERENCES users (id),
    status           order_status    NOT NULL DEFAULT 'pending',
    currency         currency_code   NOT NULL DEFAULT 'USD',
    subtotal         NUMERIC(12, 2)  NOT NULL,
    tax              NUMERIC(12, 2)  NOT NULL DEFAULT 0,
    total            NUMERIC(12, 2)  GENERATED ALWAYS AS (subtotal + tax) STORED,
    shipping_address JSONB           NOT NULL,
    billing_address  JSONB,
    notes            TEXT,
    line_items       JSONB           NOT NULL DEFAULT '[]',
    tracking_number  TEXT,
    placed_at        TIMESTAMPTZ     DEFAULT NOW(),
    shipped_at       TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ
);
`);
  });

  it('expands wrapped CHECK IN lists onto one value per line', () => {
    const sql = `CREATE TABLE job_queue (
    id            BIGSERIAL   PRIMARY KEY,
    queue_name    TEXT        NOT NULL DEFAULT 'default',
    payload       JSONB       NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'completed', 'failed','dead')),
    priority      INTEGER     NOT NULL DEFAULT 0,
    attempts      INTEGER     NOT NULL DEFAULT 0,
    max_attempts  INTEGER     NOT NULL DEFAULT 3,
    locked_by     TEXT,
    locked_at     TIMESTAMPTZ,
    scheduled_for TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ,
    last_error    TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);`;

    const out = formatSQL(sql);

    expect(out).toContain("status        TEXT        NOT NULL DEFAULT 'pending'");
    expect(out).toContain('CHECK(status IN (');
    expect(out).toContain("\n                                  'pending',");
    expect(out).toContain("\n                                  'processing',");
    expect(out).toContain("\n                                  'completed',");
    expect(out).toContain("\n                                  'failed',");
    expect(out).toContain("\n                                  'dead'");
    expect(out).not.toContain("'pending', 'processing', 'completed', 'failed', 'dead'");
  });
});
