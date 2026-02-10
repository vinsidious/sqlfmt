import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Qualified identifiers that match keywords', () => {
  it('preserves identifier casing in REFERENCES when object names match reserved words', () => {
    const sql = `ALTER TABLE ONLY public.address
    ADD CONSTRAINT c1 FOREIGN KEY (user_id) REFERENCES public.user (id);`;

    const out = formatSQL(sql);
    expect(out).toContain('REFERENCES public.user (id)');
    expect(out).not.toContain('REFERENCES public.USER');
  });

  it('preserves identifier casing in ALTER DOMAIN statements', () => {
    const sql = `CREATE DOMAIN public.year AS integer;
ALTER DOMAIN public.year OWNER TO postgres;`;

    const out = formatSQL(sql);
    expect(out).toContain('CREATE DOMAIN public.year AS integer;');
    expect(out).toContain('ALTER DOMAIN public.year\n        OWNER TO postgres;');
    expect(out).not.toContain('ALTER DOMAIN public.YEAR');
  });

  it('keeps PRIMARY KEY column identifiers lowercase when names match keyword text', () => {
    const sql = `CREATE TABLE user_custom_fields (
    name TEXT NOT NULL,
    user_id NUMERIC NOT NULL,
    value TEXT,
    PRIMARY KEY (user_id, name, value)
);`;

    const out = formatSQL(sql);
    expect(out).toContain('PRIMARY KEY (user_id, name, value)');
    expect(out).not.toContain('PRIMARY KEY (user_id, name, VALUE)');
  });

  it('keeps DROP DOMAIN target names as identifiers', () => {
    const out = formatSQL('DROP DOMAIN year;');
    expect(out).toContain('DROP DOMAIN year;');
    expect(out).not.toContain('DROP DOMAIN YEAR;');
  });

  it('keeps schema-qualified table names lowercase in trigger targets', () => {
    const sql = `create trigger set_updated_at_user
before update on public.user
for each row
execute function public.set_updated_at();`;

    const out = formatSQL(sql);
    expect(out).toContain('BEFORE UPDATE ON public.user');
    expect(out).not.toContain('ON public.USER');
  });
});
