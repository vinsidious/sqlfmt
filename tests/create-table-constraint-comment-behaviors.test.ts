import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE constraint and comment behaviors', () => {
  it('keeps standalone CONSTRAINT names stable across repeated formatting', () => {
    const sql = `CREATE TABLE t (
  id INT,
  CONSTRAINT ck_test
);`;

    const expected = `CREATE TABLE t (
    id INT,
    CONSTRAINT ck_test
);\n`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(once).toBe(expected);
    expect(twice).toBe(expected);
  });

  it('keeps MySQL conditional comments inside column definitions', () => {
    const sql = 'CREATE TABLE t (id bigint /*! unsigned */ not null);';
    const out = formatSQL(sql);

    expect(out).toContain('id BIGINT /*! unsigned */ NOT NULL');
    expect(out).not.toContain('/*! unsigned */\n');
    expect(out).not.toContain('\n    not NULL');
  });

  it('keeps NOT NULL together when long defaults wrap', () => {
    const sql = `CREATE TABLE commit_comments (
    id integer DEFAULT nextval('commit_comments_id_seq'::regclass) NOT NULL,
    commit_id integer NOT NULL
) WITHOUT OIDS;`;

    const out = formatSQL(sql, { maxLineLength: 80 });

    expect(out).toContain('NOT NULL');
    expect(out).not.toContain('NOT\n');
  });

  it('formats CURRENT_TIMESTAMP precision consistently in defaults and ON UPDATE', () => {
    const sql = 'CREATE TABLE t (c1 DATETIME DEFAULT CURRENT_TIMESTAMP(6), c2 DATETIME ON UPDATE CURRENT_TIMESTAMP(6));';
    const out = formatSQL(sql);

    expect(out).toContain('DEFAULT CURRENT_TIMESTAMP(6)');
    expect(out).toContain('ON UPDATE CURRENT_TIMESTAMP(6)');
    expect(out).not.toContain('CURRENT_TIMESTAMP (6)');
  });

  it('keeps NOT NULL together for long DEFAULT expressions in wide column lists', () => {
    const sql = `CREATE TABLE IF NOT EXISTS tracks
(
    id int DEFAULT nextval('tracks_id_seq'::regclass) NOT NULL,
    name VARCHAR(200) NOT NULL,
    album_id integer,
    media_type_id integer NOT NULL,
    genre_id integer,
    composer VARCHAR(220),
    milliseconds integer NOT NULL,
    bytes integer,
    unit_price numeric(10,2) NOT NULL
);`;

    const out = formatSQL(sql);

    expect(out).toContain("DEFAULT NEXTVAL('tracks_id_seq'::regclass)");
    expect(out).toContain('NOT NULL');
    expect(out).not.toContain('NOT\n');
    expect(out).not.toContain('\n                                NULL');
  });

  it('keeps INITIALLY DEFERRED on one line in ALTER TABLE constraints', () => {
    const sql = 'ALTER TABLE ONLY app_request ADD CONSTRAINT app_request_requested_id_fkey FOREIGN KEY (requested_id) REFERENCES app_person(id) DEFERRABLE INITIALLY DEFERRED;';
    const out = formatSQL(sql);

    expect(out).toContain('DEFERRABLE INITIALLY DEFERRED');
    expect(out).not.toContain('INITIALLY\n');
  });
});
