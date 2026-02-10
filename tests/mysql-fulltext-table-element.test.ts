import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('MySQL FULLTEXT table elements', () => {
  it('parses and formats FULLTEXT index table elements', () => {
    const sql = `CREATE TABLE productnotes (
  note_id   INT  NOT NULL AUTO_INCREMENT,
  note_text TEXT NULL,
  PRIMARY KEY(note_id),
  FULLTEXT(note_text)
) ENGINE=MyISAM;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('FULLTEXT (note_text)');
    expect(out).toContain('ENGINE = MyISAM;');
    expect(out).not.toContain(') ) ENGINE');
  });
});

