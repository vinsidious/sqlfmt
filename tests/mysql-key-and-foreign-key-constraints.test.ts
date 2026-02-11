import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('MySQL table key and foreign key constraints', () => {
  it('formats standalone KEY constraints without collapsing following constraints', () => {
    const sql = `CREATE TABLE t (
   id INT NOT NULL,
   KEY (id),
   FOREIGN KEY (id) REFERENCES other (id) ON DELETE CASCADE,
   PRIMARY KEY (id)
);`;

    const out = formatSQL(sql);
    expect(out).toContain('\n       KEY (id),\n');
    expect(out).toContain('\n    FOREIGN KEY (id)\n');
    expect(out).toContain('\n    PRIMARY KEY (id)\n');
    expect(out).not.toContain(') , FOREIGN KEY');
    expect(out).not.toContain('KEY (   id');
  });

  it('does not add empty parentheses when REFERENCES omits target columns', () => {
    const sql = `CREATE TABLE t (
    a INT,
    FOREIGN KEY (a) REFERENCES other
);`;

    const out = formatSQL(sql);
    expect(out).toContain('REFERENCES other');
    expect(out).not.toContain('REFERENCES other ()');
  });

  it('uppercases KEY and keeps spacing before parenthesis for named keys', () => {
    const sql = `CREATE TABLE t (
    id INT,
    key idx_a (id),
    UNIQUE KEY idx_b (id)
);`;

    const out = formatSQL(sql);
    expect(out).toContain('KEY idx_a (id)');
    expect(out).toContain('UNIQUE KEY idx_b (id)');
    expect(out).not.toContain('key idx_a(');
  });
});
