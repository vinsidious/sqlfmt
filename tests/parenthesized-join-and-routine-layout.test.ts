import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Parenthesized JOIN and routine layout', () => {
  it('wraps parenthesized JOIN chains across multiple lines', () => {
    const sql = `SELECT e.name, p.phone
FROM ((humanresources.employee e JOIN person.person p ON (p.id = e.id))
JOIN person.address a ON (a.id = e.id));`;

    const out = formatSQL(sql);

    expect(out).toContain('\n       JOIN person.person p');
    expect(out).toContain('\n       JOIN person.address a');
    expect(out).not.toContain('FROM ((humanresources.employee e JOIN person.person p ON(p.id = e.id)) JOIN person.address a ON(a.id = e.id));');
  });

  it('formats SELECT bodies inside CREATE PROCEDURE blocks', () => {
    const out = formatSQL('CREATE PROCEDURE p() BEGIN SELECT a FROM t WHERE b = 1; END;');

    expect(out).toContain('BEGIN\n');
    expect(out).toContain('\n    SELECT a');
    expect(out).toContain('\n      FROM t');
    expect(out).toContain('\n     WHERE b = 1;');
  });

  it('aligns FULL OUTER JOIN with peer JOIN clauses', () => {
    const sql = `SELECT *
FROM catalog.executions ex (NOLOCK)
JOIN catalog.executables e (NOLOCK) ON ex.execution_id = e.execution_id
JOIN catalog.executable_statistics es (NOLOCK) ON e.executable_id = es.executable_id
                                         AND e.execution_id = es.execution_id
FULL OUTER JOIN running r (NOLOCK) ON es.execution_path = r.execution_path
WHERE e.execution_id = @execution_id;`;

    const out = formatSQL(sql);
    expect(out).toMatch(/\n  JOIN catalog\.executable_statistics AS es\(NOLOCK\)/);
    expect(out).toMatch(/\n  FULL OUTER JOIN running AS r\(NOLOCK\)/);
    expect(out).not.toMatch(/\n\s{7,}FULL OUTER JOIN/);
  });
});

describe('Parenthesized JOIN ON subquery alignment', () => {
  it('keeps nested IN subquery lines aligned within ON clause context', () => {
    const sql = `SELECT e.id, omename
  FROM password, experimenter e
  LEFT OUTER JOIN groupexperimentermap g ON (
           g.child = e.id
       AND g.parent IN (
             SELECT id FROM experimentergroup WHERE name = 'user'
           )
       )
 WHERE password.experimenter_id = e.id;`;

    const out = formatSQL(sql);

    expect(out).toMatch(/\n\s+ON \(g\.child = e\.id\n\s+AND g\.parent IN(?:\n\s+)?\(SELECT id/);
    expect(out).toMatch(/\(SELECT id\n\s{10,}FROM experimentergroup\n\s{10,}WHERE name = 'user'\)\)/);
  });
});
