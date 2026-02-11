import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

function buildDenseInsertDump(statementCount: number, rowsPerStatement: number): string {
  const statements: string[] = [
    'CREATE TABLE telemetry (id INT, category TEXT, reading INT, created_at TIMESTAMP);',
  ];

  let id = 1;
  for (let s = 0; s < statementCount; s++) {
    const rows: string[] = [];
    for (let r = 0; r < rowsPerStatement; r++) {
      rows.push(`(${id}, 'sensor', ${id % 1000}, NOW())`);
      id++;
    }
    statements.push(`INSERT INTO telemetry (id, category, reading, created_at) VALUES ${rows.join(', ')};`);
  }

  return statements.join('\n');
}

describe('Large insert dump resilience', () => {
  it('formats many INSERT statements with dense VALUES lists', () => {
    const sql = buildDenseInsertDump(220, 45);
    const out = formatSQL(sql);

    expect(out).toContain('CREATE TABLE telemetry');
    expect(out).toContain('INSERT INTO telemetry');
    expect(out.length).toBeGreaterThan(300_000);
  }, 30000);
});
