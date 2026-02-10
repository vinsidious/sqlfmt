import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('CREATE TABLE ON UPDATE DEFAULT alignment', () => {
  it('keeps DEFAULT column alignment consistent when ON UPDATE wraps', () => {
    const sql = `CREATE TABLE t (
  marker TIMESTAMP WITH TIME ZONE,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`;

    const out = formatSQL(sql);
    const createdLine = out.split('\n').find(line => line.includes('created_date')) ?? '';
    const updatedLine = out.split('\n').find(line => line.includes('updated_date')) ?? '';

    const createdGap = createdLine.match(/TIMESTAMP( +)DEFAULT/)?.[1] ?? '';
    const updatedGap = updatedLine.match(/TIMESTAMP( +)DEFAULT/)?.[1] ?? '';

    expect(out).toContain('ON UPDATE CURRENT_TIMESTAMP');
    expect(createdGap.length).toBeGreaterThan(0);
    expect(updatedGap.length).toBeGreaterThan(0);
    expect(updatedGap.length).toBe(createdGap.length);
  });
});

