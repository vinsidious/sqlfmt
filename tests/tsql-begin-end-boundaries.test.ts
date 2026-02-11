import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL BEGIN END statement boundaries', () => {
  it('keeps consecutive IF BEGIN END blocks separated', () => {
    const sql = `USE ANTERO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='sa' and TABLE_NAME='sa_suorat_yo_talous_3_tuloslaskelman_toiminnot'
  AND COLUMN_NAME ='KKVALTRAH')
BEGIN
    ALTER TABLE sa.sa_suorat_yo_talous_3_tuloslaskelman_toiminnot ADD KKVALTRAH int null
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='sa' and TABLE_NAME='sa_suorat_yo_talous_3_tuloslaskelman_toiminnot'
  AND COLUMN_NAME ='KULKAT')
BEGIN
    ALTER TABLE sa.sa_suorat_yo_talous_3_tuloslaskelman_toiminnot ADD KULKAT int null
END`;

    const out = formatSQL(sql);
    expect(out).toContain('ADD kkvaltrah INT NULL;\nEND\n\nIF NOT EXISTS');
    expect(out).toContain('ADD KULKAT INT NULL\nEND');
    expect(out).not.toContain('END IF NOT EXISTS(');
  });
});
