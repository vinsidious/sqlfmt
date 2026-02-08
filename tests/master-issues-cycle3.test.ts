import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('master issues cycle 3 regressions (2026-02-08)', () => {
  it('BUG-1: supports backslash inside bracket-quoted identifiers', () => {
    const sql = "CREATE LOGIN [IIS APPPOOL\\DefaultAppPool] FROM WINDOWS;";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(formatSQL(sql)).toContain('[IIS APPPOOL\\DefaultAppPool]');
  });

  it('BUG-2: remains idempotent for #temp tables with trailing comments', () => {
    const sql = `/****** Script for SelectTopNRows command from SSMS  ******/
DROP TABLE IF EXISTS #1 --distinct: 3736804
SELECT a.[PUB_DOI]
INTO #1
FROM [dbo].[country] a`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
    expect(once).not.toContain(';;');
  });

  it('BUG-3: remains idempotent for comment blocks followed by ALTER TABLE', () => {
    const sql = `CREATE TABLE FlowerImports (
    disposalDate DATETIME NULL,
    -- 1: usable, 0: expired disposalDate DATETIME NULL,
    -- date mark expired disposalReason NVARCHAR(255) NULL;
-- reason removed
ALTER TABLE FlowerImports
    ADD COLUMN usedQuantity INT DEFAULT 0 NOT NULL;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('DIALECT-1: recognizes GO batch separator after semicolon-terminated statements', () => {
    const sql = 'USE master;\nGO\nSELECT 1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('DIALECT-2: supports bracket-quoted multipart identifiers', () => {
    expect(() => parse('SELECT * FROM [config].[GroupAccess];', { recover: false })).not.toThrow();
    expect(() => parse('SELECT A.[bt_ConversationID] FROM [userdb].[dbo].[t7] A;', { recover: false })).not.toThrow();
  });

  it('DIALECT-3: supports EXEC/EXECUTE statements', () => {
    expect(() => parse("EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'NorthAmerica';", { recover: false })).not.toThrow();
    expect(() => parse('EXECUTE data_sel;', { recover: false })).not.toThrow();
  });

  it('DIALECT-4: supports T-SQL @variable syntax in assignments and predicates', () => {
    const sql = "DECLARE @kill varchar(8000) = '';\nSELECT @kill = @kill + 'kill ';";
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(() => parse('SELECT * FROM products WHERE @product_name IS NOT NULL;', { recover: false })).not.toThrow();
  });

  it('DIALECT-5: supports Oracle START WITH / CONNECT BY hierarchical queries', () => {
    const sql = 'SELECT employee_id FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('DIALECT-6: supports DROP TABLE ... CASCADE CONSTRAINTS', () => {
    const sql = 'DROP TABLE typy CASCADE CONSTRAINTS;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(formatSQL(sql)).toContain('CASCADE CONSTRAINTS');
  });

  it('DIALECT-7: supports Oracle INSERT ALL statements', () => {
    const sql = 'INSERT ALL INTO t1 (a) VALUES (1) INTO t2 (b) VALUES (2) SELECT 1 FROM dual;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('INSERT ALL');
    expect(out).toContain('INTO t2');
  });

  it('DIALECT-8: supports CALL statements', () => {
    const sql = 'CALL my_proc();';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(formatSQL(sql)).toContain('CALL my_proc()');
  });

  it('DIALECT-9: accepts PL/SQL procedure internals with declarations', () => {
    const sql = `CREATE OR REPLACE PROCEDURE ExportSuppliersXML AS
  XDATA XMLTYPE;
BEGIN
  NULL;
END;`;
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('DIALECT-10: supports COMMENT ON TABLE/COLUMN', () => {
    expect(() => parse("COMMENT ON TABLE users IS 'table comment';", { recover: false })).not.toThrow();
    expect(() => parse("COMMENT ON COLUMN users.id IS 'id comment';", { recover: false })).not.toThrow();
  });

  it('DIALECT-11: supports GRANT system privileges without ON clause', () => {
    const sql = 'GRANT CREATE SESSION TO c##mtd;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(formatSQL(sql)).toContain('GRANT CREATE SESSION');
  });

  it('CLIENT-1: supports Oracle SQL*Plus slash execute lines', () => {
    const sql = 'BEGIN\n  NULL;\nEND;\n/';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('CLIENT-2: supports SQL*Plus SET SERVEROUTPUT ON', () => {
    expect(() => parse('SET SERVEROUTPUT ON;', { recover: false })).not.toThrow();
  });

  it('CLIENT-3: supports SQL*Plus ACCEPT command', () => {
    expect(() => parse("ACCEPT p_id NUMBER PROMPT 'id:';", { recover: false })).not.toThrow();
  });

  it('CLIENT-4: supports SQL*Plus DESCRIBE command', () => {
    expect(() => parse('DESCRIBE employees;', { recover: false })).not.toThrow();
  });

  it('CLIENT-5: supports SQL*Plus REM directive', () => {
    const sql = 'REM loading seed data\nSELECT 1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(formatSQL(sql)).toContain('REM loading seed data');
  });

  it('CLIENT-6: supports SQL*Plus @ commands', () => {
    const sql = '@set_env.sql\nSELECT 1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(formatSQL(sql)).toContain('@set_env.sql');
  });

  it('CLIENT-7: supports SQL*Plus define directive', () => {
    const sql = 'define x=1\nSELECT &x FROM dual;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('CLIENT-8: supports psql \\i meta-commands in strict mode', () => {
    const sql = '\\i FD_general.sql\nSELECT 1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    expect(formatSQL(sql)).toContain('\\i FD_general.sql');
  });

  it('EDGE-1: handles missing semicolons between statements', () => {
    const sql = 'SELECT * FROM Buy\nSELECT * FROM Flower';
    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('SELECT *\n  FROM buy;');
    expect(out).toContain('SELECT *\n  FROM flower;');
  });
});
