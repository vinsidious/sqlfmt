import { describe, expect, it } from 'bun:test';
import * as api from '../src/index';

describe('public API surface', () => {
  it('exports formatSQL plus parser/tokenizer APIs', () => {
    expect(typeof api.formatSQL).toBe('function');
    expect(typeof (api as any).tokenize).toBe('function');
    expect(typeof (api as any).Parser).toBe('function');
    expect(typeof (api as any).parse).toBe('function');
    expect(typeof (api as any).ParseError).toBe('function');
  });

  it('accepts options on formatSQL for depth limits', () => {
    const deep = 'SELECT ' + '('.repeat(40) + '1' + ')'.repeat(40) + ';';
    expect(() => api.formatSQL(deep, { maxDepth: 20 })).toThrow();
  });

  it('accepts maxLineLength and changes wrapping behavior', () => {
    const sql = 'SELECT customer_identifier, product_identifier, order_identifier, shipment_identifier FROM very_long_table_name;';
    const defaultOut = api.formatSQL(sql);
    const wideOut = api.formatSQL(sql, { maxLineLength: 140 });
    expect(defaultOut).toContain('\n       product_identifier');
    expect(wideOut).toContain('SELECT customer_identifier, product_identifier, order_identifier, shipment_identifier');
  });

  it('exposes parse() convenience API with recovery context callbacks', () => {
    const contexts: Array<{ statementIndex: number; totalStatements: number }> = [];
    const nodes = api.parse('SELECT 1; SELECT (; SELECT 2;', {
      recover: true,
      onRecover: (_error, _raw, context) => contexts.push(context),
    });

    expect(nodes.length).toBe(3);
    expect(contexts).toEqual([{ statementIndex: 2, totalStatements: 3 }]);
  });

  it('exports version as a non-empty string', () => {
    expect(typeof api.version).toBe('string');
    expect(api.version.length).toBeGreaterThan(0);
  });
});
