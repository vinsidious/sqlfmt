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
});
