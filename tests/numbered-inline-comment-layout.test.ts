import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Numbered inline comment layout', () => {
  it('keeps number prefix and inline comment on the same line', () => {
    const out = formatSQL('12. -- [12]');
    expect(out).toBe('12. -- [12]\n');
  });
});
