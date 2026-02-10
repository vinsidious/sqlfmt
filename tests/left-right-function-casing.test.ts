import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('LEFT and RIGHT function casing', () => {
  it('uppercases LEFT() and RIGHT() in function-call position', () => {
    const out = formatSQL('SELECT RIGHT(name, 3), LEFT(name, 2) FROM users;');

    expect(out).toContain('RIGHT(name, 3)');
    expect(out).toContain('LEFT(name, 2)');
    expect(out).not.toContain('right(name, 3)');
    expect(out).not.toContain('left(name, 2)');
  });
});

