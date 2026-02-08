import { describe, expect, it } from 'bun:test';
import { parse, visitAst } from '../src/index';

describe('visitAst', () => {
  it('traverses parsed AST nodes depth-first', () => {
    const nodes = parse('SELECT a + b AS total FROM t WHERE c = 1;');
    const seen: string[] = [];

    visitAst(nodes, {
      enter(node) {
        if (typeof node === 'object' && node !== null && 'type' in node) {
          seen.push((node as { type: string }).type);
        }
      },
    });

    expect(seen[0]).toBe('select');
    expect(seen).toContain('binary');
    expect(seen).toContain('identifier');
  });

  it('supports per-type handlers', () => {
    const nodes = parse('SELECT (a + b) * (c - d) FROM t;');
    let binaryCount = 0;

    visitAst(nodes, {
      byType: {
        binary() {
          binaryCount++;
        },
      },
    });

    expect(binaryCount).toBe(3);
  });
});
