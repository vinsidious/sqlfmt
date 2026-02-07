import { describe, expect, it } from 'bun:test';
import { readFileSync } from 'fs';
import { join } from 'path';
import { formatSQL } from '../src/format';

function skipWhitespace(text: string, i: number): number {
  while (i < text.length && /\s/.test(text[i])) i++;
  return i;
}

function readStringLiteral(text: string, i: number): { value: string; next: number } {
  const quote = text[i];
  if (quote !== "'" && quote !== '"' && quote !== '`') {
    throw new Error(`Expected string literal at index ${i}`);
  }

  let j = i + 1;
  while (j < text.length) {
    const ch = text[j];
    if (ch === '\\') {
      j += 2;
      continue;
    }
    if (ch === quote) {
      const literal = text.slice(i, j + 1);
      // eslint-disable-next-line no-new-func
      const value = Function(`return ${literal};`)() as string;
      return { value, next: j + 1 };
    }
    j++;
  }

  throw new Error(`Unterminated string literal at index ${i}`);
}

function skipExpression(text: string, i: number): number {
  let depth = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === "'" || ch === '"' || ch === '`') {
      const literal = readStringLiteral(text, i);
      i = literal.next;
      continue;
    }
    if (ch === '(' || ch === '[' || ch === '{') {
      depth++;
      i++;
      continue;
    }
    if (ch === ')' || ch === ']' || ch === '}') {
      depth--;
      i++;
      continue;
    }
    if (ch === ',' && depth === 0) {
      return i;
    }
    i++;
  }
  return i;
}

function extractFormatterInputs(): string[] {
  const file = join(import.meta.dir, 'formatter.test.ts');
  const text = readFileSync(file, 'utf8');
  const inputs: string[] = [];

  let i = 0;
  while (i < text.length) {
    const idx = text.indexOf('assertFormat(', i);
    if (idx === -1) break;
    if (text.slice(Math.max(0, idx - 9), idx) === 'function ') {
      i = idx + 'assertFormat('.length;
      continue;
    }

    let pos = idx + 'assertFormat('.length;
    pos = skipExpression(text, pos); // first arg
    if (text[pos] !== ',') throw new Error(`Malformed assertFormat call near index ${idx}`);
    pos++;
    pos = skipWhitespace(text, pos);
    if (text[pos] !== '`' && text[pos] !== "'" && text[pos] !== '"') {
      i = pos + 1;
      continue;
    }

    const second = readStringLiteral(text, pos);
    inputs.push(second.value);
    i = second.next;
  }

  return inputs;
}

describe('idempotency', () => {
  it('formats all formatter-test inputs idempotently', () => {
    const inputs = extractFormatterInputs();
    expect(inputs.length).toBeGreaterThan(0);

    for (const sql of inputs) {
      const once = formatSQL(sql);
      const twice = formatSQL(once);
      expect(twice).toBe(once);
    }
  });
});
