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

describe('idempotency with comments', () => {
  it('SQL with line comment after keyword', () => {
    const sql = "SELECT -- comment\n id FROM t;";
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('SQL with block comment in SELECT list', () => {
    const sql = "SELECT /* pick columns */ a, b FROM t;";
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('SQL with block comment before FROM', () => {
    const sql = "SELECT a /* all columns */ FROM t WHERE x = 1;";
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('multiple statements with comments between them', () => {
    const sql = "SELECT 1;\n-- between statements\nSELECT 2;";
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('block comment between statements', () => {
    const sql = "SELECT 1;\n/* divider */\nSELECT 2;";
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('already-formatted SQL with river alignment is preserved', () => {
    const formatted = `SELECT file_hash
  FROM file_system
 WHERE file_name = '.vimrc';`;
    const once = formatSQL(formatted);
    const twice = formatSQL(once);
    expect(once.trimEnd()).toBe(formatted);
    expect(twice).toBe(once);
  });

  it('already-formatted SQL with JOIN alignment is preserved', () => {
    const formatted = `SELECT r.last_name
  FROM riders AS r
       INNER JOIN bikes AS b
       ON r.bike_vin_num = b.vin_num
          AND b.engine_tally > 2;`;
    const once = formatSQL(formatted);
    const twice = formatSQL(once);
    expect(once.trimEnd()).toBe(formatted);
    expect(twice).toBe(once);
  });
});

describe('idempotency for new SQL constructs', () => {
  it('CTE with column list', () => {
    const sql = `WITH cte (id, name) AS (SELECT 1, 'Alice') SELECT * FROM cte;`;
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('INTERVAL with precision unit', () => {
    const sql = "SELECT INTERVAL '1' DAY FROM t;";
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('GROUPS window frame', () => {
    const sql = 'SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;';
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('long IN list (20+ items) wrapping is idempotent', () => {
    const items = Array.from({ length: 25 }, (_, i) => i + 1).join(', ');
    const sql = `SELECT * FROM items WHERE id IN (${items});`;
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('wrapped OVER clause', () => {
    const sql = 'SELECT ROW_NUMBER() OVER (PARTITION BY department, region, division, category ORDER BY salary DESC) FROM employees;';
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('wrapped ARRAY constructor', () => {
    const items = Array.from({ length: 20 }, (_, i) => i + 1).join(', ');
    const sql = `SELECT ARRAY[${items}] AS big_array FROM t;`;
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });

  it('CTE with column list + INTERVAL + GROUPS window + IN list combined', () => {
    const sql = `WITH summary (dt, total) AS (SELECT order_date, SUM(amount) FROM orders GROUP BY order_date) SELECT dt, total, SUM(total) OVER (ORDER BY dt GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling FROM summary WHERE dt > INTERVAL '30' DAY AND total IN (100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500);`;
    const once = formatSQL(sql);
    const twice = formatSQL(once);
    expect(twice).toBe(once);
  });
});
