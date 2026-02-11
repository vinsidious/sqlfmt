import { describe, expect, it } from 'bun:test';
import { parse } from '../src/parser';

describe('Tokenizer token count guidance', () => {
  it('includes actionable guidance when token limits are exceeded', () => {
    const sql = 'SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;';

    let message = '';
    try {
      parse(sql, { recover: false, maxTokenCount: 5 } as never);
    } catch (err) {
      message = (err as Error).message;
    }

    expect(message).toContain('Token count exceeds maximum of 5');
    expect(message).toContain('Use the maxTokenCount option to increase the limit');
  });
});
