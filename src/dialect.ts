/**
 * Optional dialect extension points for parser/tokenizer behavior.
 *
 * holywell remains PostgreSQL-first by default. Provide this object when you need
 * to teach the tokenizer/parser about vendor-specific keywords or clause words.
 */
export interface SQLDialect {
  /**
   * Extra words that should be recognized as SQL keywords during tokenization.
   *
   * Useful for vendor-specific statements/functions without forking holywell.
   */
  readonly additionalKeywords?: readonly string[];

  /**
   * Extra clause boundary keywords used for alias/primary-expression disambiguation.
   *
   * Add words here if your dialect introduces new top-level clause starters.
   */
  readonly clauseKeywords?: readonly string[];
}

