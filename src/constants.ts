// Shared limits and layout baselines.
// Keep these centralized so parser/formatter defaults stay in sync.
export const DEFAULT_MAX_DEPTH = 200;

// 80 columns remains the default terminal-oriented formatting target.
export const TERMINAL_WIDTH = 80;

// 10MB default safety ceiling for input payloads.
export const DEFAULT_MAX_INPUT_SIZE = 10_485_760;

// Tokenizer hard limits for DoS protection.
export const MAX_TOKEN_COUNT = 1_000_000;
export const MAX_IDENTIFIER_LENGTH = 10_000;
