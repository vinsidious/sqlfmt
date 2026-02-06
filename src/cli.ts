#!/usr/bin/env node

import { readFileSync } from 'fs';
import { formatSQL } from './format';

function main() {
  const args = process.argv.slice(2);
  let checkMode = false;
  let filePath: string | null = null;

  for (const arg of args) {
    if (arg === '--check') {
      checkMode = true;
    } else if (arg === '--help' || arg === '-h') {
      console.log('Usage: sqlfmt [--check] [file]');
      console.log('  Formats SQL from file or stdin to stdout.');
      console.log('  --check  Exit non-zero if input is not already formatted.');
      process.exit(0);
    } else {
      filePath = arg;
    }
  }

  let input: string;
  if (filePath) {
    input = readFileSync(filePath, 'utf-8');
  } else {
    input = readFileSync(0, 'utf-8'); // stdin
  }

  const output = formatSQL(input);

  if (checkMode) {
    if (input.trimEnd() + '\n' === output) {
      process.exit(0);
    } else {
      console.error('Input is not formatted. Run without --check to format.');
      process.exit(1);
    }
  } else {
    process.stdout.write(output);
  }
}

main();
