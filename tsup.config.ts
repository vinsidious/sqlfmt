import { defineConfig } from 'tsup';
import { readFileSync } from 'fs';

const { version } = JSON.parse(readFileSync('./package.json', 'utf-8'));

export default defineConfig([
  {
    entry: ['src/index.ts'],
    format: ['cjs', 'esm'],
    dts: true,
    sourcemap: false,
    clean: true,
    outDir: 'dist',
    define: {
      '__HOLYWELL_VERSION__': JSON.stringify(version),
    },
  },
  {
    entry: ['src/cli.ts'],
    format: ['cjs'],
    sourcemap: false,
    outDir: 'dist',
    banner: {
      js: '#!/usr/bin/env node',
    },
    define: {
      '__HOLYWELL_VERSION__': JSON.stringify(version),
    },
  },
]);
