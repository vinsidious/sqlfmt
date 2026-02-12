import type { NextConfig } from 'next';
import path from 'path';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const rootPkg = JSON.parse(
  readFileSync(path.resolve(__dirname, '..', 'package.json'), 'utf-8'),
);

const nextConfig: NextConfig = {
  outputFileTracingRoot: path.resolve(__dirname, '..'),
  env: {
    NEXT_PUBLIC_HOLYWELL_VERSION: rootPkg.version,
  },
};

export default nextConfig;
