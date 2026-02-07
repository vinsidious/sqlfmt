import fs from 'fs';
import path from 'path';
import type { Metadata } from 'next';
import { MarkdownContent } from '../../components/markdown-content';

export const metadata: Metadata = {
  title: 'Migration Guide — sqlfmt',
};

export default function MigrationPage() {
  const filePath = path.resolve(
    process.cwd(),
    '..',
    'docs',
    'migration-guide.md',
  );
  const content = fs.readFileSync(filePath, 'utf-8');

  return <MarkdownContent content={content} />;
}
