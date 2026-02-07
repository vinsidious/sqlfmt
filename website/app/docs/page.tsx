import fs from 'fs';
import path from 'path';
import type { Metadata } from 'next';
import { MarkdownContent } from '../components/markdown-content';

export const metadata: Metadata = {
  title: 'Documentation — sqlfmt',
};

function getReadmeContent(): string {
  const filePath = path.resolve(process.cwd(), '..', 'README.md');
  let content = fs.readFileSync(filePath, 'utf-8');

  // Strip badge lines (lines containing img.shields.io)
  content = content
    .split('\n')
    .filter((line) => !line.includes('img.shields.io'))
    .join('\n');

  // Strip the "Documentation" section
  content = content.replace(
    /## Documentation\n[\s\S]*?(?=\n## |\n*$)/,
    '',
  );

  // Strip the "Development" section and everything after it
  const devIndex = content.indexOf('\n## Development');
  if (devIndex !== -1) {
    content = content.substring(0, devIndex);
  }

  // Clean up any trailing whitespace
  content = content.trimEnd() + '\n';

  return content;
}

export default function DocsPage() {
  const content = getReadmeContent();

  return <MarkdownContent content={content} />;
}
