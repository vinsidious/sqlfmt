import fs from 'fs';
import path from 'path';
import type { Metadata } from 'next';
import { MarkdownContent } from '../../components/markdown-content';

export const metadata: Metadata = {
  title: 'Style Guide — sqlfmt',
};

export default function StyleGuidePage() {
  const filePath = path.resolve(process.cwd(), '..', 'docs', 'style-guide.md');
  const content = fs.readFileSync(filePath, 'utf-8');

  return <MarkdownContent content={content} />;
}
