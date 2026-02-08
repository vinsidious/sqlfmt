import fs from 'fs';
import path from 'path';
import type { Metadata } from 'next';
import { MarkdownContent } from '../../components/markdown-content';

export const metadata: Metadata = {
  title: 'Architecture — holywell',
};

export default function ArchitecturePage() {
  const filePath = path.resolve(process.cwd(), '..', 'docs', 'architecture.md');
  const content = fs.readFileSync(filePath, 'utf-8');

  return <MarkdownContent content={content} />;
}
