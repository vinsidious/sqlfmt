import fs from 'fs';
import path from 'path';
import type { Metadata } from 'next';
import { MarkdownContent } from '../../components/markdown-content';

export const metadata: Metadata = {
  title: 'Integrations — holywell',
};

export default function IntegrationsPage() {
  const filePath = path.resolve(process.cwd(), '..', 'docs', 'integrations.md');
  const content = fs.readFileSync(filePath, 'utf-8');

  return <MarkdownContent content={content} />;
}
