import fs from 'fs';
import path from 'path';
import type { Metadata } from 'next';
import type { DialectName } from '@/lib/holywell';
import { ExamplesViewer } from '../components/examples-viewer';

export const metadata: Metadata = {
  title: 'Examples — holywell',
  description:
    'See what holywell-formatted SQL looks like across PostgreSQL, ANSI, MySQL, and SQL Server dialects.',
};

const DIALECT_FILES: readonly { dialect: DialectName; filename: string }[] = [
  { dialect: 'ansi', filename: 'ansi.sql' },
  { dialect: 'postgres', filename: 'postgres.sql' },
  { dialect: 'mysql', filename: 'mysql.sql' },
  { dialect: 'tsql', filename: 'tsql.sql' },
];

export default function ExamplesPage() {
  const examplesDir = path.resolve(process.cwd(), 'examples');
  const examples = Object.fromEntries(
    DIALECT_FILES.map(({ dialect, filename }) => [
      dialect,
      fs.readFileSync(path.join(examplesDir, filename), 'utf-8'),
    ]),
  ) as Record<DialectName, string>;

  return (
    <div className="relative">
      <section className="pt-10 pb-6 sm:pt-14 sm:pb-8">
        <div className="mx-auto max-w-7xl px-4 sm:px-6">
          <h1 className="text-2xl sm:text-3xl font-bold text-white font-mono tracking-tighter">
            Examples
          </h1>
          <p className="mt-2 text-sm text-zinc-500 max-w-2xl">
            Comprehensive formatted output for each supported dialect. Select a
            dialect to see how holywell transforms dense, unformatted SQL into
            clean, readable queries.
          </p>
        </div>
      </section>

      <section className="mx-auto max-w-7xl px-4 sm:px-6 pb-28 sm:pb-36">
        <ExamplesViewer examples={examples} />
      </section>
    </div>
  );
}
