import type { Metadata } from 'next';
import Link from 'next/link';
import { Playground } from './components/playground';
import { CopyInstall } from './components/copy-install';

export const metadata: Metadata = {
  title: 'holywell — Beautiful, Readable SQL',
  description:
    'SQL formatter that makes your queries beautiful and readable. Format SQL instantly in your browser with full PostgreSQL support.',
};

export default function HomePage() {
  return (
    <div className="relative">
      {/* Hero */}
      <section className="relative overflow-hidden pt-12 pb-8 sm:pt-16 sm:pb-10">
        <div className="relative mx-auto max-w-5xl px-4 sm:px-6 text-center">
          {/* Heading */}
          <div className="animate-fade-in-up">
            <h1 className="text-4xl sm:text-5xl font-bold text-white font-mono tracking-tighter">
              holywell
            </h1>
          </div>

          {/* Subtitle */}
          <p className="animate-fade-in-up animate-delay-1 mt-3 text-base text-zinc-500">
            Beautiful, readable SQL that conforms to{' '}
            <a
              href="https://www.sqlstyle.guide"
              target="_blank"
              rel="noopener noreferrer"
              className="text-zinc-400 underline decoration-zinc-700 underline-offset-2 hover:text-brand hover:decoration-brand/40 transition-colors duration-200"
            >
              sqlstyle.guide
            </a>
          </p>

          {/* CTAs */}
          <div className="animate-fade-in-up animate-delay-2 mt-5 flex flex-col sm:flex-row items-center justify-center gap-3">
            <CopyInstall />
            <Link
              href="/docs"
              className="text-sm text-zinc-500 hover:text-brand transition-colors duration-200"
            >
              Documentation <span className="ml-0.5">&rarr;</span>
            </Link>
          </div>
        </div>
      </section>

      {/* Playground */}
      <section className="animate-fade-in-up animate-delay-3 mx-auto max-w-7xl px-4 sm:px-6 pb-28 sm:pb-36">
        <Playground />
      </section>
    </div>
  );
}
