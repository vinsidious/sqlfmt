import type { Metadata } from 'next';
import { Playground } from './components/playground';

export const metadata: Metadata = {
  title: 'holywell — Format SQL in Your Browser',
  description:
    'Zero-config SQL formatter with river alignment. Format SQL instantly in your browser with full PostgreSQL support.',
};

const FEATURES = [
  {
    title: 'Zero Config',
    description: 'One style, everywhere. No toggles, no config files.',
  },
  {
    title: 'PostgreSQL-First',
    description: 'Type casts, JSON ops, dollar-quoting, arrays — all supported.',
  },
  {
    title: 'Idempotent',
    description: 'Format once or a hundred times. Same result.',
  },
  {
    title: 'Blazing Fast',
    description: '5,000+ statements/sec. Zero dependencies.',
  },
] as const;

export default function HomePage() {
  return (
    <div className="py-16 sm:py-24">
      {/* Hero */}
      <section className="mx-auto max-w-6xl px-4 sm:px-6 text-center mb-16 sm:mb-20">
        <div className="inline-flex items-baseline gap-1">
          <h1 className="text-5xl sm:text-7xl font-bold text-zinc-50 font-mono tracking-tighter">
            holywell
          </h1>
          <span className="text-indigo-400 font-mono text-5xl sm:text-7xl font-bold">
            .
          </span>
        </div>
        <p className="mt-4 text-base sm:text-lg text-zinc-500 max-w-md mx-auto">
          Zero-config SQL formatter with river alignment
        </p>
        <div className="mt-6 inline-flex items-center gap-2.5 rounded-lg border border-zinc-800/60 bg-zinc-900/50 px-4 py-2">
          <code className="font-mono text-sm text-zinc-400">
            npm i holywell
          </code>
        </div>
      </section>

      {/* Playground */}
      <section className="mx-auto max-w-7xl px-4 sm:px-6 mb-24 sm:mb-32">
        <Playground />
      </section>

      {/* Features */}
      <section className="mx-auto max-w-4xl px-4 sm:px-6">
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-px bg-zinc-800/40 rounded-xl overflow-hidden border border-zinc-800/60">
          {FEATURES.map((feature) => (
            <div key={feature.title} className="bg-zinc-950 p-6">
              <h3 className="text-sm font-semibold text-zinc-200">
                {feature.title}
              </h3>
              <p className="mt-1.5 text-xs leading-relaxed text-zinc-500">
                {feature.description}
              </p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
