import type { Metadata } from 'next';
import { Playground } from './components/playground';

export const metadata: Metadata = {
  title: 'sqlfmt — Format SQL in Your Browser',
  description:
    'Zero-config SQL formatter with river alignment. Format SQL instantly in your browser with full PostgreSQL support.',
};

const FEATURES = [
  {
    title: 'Zero Config',
    description:
      'No .sqlfmtrc, no init, no style toggles. One style, everywhere.',
  },
  {
    title: 'PostgreSQL-First',
    description:
      'Full support for type casts, JSON operators, dollar-quoting, arrays, and more.',
  },
  {
    title: 'Idempotent',
    description:
      'Format once or a hundred times — the output never changes.',
  },
  {
    title: 'Blazing Fast',
    description:
      '5,000+ statements per second. Zero runtime dependencies.',
  },
] as const;

export default function HomePage() {
  return (
    <div className="py-12 sm:py-16">
      {/* Hero */}
      <section className="mx-auto max-w-6xl px-4 sm:px-6 text-center mb-10">
        <h1 className="text-4xl sm:text-5xl font-bold text-zinc-50 font-mono tracking-tight">
          sqlfmt
        </h1>
        <p className="mt-3 text-lg text-zinc-400">
          Zero-config SQL formatter with river alignment
        </p>
        <div className="mt-4 inline-block rounded-md border border-zinc-800 bg-zinc-900 px-3 py-1.5">
          <code className="font-mono text-sm text-zinc-300">
            npm install @vcoppola/sqlfmt
          </code>
        </div>
      </section>

      {/* Playground */}
      <section className="mx-auto max-w-7xl px-4 sm:px-6 mb-16">
        <Playground />
      </section>

      {/* Features */}
      <section className="mx-auto max-w-6xl px-4 sm:px-6">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {FEATURES.map((feature) => (
            <div
              key={feature.title}
              className="rounded-lg border border-zinc-800 bg-zinc-900 p-6"
            >
              <h3 className="text-lg font-semibold text-zinc-50">
                {feature.title}
              </h3>
              <p className="mt-1 text-sm text-zinc-400">
                {feature.description}
              </p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
