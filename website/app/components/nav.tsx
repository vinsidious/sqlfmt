'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const NAV_LINKS = [
  { href: '/', label: 'Playground' },
  { href: '/docs', label: 'Docs' },
] as const;

export function Nav() {
  const pathname = usePathname();

  return (
    <nav className="sticky top-0 z-50 border-b border-zinc-800 bg-zinc-950/80 backdrop-blur-md">
      <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4 sm:px-6">
        <Link href="/" className="flex items-center gap-0.5">
          <span className="font-mono text-lg font-bold text-zinc-50">
            sqlfmt
          </span>
          <span className="text-indigo-500 font-mono text-lg font-bold">
            .
          </span>
        </Link>

        <div className="flex items-center gap-6">
          {NAV_LINKS.map(({ href, label }) => {
            const isActive =
              href === '/' ? pathname === '/' : pathname.startsWith(href);

            return (
              <Link
                key={href}
                href={href}
                className={`text-sm font-medium transition-colors ${
                  isActive
                    ? 'text-zinc-50'
                    : 'text-zinc-400 hover:text-zinc-50'
                }`}
              >
                {label}
              </Link>
            );
          })}
        </div>
      </div>
    </nav>
  );
}
