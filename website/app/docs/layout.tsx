'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useState } from 'react';

const SIDEBAR_LINKS = [
  { href: '/docs', label: 'Overview' },
  { href: '/docs/style-guide', label: 'Style Guide' },
  { href: '/docs/integrations', label: 'Integrations' },
  { href: '/docs/architecture', label: 'Architecture' },
  { href: '/docs/migration', label: 'Migration Guide' },
] as const;

export default function DocsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const [sidebarOpen, setSidebarOpen] = useState(false);

  return (
    <div className="mx-auto max-w-6xl flex">
      {/* Mobile sidebar toggle */}
      <button
        type="button"
        onClick={() => setSidebarOpen(!sidebarOpen)}
        className="fixed bottom-4 right-4 z-40 md:hidden bg-zinc-800 border border-zinc-700 rounded-lg p-3 text-zinc-300"
        aria-label="Toggle documentation sidebar"
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <line x1="3" y1="6" x2="21" y2="6" />
          <line x1="3" y1="12" x2="21" y2="12" />
          <line x1="3" y1="18" x2="21" y2="18" />
        </svg>
      </button>

      {/* Sidebar overlay for mobile */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-30 bg-black/50 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={`
          fixed top-14 left-0 z-30 h-[calc(100vh-3.5rem)] w-64 shrink-0
          bg-zinc-950 border-r border-zinc-800 py-6 px-4
          transition-transform duration-200 ease-in-out
          md:sticky md:translate-x-0
          ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
        `}
      >
        <h2 className="text-xs uppercase tracking-wider text-zinc-500 mb-4">
          Documentation
        </h2>
        <nav className="flex flex-col gap-1">
          {SIDEBAR_LINKS.map(({ href, label }) => {
            const isActive = pathname === href;
            return (
              <Link
                key={href}
                href={href}
                onClick={() => setSidebarOpen(false)}
                className={`px-3 py-1.5 rounded text-sm transition-colors ${
                  isActive
                    ? 'text-indigo-400 bg-zinc-900'
                    : 'text-zinc-400 hover:text-zinc-200'
                }`}
              >
                {label}
              </Link>
            );
          })}
        </nav>
      </aside>

      {/* Content */}
      <div className="flex-1 min-w-0 max-w-3xl py-8 px-4 md:px-6">
        {children}
      </div>
    </div>
  );
}
