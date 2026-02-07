'use client';

import { useState } from 'react';

const INSTALL_CMD = 'npm install @vcoppola/sqlfmt';

export function Footer() {
  const [copied, setCopied] = useState(false);

  function handleCopy() {
    navigator.clipboard.writeText(INSTALL_CMD).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }

  return (
    <footer className="border-t border-zinc-800 bg-zinc-950">
      <div className="mx-auto flex max-w-6xl flex-col items-center gap-3 px-4 py-6 sm:flex-row sm:justify-between sm:px-6">
        <button
          onClick={handleCopy}
          className="group flex items-center gap-2 text-sm text-zinc-400 transition-colors hover:text-zinc-50"
          title="Copy install command"
        >
          <code className="rounded border border-zinc-800 bg-zinc-900 px-2 py-1 font-mono text-xs">
            {INSTALL_CMD}
          </code>
          <span
            className={`text-xs transition-colors ${
              copied ? 'text-green-500' : 'text-zinc-500 group-hover:text-zinc-400'
            }`}
          >
            {copied ? 'copied' : 'copy'}
          </span>
        </button>

        <div className="flex items-center gap-4 text-xs text-zinc-400">
          <a
            href="https://github.com/vinsidious/sqlfmt"
            target="_blank"
            rel="noopener noreferrer"
            className="transition-colors hover:text-zinc-50"
          >
            GitHub
          </a>
          <span className="text-zinc-700">|</span>
          <a
            href="https://www.npmjs.com/package/@vcoppola/sqlfmt"
            target="_blank"
            rel="noopener noreferrer"
            className="transition-colors hover:text-zinc-50"
          >
            npm
          </a>
          <span className="text-zinc-700">|</span>
          <span>MIT</span>
        </div>
      </div>
    </footer>
  );
}
