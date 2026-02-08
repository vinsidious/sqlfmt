'use client';

import { useState } from 'react';

const INSTALL_CMD = 'npm i holywell';

export function Footer() {
  const [copied, setCopied] = useState(false);

  function handleCopy() {
    navigator.clipboard.writeText(INSTALL_CMD).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }

  return (
    <footer className="border-t border-zinc-800/50">
      <div className="mx-auto flex max-w-7xl flex-col items-center gap-4 px-4 py-5 sm:flex-row sm:justify-between sm:px-6">
        <button
          onClick={handleCopy}
          className="group flex items-center gap-2 text-zinc-500 transition-colors hover:text-zinc-300"
          title="Copy install command"
        >
          <code className="font-mono text-xs">{INSTALL_CMD}</code>
          <span
            className={`text-[10px] uppercase tracking-wider transition-colors ${
              copied ? 'text-emerald-500' : 'text-zinc-600 group-hover:text-zinc-500'
            }`}
          >
            {copied ? 'copied' : 'copy'}
          </span>
        </button>

        <div className="flex items-center gap-5 text-[11px] text-zinc-600">
          <a
            href="https://github.com/vinsidious/holywell"
            target="_blank"
            rel="noopener noreferrer"
            className="transition-colors hover:text-zinc-400"
          >
            GitHub
          </a>
          <a
            href="https://www.npmjs.com/package/holywell"
            target="_blank"
            rel="noopener noreferrer"
            className="transition-colors hover:text-zinc-400"
          >
            npm
          </a>
          <span>MIT</span>
        </div>
      </div>
    </footer>
  );
}
