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
    <footer className="border-t border-white/[0.04]">
      <div className="mx-auto flex max-w-7xl flex-col items-center gap-4 px-4 py-5 sm:flex-row sm:justify-between sm:px-6">
        <button
          onClick={handleCopy}
          className="group flex items-center gap-2 text-zinc-600 transition-colors duration-200 hover:text-zinc-300"
          title="Copy install command"
        >
          <span className="text-brand/50 font-mono text-xs">$</span>
          <code className="font-mono text-xs">{INSTALL_CMD}</code>
          <span
            className={`text-[10px] uppercase tracking-wider transition-colors duration-200 ${
              copied
                ? 'text-brand'
                : 'text-zinc-700 group-hover:text-zinc-500'
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
            className="transition-colors duration-200 hover:text-zinc-300"
          >
            GitHub
          </a>
          <a
            href="https://www.npmjs.com/package/holywell"
            target="_blank"
            rel="noopener noreferrer"
            className="transition-colors duration-200 hover:text-zinc-300"
          >
            npm
          </a>
          <span className="text-zinc-700">MIT</span>
        </div>
      </div>
    </footer>
  );
}
