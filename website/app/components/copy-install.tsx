'use client';

import { useState, useRef } from 'react';

const INSTALL_CMD = 'npm i holywell';

export function CopyInstall() {
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  function handleCopy() {
    navigator.clipboard.writeText(INSTALL_CMD).then(() => {
      if (timerRef.current) clearTimeout(timerRef.current);
      setCopied(true);
      timerRef.current = setTimeout(() => setCopied(false), 2000);
    });
  }

  return (
    <button
      onClick={handleCopy}
      className="group flex items-center gap-3 rounded-lg border border-white/[0.06] bg-white/[0.02] px-4 py-2.5 transition-all duration-200 hover:border-brand/20 hover:bg-brand/[0.02]"
      title="Copy install command"
    >
      <span className="text-brand font-mono text-sm select-none">$</span>
      <code className="font-mono text-sm text-zinc-300">{INSTALL_CMD}</code>
      <span
        className={`text-[10px] font-medium uppercase tracking-wider transition-colors duration-200 ${
          copied
            ? 'text-brand'
            : 'text-zinc-600 group-hover:text-zinc-400'
        }`}
      >
        {copied ? 'copied!' : 'copy'}
      </span>
    </button>
  );
}
