'use client';

import { useState, useMemo, useCallback, type ChangeEvent } from 'react';
import { formatSQL, type DialectName } from '@/lib/holywell';
import dynamic from 'next/dynamic';
import { sql } from '@codemirror/lang-sql';
import { EditorView } from '@codemirror/view';
import { holywellTheme } from './editor-theme';
import {
  DEFAULT_DIALECT,
  DIALECT_OPTIONS,
  CODEMIRROR_DIALECTS,
} from './dialect-config';

const CodeMirror = dynamic(() => import('@uiw/react-codemirror'), {
  ssr: false,
  loading: () => (
    <div className="h-96 animate-pulse bg-white/[0.02] rounded-b-xl" />
  ),
});

interface ExamplesViewerProps {
  examples: Record<DialectName, string>;
}

export function ExamplesViewer({ examples }: ExamplesViewerProps) {
  const [dialect, setDialect] = useState<DialectName>(DEFAULT_DIALECT);
  const [showRaw, setShowRaw] = useState(false);

  const codeMirrorDialect = CODEMIRROR_DIALECTS[dialect];

  const extensions = useMemo(
    () => [
      sql({ dialect: codeMirrorDialect }),
      ...holywellTheme,
      EditorView.editable.of(false),
      EditorView.lineWrapping,
    ],
    [codeMirrorDialect],
  );

  const rawInput = examples[dialect];

  const formatted = useMemo(() => {
    try {
      return formatSQL(rawInput, { dialect, recover: true });
    } catch {
      return `-- Formatting error\n${rawInput}`;
    }
  }, [rawInput, dialect]);

  const displayValue = showRaw ? rawInput : formatted;

  const handleDialectChange = useCallback(
    (event: ChangeEvent<HTMLSelectElement>) => {
      setDialect(event.target.value as DialectName);
    },
    [],
  );

  const handleToggleRaw = useCallback(() => {
    setShowRaw(prev => !prev);
  }, []);

  return (
    <div className="group relative rounded-xl border border-white/[0.06] overflow-hidden">
      {/* Top accent line */}
      <div className="h-px bg-gradient-to-r from-transparent via-brand/30 to-transparent" />

      <div className="flex items-center justify-between border-b border-white/[0.04] px-4 py-2.5">
        <span className="text-[11px] font-medium uppercase tracking-widest text-zinc-600">
          {showRaw ? 'Raw Input' : 'Formatted Output'}
        </span>

        <div className="flex items-center gap-3">
          {/* Raw/Formatted toggle */}
          <button
            onClick={handleToggleRaw}
            className={`flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-medium transition-all duration-200 ${
              showRaw
                ? 'text-brand bg-brand/10'
                : 'text-zinc-500 hover:bg-white/[0.04] hover:text-zinc-300'
            }`}
          >
            <svg
              className="h-3 w-3"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M17.25 6.75L22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3l-4.5 16.5"
              />
            </svg>
            {showRaw ? 'Show formatted' : 'Show raw'}
          </button>

          {/* Dialect selector */}
          <div className="flex items-center gap-1.5">
            <label
              htmlFor="examples-dialect-select"
              className="text-[10px] font-medium uppercase tracking-widest text-zinc-600"
            >
              Dialect
            </label>
            <select
              id="examples-dialect-select"
              value={dialect}
              onChange={handleDialectChange}
              className="rounded-md border border-white/[0.08] bg-white/[0.02] px-2 py-1 text-xs font-medium text-zinc-300 outline-none transition-all duration-200 hover:border-white/[0.12] focus:border-brand/30"
            >
              {DIALECT_OPTIONS.map(option => (
                <option
                  key={option.value}
                  value={option.value}
                  className="bg-[#0A0A0A] text-zinc-200"
                >
                  {option.label}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      <CodeMirror
        value={displayValue}
        extensions={extensions}
        readOnly
        basicSetup={{
          lineNumbers: true,
          foldGutter: false,
          highlightActiveLine: false,
          highlightSelectionMatches: false,
        }}
        theme="none"
      />
    </div>
  );
}
