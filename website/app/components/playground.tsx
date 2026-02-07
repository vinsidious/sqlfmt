'use client';

import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { formatSQL } from '@/lib/sqlfmt';
import dynamic from 'next/dynamic';
import { sql, PostgreSQL, MySQL, MariaSQL, MSSQL, SQLite, StandardSQL } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { EditorView } from '@codemirror/view';

const CodeMirror = dynamic(() => import('@uiw/react-codemirror'), {
  ssr: false,
  loading: () => (
    <div className="h-[300px] sm:h-[400px] animate-pulse bg-zinc-800/50" />
  ),
});

const DEFAULT_INPUT = `select u.id, u.name, u.email,
       count(o.id) as order_count,
       sum(o.total) as total_spent
from users u
left join orders o on u.id = o.user_id
where u.created_at > '2024-01-01'
  and u.status = 'active'
group by u.id, u.name, u.email
having count(o.id) > 5
order by total_spent desc
limit 20;`;

const DIALECT_OPTIONS = [
  { label: 'PostgreSQL', value: 'postgresql' },
  { label: 'MySQL', value: 'mysql' },
  { label: 'MariaDB', value: 'mariadb' },
  { label: 'MSSQL', value: 'mssql' },
  { label: 'SQLite', value: 'sqlite' },
  { label: 'Standard SQL', value: 'standard' },
] as const;

type DialectValue = (typeof DIALECT_OPTIONS)[number]['value'];

const DIALECT_MAP: Record<DialectValue, typeof PostgreSQL> = {
  postgresql: PostgreSQL,
  mysql: MySQL,
  mariadb: MariaSQL,
  mssql: MSSQL,
  sqlite: SQLite,
  standard: StandardSQL,
};

// Make the CodeMirror background transparent so the parent zinc-900 shows through.
const transparentBg = EditorView.theme({
  '&': { backgroundColor: 'transparent' },
  '.cm-gutters': { backgroundColor: 'transparent', borderRight: 'none' },
});

// Consistent font styling for the editors.
const editorFont = EditorView.theme({
  '.cm-content, .cm-gutters': {
    fontFamily: 'var(--font-mono), "JetBrains Mono", "Fira Code", monospace',
    fontSize: '14px',
  },
});

export function Playground() {
  const [input, setInput] = useState(DEFAULT_INPUT);
  const [output, setOutput] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [formatTime, setFormatTime] = useState<number | null>(null);
  const [dialect, setDialect] = useState<DialectValue>('postgresql');

  const copiedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Memoize CodeMirror extensions per dialect.
  const inputExtensions = useMemo(
    () => [sql({ dialect: DIALECT_MAP[dialect] }), oneDark, transparentBg, editorFont],
    [dialect],
  );

  const outputExtensions = useMemo(
    () => [
      sql({ dialect: DIALECT_MAP[dialect] }),
      oneDark,
      transparentBg,
      editorFont,
      EditorView.editable.of(false),
    ],
    [dialect],
  );

  // Copy helper that handles missing clipboard API gracefully.
  const copyToClipboard = useCallback((text: string) => {
    if (!navigator.clipboard) return;
    navigator.clipboard.writeText(text).then(() => {
      if (copiedTimerRef.current) clearTimeout(copiedTimerRef.current);
      setCopied(true);
      copiedTimerRef.current = setTimeout(() => setCopied(false), 2000);
    }).catch(() => {
      // Clipboard write can fail in non-HTTPS or non-focused contexts.
    });
  }, []);

  // Format on mount and whenever input changes, debounced 300ms.
  useEffect(() => {
    const timer = setTimeout(() => {
      try {
        const start = performance.now();
        const result = formatSQL(input);
        const elapsed = performance.now() - start;
        setOutput(result);
        setFormatTime(elapsed);
        setError(null);
        copyToClipboard(result);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setOutput('');
        setFormatTime(null);
      }
    }, 300);

    return () => clearTimeout(timer);
  }, [input, copyToClipboard]);

  const handleCopy = useCallback(() => {
    if (output) copyToClipboard(output);
  }, [output, copyToClipboard]);

  const handleInputChange = useCallback((value: string) => {
    setInput(value);
  }, []);

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Input pane */}
        <div className="rounded-lg border border-zinc-800 bg-zinc-900 overflow-hidden">
          <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-2">
            <span className="text-sm font-medium text-zinc-300">Input</span>
            <select
              value={dialect}
              onChange={(e) => setDialect(e.target.value as DialectValue)}
              className="rounded border border-zinc-700 bg-zinc-800 px-2 py-1 text-sm text-zinc-300 outline-none focus:border-zinc-600"
            >
              {DIALECT_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
          <div className="h-[300px] sm:h-[400px] overflow-auto">
            <CodeMirror
              value={input}
              onChange={handleInputChange}
              extensions={inputExtensions}
              basicSetup={{
                lineNumbers: true,
                foldGutter: false,
                highlightActiveLine: true,
                highlightSelectionMatches: true,
              }}
              theme="dark"
            />
          </div>
        </div>

        {/* Output pane */}
        <div className="rounded-lg border border-zinc-800 bg-zinc-900 overflow-hidden">
          <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-2">
            <span className="text-sm font-medium text-zinc-300">Output</span>
            <div className="flex items-center gap-3">
              {formatTime !== null && (
                <span className="text-xs text-zinc-500">
                  {formatTime < 1
                    ? `${formatTime.toFixed(2)}ms`
                    : `${formatTime.toFixed(1)}ms`}
                </span>
              )}
              <button
                onClick={handleCopy}
                disabled={!output}
                className={`rounded px-3 py-1 text-sm transition-colors ${
                  copied
                    ? 'bg-zinc-800 text-green-400'
                    : 'bg-zinc-800 text-zinc-300 hover:bg-zinc-700 disabled:opacity-40 disabled:cursor-not-allowed'
                }`}
              >
                {copied ? (
                  <span className="flex items-center gap-1">
                    <svg
                      className="h-3.5 w-3.5"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      strokeWidth={2.5}
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        d="M5 13l4 4L19 7"
                      />
                    </svg>
                    Copied
                  </span>
                ) : (
                  'Copy'
                )}
              </button>
            </div>
          </div>
          <div className="h-[300px] sm:h-[400px] overflow-auto">
            <CodeMirror
              value={output}
              extensions={outputExtensions}
              readOnly
              basicSetup={{
                lineNumbers: true,
                foldGutter: false,
                highlightActiveLine: false,
                highlightSelectionMatches: false,
              }}
              theme="dark"
            />
          </div>
        </div>
      </div>

      {/* Error display */}
      {error && (
        <div className="rounded border border-red-800 bg-red-950/50 p-3 text-sm text-red-400">
          {error}
        </div>
      )}
    </div>
  );
}
