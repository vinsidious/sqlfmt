'use client';

import {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
  type ChangeEvent,
} from 'react';
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
    <div className="h-[350px] sm:h-[500px] animate-pulse bg-white/[0.02] rounded-b-xl" />
  ),
});

export function Playground() {
  const [input, setInput] = useState(
    `select e.name, e.salary, d.department_name, rank() over (partition by d.department_name order by e.salary desc) as dept_rank from employees as e inner join departments as d on e.department_id = d.id where e.start_date >= '2024-01-01' and d.active = true order by d.department_name, dept_rank;`
  );
  const [output, setOutput] = useState('');
  const [dialect, setDialect] = useState<DialectName>(DEFAULT_DIALECT);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [formatTime, setFormatTime] = useState<number | null>(null);

  const copiedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const codeMirrorDialect = CODEMIRROR_DIALECTS[dialect];

  const extensions = useMemo(
    () => [sql({ dialect: codeMirrorDialect }), ...holywellTheme],
    [codeMirrorDialect],
  );

  const outputExtensions = useMemo(
    () => [
      sql({ dialect: codeMirrorDialect }),
      ...holywellTheme,
      EditorView.editable.of(false),
    ],
    [codeMirrorDialect],
  );

  const handleCopy = useCallback(() => {
    if (!output || !navigator.clipboard) return;
    navigator.clipboard
      .writeText(output)
      .then(() => {
        if (copiedTimerRef.current) clearTimeout(copiedTimerRef.current);
        setCopied(true);
        copiedTimerRef.current = setTimeout(() => setCopied(false), 2000);
      })
      .catch(() => {});
  }, [output]);

  useEffect(() => {
    if (!input.trim()) {
      setOutput('');
      setError(null);
      setFormatTime(null);
      return;
    }

    const timer = setTimeout(() => {
      try {
        const start = performance.now();
        const result = formatSQL(input, { dialect });
        const elapsed = performance.now() - start;
        setOutput(result);
        setFormatTime(elapsed);
        setError(null);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setOutput('');
        setFormatTime(null);
      }
    }, 300);

    return () => clearTimeout(timer);
  }, [input, dialect]);

  const handleInputChange = useCallback((value: string) => {
    setInput(value);
  }, []);

  const handleDialectChange = useCallback(
    (event: ChangeEvent<HTMLSelectElement>) => {
      setDialect(event.target.value as DialectName);
    },
    [],
  );

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        {/* Input pane */}
        <div className="group relative rounded-xl border border-white/[0.06] overflow-hidden transition-all duration-300 focus-within:border-brand/20">
          {/* Top accent line */}
          <div className="h-px bg-gradient-to-r from-transparent via-brand/30 to-transparent" />

          <div className="flex items-center justify-between border-b border-white/[0.04] px-4 py-2.5">
            <span className="text-[11px] font-medium uppercase tracking-widest text-zinc-600">
              Input
            </span>
            <div className="flex items-center gap-1.5">
              <label
                htmlFor="dialect-select"
                className="text-[10px] font-medium uppercase tracking-widest text-zinc-600"
              >
                Dialect
              </label>
              <select
                id="dialect-select"
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

          <div className="h-[350px] sm:h-[500px] overflow-auto">
            <CodeMirror
              value={input}
              onChange={handleInputChange}
              extensions={extensions}
              basicSetup={{
                lineNumbers: true,
                foldGutter: false,
                highlightActiveLine: true,
                highlightSelectionMatches: true,
              }}
              theme="none"
              placeholder="Paste your SQL here..."
            />
          </div>
        </div>

        {/* Output pane */}
        <div className="relative rounded-xl border border-white/[0.06] overflow-hidden transition-all duration-300">
          {/* Top accent line */}
          <div className="h-px bg-gradient-to-r from-transparent via-brand/30 to-transparent" />

          <div className="flex items-center justify-between border-b border-white/[0.04] px-4 py-2.5">
            <div className="flex items-center gap-3">
              <span className="text-[11px] font-medium uppercase tracking-widest text-zinc-600">
                Output
              </span>
              {formatTime !== null && (
                <span className="rounded-full bg-brand/10 px-2 py-0.5 text-[10px] font-mono text-brand/70">
                  {formatTime < 1
                    ? `${formatTime.toFixed(2)}ms`
                    : `${formatTime.toFixed(1)}ms`}
                </span>
              )}
            </div>
            <button
              onClick={handleCopy}
              disabled={!output}
              className={`flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-medium transition-all duration-200 ${
                copied
                  ? 'text-brand'
                  : 'text-zinc-500 hover:bg-white/[0.04] hover:text-zinc-300 disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent disabled:hover:text-zinc-500'
              }`}
            >
              {copied ? (
                <>
                  <svg
                    className="h-3 w-3"
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
                </>
              ) : (
                <>
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
                      d="M15.666 3.888A2.25 2.25 0 0013.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 01-.75.75H9.75a.75.75 0 01-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 01-2.25 2.25H6.75A2.25 2.25 0 014.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 011.927-.184"
                    />
                  </svg>
                  Copy
                </>
              )}
            </button>
          </div>

          <div className="h-[350px] sm:h-[500px] overflow-auto">
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
              theme="none"
            />
          </div>
        </div>
      </div>

      {/* Error display */}
      {error && (
        <div className="rounded-xl border border-red-500/20 bg-red-500/[0.03] px-4 py-3 text-sm font-mono text-red-400">
          {error}
        </div>
      )}
    </div>
  );
}
