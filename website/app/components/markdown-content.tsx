import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { Components } from 'react-markdown';
import { CopyMarkdown } from './copy-markdown';

const components: Components = {
  h1: ({ children, ...props }) => (
    <h1
      className="text-3xl font-bold text-white mb-6 mt-8 first:mt-0"
      {...props}
    >
      {children}
    </h1>
  ),
  h2: ({ children, ...props }) => (
    <h2
      className="text-2xl font-semibold text-white mb-4 mt-10 pb-3 border-b border-white/[0.06]"
      {...props}
    >
      {children}
    </h2>
  ),
  h3: ({ children, ...props }) => (
    <h3 className="text-xl font-medium text-zinc-100 mb-3 mt-6" {...props}>
      {children}
    </h3>
  ),
  p: ({ children, ...props }) => (
    <p
      className="text-[15px] text-zinc-400 mb-4 leading-relaxed"
      {...props}
    >
      {children}
    </p>
  ),
  a: ({ children, href, ...props }) => {
    // Transform relative links to ../README.md into /docs route
    const resolvedHref = href?.replace(/^\.\.\/README\.md(#.*)?$/, '/docs$1') ?? href;
    const isExternal = resolvedHref?.startsWith('http://') || resolvedHref?.startsWith('https://');
    return (
      <a
        href={resolvedHref}
        className="text-brand hover:text-brand-light underline underline-offset-2 transition-colors duration-200"
        {...(isExternal && { target: '_blank', rel: 'noopener noreferrer' })}
        {...props}
      >
        {children}
      </a>
    );
  },
  code: ({ children, className, ...props }) => {
    const isBlock = className?.startsWith('language-');
    if (isBlock) {
      return (
        <code
          className={`text-sm text-zinc-300 ${className ?? ''}`}
          style={{ fontFamily: 'var(--font-mono)' }}
          {...props}
        >
          {children}
        </code>
      );
    }
    return (
      <code
        className="bg-brand/10 text-brand px-1.5 py-0.5 rounded-md text-sm"
        style={{ fontFamily: 'var(--font-mono)' }}
        {...props}
      >
        {children}
      </code>
    );
  },
  pre: ({ children, ...props }) => (
    <pre
      className="bg-[#0A0A0A] border border-white/[0.06] rounded-xl p-4 mb-4 overflow-x-auto"
      {...props}
    >
      {children}
    </pre>
  ),
  ul: ({ children, ...props }) => (
    <ul
      className="list-disc list-inside mb-4 text-zinc-400 space-y-1.5"
      {...props}
    >
      {children}
    </ul>
  ),
  ol: ({ children, ...props }) => (
    <ol
      className="list-decimal list-inside mb-4 text-zinc-400 space-y-1.5"
      {...props}
    >
      {children}
    </ol>
  ),
  li: ({ children, ...props }) => (
    <li className="leading-relaxed text-[15px]" {...props}>
      {children}
    </li>
  ),
  table: ({ children, ...props }) => (
    <div className="overflow-x-auto mb-4 rounded-xl border border-white/[0.06]">
      <table className="w-full text-sm" {...props}>
        {children}
      </table>
    </div>
  ),
  thead: ({ children, ...props }) => (
    <thead className="border-b border-white/[0.06] bg-white/[0.02]" {...props}>
      {children}
    </thead>
  ),
  th: ({ children, ...props }) => (
    <th
      className="text-left p-3 text-zinc-300 font-medium text-sm"
      {...props}
    >
      {children}
    </th>
  ),
  td: ({ children, ...props }) => (
    <td
      className="p-3 text-zinc-500 border-b border-white/[0.04] text-sm"
      {...props}
    >
      {children}
    </td>
  ),
  blockquote: ({ children, ...props }) => (
    <blockquote
      className="border-l-2 border-brand/40 pl-4 italic text-zinc-500 mb-4"
      {...props}
    >
      {children}
    </blockquote>
  ),
  hr: (props) => <hr className="border-white/[0.06] my-8" {...props} />,
};

export function MarkdownContent({ content }: { content: string }) {
  return (
    <div className="relative">
      <div className="absolute top-0 right-0">
        <CopyMarkdown content={content} />
      </div>
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
        {content}
      </ReactMarkdown>
    </div>
  );
}
