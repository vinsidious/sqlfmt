import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { Components } from 'react-markdown';

const components: Components = {
  h1: ({ children, ...props }) => (
    <h1
      className="text-3xl font-bold text-zinc-50 mb-6 mt-8 first:mt-0"
      {...props}
    >
      {children}
    </h1>
  ),
  h2: ({ children, ...props }) => (
    <h2
      className="text-2xl font-semibold text-zinc-50 mb-4 mt-8 border-b border-zinc-800 pb-2"
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
    <p className="text-base text-zinc-300 mb-4 leading-relaxed" {...props}>
      {children}
    </p>
  ),
  a: ({ children, ...props }) => (
    <a
      className="text-indigo-400 hover:text-indigo-300 underline underline-offset-2"
      {...props}
    >
      {children}
    </a>
  ),
  code: ({ children, className, ...props }) => {
    // If this code element is inside a pre (code block), use block styling
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
    // Inline code styling
    return (
      <code
        className="bg-zinc-800 text-indigo-300 px-1.5 py-0.5 rounded text-sm"
        style={{ fontFamily: 'var(--font-mono)' }}
        {...props}
      >
        {children}
      </code>
    );
  },
  pre: ({ children, ...props }) => (
    <pre
      className="bg-zinc-900 border border-zinc-800 rounded-lg p-4 mb-4 overflow-x-auto"
      {...props}
    >
      {children}
    </pre>
  ),
  ul: ({ children, ...props }) => (
    <ul
      className="list-disc list-inside mb-4 text-zinc-300 space-y-1"
      {...props}
    >
      {children}
    </ul>
  ),
  ol: ({ children, ...props }) => (
    <ol
      className="list-decimal list-inside mb-4 text-zinc-300 space-y-1"
      {...props}
    >
      {children}
    </ol>
  ),
  li: ({ children, ...props }) => (
    <li className="leading-relaxed" {...props}>
      {children}
    </li>
  ),
  table: ({ children, ...props }) => (
    <div className="overflow-x-auto mb-4">
      <table className="w-full text-sm" {...props}>
        {children}
      </table>
    </div>
  ),
  thead: ({ children, ...props }) => (
    <thead className="border-b border-zinc-700" {...props}>
      {children}
    </thead>
  ),
  th: ({ children, ...props }) => (
    <th className="text-left p-2 text-zinc-300 font-medium" {...props}>
      {children}
    </th>
  ),
  td: ({ children, ...props }) => (
    <td className="p-2 text-zinc-400 border-b border-zinc-800" {...props}>
      {children}
    </td>
  ),
  blockquote: ({ children, ...props }) => (
    <blockquote
      className="border-l-2 border-indigo-500 pl-4 italic text-zinc-400 mb-4"
      {...props}
    >
      {children}
    </blockquote>
  ),
  hr: (props) => <hr className="border-zinc-800 my-8" {...props} />,
};

export function MarkdownContent({ content }: { content: string }) {
  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
      {content}
    </ReactMarkdown>
  );
}
