import { EditorView } from '@codemirror/view';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { tags as t } from '@lezer/highlight';

/**
 * Custom CodeMirror theme — Emerald dark.
 * Signature green keywords, warm amber strings, cool cyan functions.
 */

const baseTheme = EditorView.theme(
  {
    '&': {
      backgroundColor: 'transparent',
      color: '#e4e4e7',
    },
    '.cm-content': {
      caretColor: '#3ECF8E',
      fontFamily:
        'var(--font-mono), "JetBrains Mono", "Fira Code", monospace',
      fontSize: '13px',
      lineHeight: '1.7',
      padding: '12px 0',
    },
    '.cm-cursor, .cm-dropCursor': {
      borderLeftColor: '#3ECF8E',
      borderLeftWidth: '2px',
    },
    '&.cm-focused .cm-selectionBackground, .cm-selectionBackground, .cm-content ::selection':
      {
        backgroundColor: 'rgba(62, 207, 142, 0.12) !important',
      },
    '.cm-activeLine': {
      backgroundColor: 'rgba(255, 255, 255, 0.025)',
    },
    '.cm-gutters': {
      backgroundColor: 'transparent',
      color: '#2a2a2a',
      borderRight: 'none',
      fontFamily:
        'var(--font-mono), "JetBrains Mono", "Fira Code", monospace',
      fontSize: '13px',
      paddingLeft: '8px',
    },
    '.cm-activeLineGutter': {
      backgroundColor: 'transparent',
      color: '#525252',
    },
    '.cm-lineNumbers .cm-gutterElement': {
      padding: '0 8px 0 0',
      minWidth: '32px',
    },
    '&.cm-focused': {
      outline: 'none',
    },
    '.cm-matchingBracket': {
      backgroundColor: 'rgba(62, 207, 142, 0.2)',
      color: '#3ECF8E !important',
      borderRadius: '2px',
    },
    '.cm-selectionMatch': {
      backgroundColor: 'rgba(62, 207, 142, 0.08)',
    },
    '.cm-searchMatch': {
      backgroundColor: 'rgba(62, 207, 142, 0.15)',
      borderRadius: '2px',
    },
    '.cm-searchMatch.cm-searchMatch-selected': {
      backgroundColor: 'rgba(62, 207, 142, 0.3)',
    },
    '.cm-tooltip': {
      backgroundColor: '#0f0f0f',
      border: '1px solid rgba(255,255,255,0.08)',
      borderRadius: '8px',
      boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
    },
    '.cm-tooltip-autocomplete > ul': {
      fontFamily:
        'var(--font-mono), "JetBrains Mono", "Fira Code", monospace',
    },
    '.cm-foldPlaceholder': {
      backgroundColor: 'transparent',
      border: 'none',
      color: '#525252',
    },
  },
  { dark: true },
);

const highlightStyle = HighlightStyle.define([
  // Keywords — signature emerald green
  { tag: t.keyword, color: '#3ECF8E' },
  { tag: t.definitionKeyword, color: '#3ECF8E' },
  { tag: t.operatorKeyword, color: '#3ECF8E' },
  { tag: t.moduleKeyword, color: '#3ECF8E' },
  { tag: t.controlKeyword, color: '#3ECF8E' },

  // Strings — warm amber gold
  { tag: t.string, color: '#E8B960' },
  { tag: t.special(t.string), color: '#E8B960' },
  { tag: t.character, color: '#E8B960' },

  // Numbers — soft violet
  { tag: t.number, color: '#C4B5FD' },
  { tag: t.integer, color: '#C4B5FD' },
  { tag: t.float, color: '#C4B5FD' },

  // Comments — muted, italic
  { tag: t.comment, color: '#404040', fontStyle: 'italic' },
  { tag: t.lineComment, color: '#404040', fontStyle: 'italic' },
  { tag: t.blockComment, color: '#404040', fontStyle: 'italic' },

  // Operators
  { tag: t.operator, color: '#a1a1aa' },
  { tag: t.compareOperator, color: '#67E8F9' },
  { tag: t.logicOperator, color: '#3ECF8E' },

  // Built-in functions — electric cyan
  { tag: t.function(t.variableName), color: '#67E8F9' },
  { tag: t.function(t.propertyName), color: '#67E8F9' },
  { tag: t.standard(t.variableName), color: '#67E8F9' },

  // Type names — soft fuchsia
  { tag: t.typeName, color: '#F0ABFC' },
  { tag: t.className, color: '#F0ABFC' },
  { tag: t.namespace, color: '#F0ABFC' },

  // Booleans and null — warm orange
  { tag: t.bool, color: '#FB923C' },
  { tag: t.null, color: '#FB923C' },

  // Variables/identifiers — light gray
  { tag: t.variableName, color: '#e4e4e7' },
  { tag: t.propertyName, color: '#d4d4d8' },

  // Punctuation
  { tag: t.punctuation, color: '#525252' },
  { tag: t.separator, color: '#525252' },
  { tag: t.bracket, color: '#a1a1aa' },

  // Special
  { tag: t.meta, color: '#6b7280' },
  { tag: t.labelName, color: '#67E8F9' },
  { tag: t.annotation, color: '#FB923C' },

  // Emphasis
  { tag: t.emphasis, fontStyle: 'italic' },
  { tag: t.strong, fontWeight: 'bold' },
]);

export const holywellTheme = [baseTheme, syntaxHighlighting(highlightStyle)];
