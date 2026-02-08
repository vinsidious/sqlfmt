import type * as AST from './ast';

export interface VisitContext {
  parent: unknown | null;
  key?: string | number;
  depth: number;
}

export interface AstVisitor {
  enter?: (node: unknown, context: VisitContext) => void;
  leave?: (node: unknown, context: VisitContext) => void;
  byType?: Record<string, (node: unknown, context: VisitContext) => void>;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isTypedNode(value: unknown): value is { type: string } & Record<string, unknown> {
  return isObject(value) && typeof value.type === 'string';
}

/**
 * Traverse AST nodes depth-first and invoke visitor callbacks.
 *
 * This is intentionally schema-agnostic: it walks object/array structure and
 * treats any object with a string `type` field as a node. That keeps traversal
 * resilient as AST node variants evolve.
 */
export function visitAst(
  root: AST.Node | readonly AST.Node[],
  visitor: AstVisitor
): void {
  const walk = (
    value: unknown,
    parent: unknown | null,
    key: string | number | undefined,
    depth: number
  ): void => {
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        walk(value[i], parent, i, depth);
      }
      return;
    }

    if (!isObject(value)) return;

    const context: VisitContext = { parent, key, depth };
    if (isTypedNode(value)) {
      visitor.enter?.(value, context);
      visitor.byType?.[value.type]?.(value, context);
    }

    for (const [childKey, childValue] of Object.entries(value)) {
      walk(childValue, value, childKey, depth + 1);
    }

    if (isTypedNode(value)) {
      visitor.leave?.(value, context);
    }
  };

  walk(root, null, undefined, 0);
}

