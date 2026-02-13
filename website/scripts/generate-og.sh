#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TEMPLATE="$ROOT_DIR/og-template.html"
OUTPUT="$ROOT_DIR/public/og.png"

if [ ! -f "$TEMPLATE" ]; then
  echo "Error: og-template.html not found at $TEMPLATE"
  exit 1
fi

echo "Rendering OG image..."
npx playwright screenshot \
  --viewport-size="1200,630" \
  --wait-for-timeout=4000 \
  "file://$TEMPLATE" \
  "$OUTPUT"

echo "Done: $OUTPUT"
