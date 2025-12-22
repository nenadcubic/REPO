#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

mkdir -p "$ROOT/.relay"
"$ROOT/tools/relay_collect.sh" "$ROOT/.relay/payload.txt"

if command -v wl-copy >/dev/null 2>&1; then
  wl-copy < "$ROOT/.relay/payload.txt"
elif command -v xclip >/dev/null 2>&1; then
  xclip -selection clipboard < "$ROOT/.relay/payload.txt"
else
  echo "Instaliraj clipboard alat:"
  echo "  Wayland: sudo apt install wl-clipboard"
  echo "  X11:     sudo apt install xclip"
  exit 1
fi

echo "OK: payload je u clipboardu. Ctrl+V u ChatGPT."

