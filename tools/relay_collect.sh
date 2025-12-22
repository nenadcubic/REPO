#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

OUT="${1:-$ROOT/.relay/payload.txt}"

# Limits (tune as needed)
MAX_DIFF_LINES="${MAX_DIFF_LINES:-3500}"         # total diff lines included
MAX_FILE_PREVIEW_LINES="${MAX_FILE_PREVIEW_LINES:-220}"  # per-file preview lines
SHOW_WORKTREE_DIFF="${SHOW_WORKTREE_DIFF:-0}"   # 1 to include unstaged changes

mkdir -p "$(dirname "$OUT")"

{
  echo "### RELAY PAYLOAD (chat-friendly)"
  echo "DATE: $(date -Is)"
  echo

  echo "## 0) Intent"
  echo "- Please review the staged changes (what is going to be committed)."
  echo "- Goal: detect regressions, architecture drift, and propose next steps."
  echo "- Reply with: (1) Risks, (2) Required fixes, (3) Nice-to-haves, (4) Next-step instructions for Codex."
  echo

  echo "## 1) git status (porcelain)"
  git status --porcelain=v1 || true
  echo

  echo "## 2) last commits (log -5)"
  git --no-pager log -5 --oneline || true
  echo

  echo "## 3) staged files"
  git diff --staged --name-only || true
  echo

  echo "## 4) staged diff (trimmed to ${MAX_DIFF_LINES} lines)"
  echo "NOTE: If diff is trimmed, ask me for specific files and I will paste them."
  echo
  git --no-pager diff --staged | sed -n "1,${MAX_DIFF_LINES}p" || true
  echo

  echo "## 5) per-file staged preview (first ${MAX_FILE_PREVIEW_LINES} lines each)"
  echo "NOTE: This is a quick context preview, not the full file."
  echo

  # Preview only changed files (staged)
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    if [ -f "$f" ]; then
      echo "### FILE: $f"
      sed -n "1,${MAX_FILE_PREVIEW_LINES}p" "$f" || true
      echo
    else
      echo "### FILE: $f (not a regular file on disk)"
      echo
    fi
  done < <(git diff --staged --name-only || true)

  # Optional: include working tree diff (unstaged)
  if [ "$SHOW_WORKTREE_DIFF" = "1" ]; then
    echo "## 6) worktree diff (UNSTAGED) - trimmed to ${MAX_DIFF_LINES} lines"
    echo "WARNING: This includes changes not yet staged."
    echo
    git --no-pager diff | sed -n "1,${MAX_DIFF_LINES}p" || true
    echo
  fi

  # Optional project context file
  if [ -f "PROJECT_CONTEXT.md" ]; then
    echo "## 7) PROJECT_CONTEXT.md"
    sed -n "1,260p" "PROJECT_CONTEXT.md" || true
    echo
  else
    echo "## 7) PROJECT_CONTEXT.md"
    echo "(missing) — consider adding a short context file (goal, current state, rules, next steps)."
    echo
  fi

} > "$OUT"

echo "Wrote $OUT"
echo "Tips:"
echo "- To include unstaged changes too: SHOW_WORKTREE_DIFF=1 tools/relay_collect.sh"
echo "- To adjust size: MAX_DIFF_LINES=5000 MAX_FILE_PREVIEW_LINES=300 tools/relay_collect.sh"
#!/usr/bin/env bash
set -euo pipefail

# Always operate from repo root (works from any cwd)
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

OUT="${1:-$ROOT/.relay/payload.txt}"

# Limits (tune as needed)
MAX_DIFF_LINES="${MAX_DIFF_LINES:-3500}"            # total diff lines included
MAX_FILE_PREVIEW_LINES="${MAX_FILE_PREVIEW_LINES:-220}" # per-file preview lines
SHOW_WORKTREE_DIFF="${SHOW_WORKTREE_DIFF:-0}"       # 1 to include unstaged changes

mkdir -p "$(dirname "$OUT")"

{
  echo "### RELAY PAYLOAD (chat-friendly)"
  echo "DATE: $(date -Is)"
  echo

  echo "## 0) Intent"
  echo "- Please review the staged changes (what is going to be committed)."
  echo "- Goal: detect regressions, architecture drift, and propose next steps."
  echo "- Reply with: (1) Risks, (2) Required fixes, (3) Nice-to-haves, (4) Next-step instructions for Codex."
  echo

  echo "## 1) git status (porcelain)"
  git status --porcelain=v1 || true
  echo

  echo "## 2) last commits (log -5)"
  git --no-pager log -5 --oneline || true
  echo

  echo "## 3) staged files"
  git diff --staged --name-only || true
  echo

  echo "## 3a) staged diff stats"
  git --no-pager diff --staged --stat || true
  echo

  echo "## 4) staged diff (trimmed to ${MAX_DIFF_LINES} lines)"
  echo "NOTE: If diff is trimmed, ask me for specific files and I will paste them."
  echo
  git --no-pager diff --staged | sed -n "1,${MAX_DIFF_LINES}p" || true
  echo

  echo "## 5) per-file staged preview (first ${MAX_FILE_PREVIEW_LINES} lines each)"
  echo "NOTE: This is a quick context preview, not the full file."
  echo

  # Preview only changed files (staged)
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    if [ -f "$f" ]; then
      echo "### FILE: $f"
      sed -n "1,${MAX_FILE_PREVIEW_LINES}p" "$f" || true
      echo
    else
      echo "### FILE: $f (not a regular file on disk)"
      echo
    fi
  done < <(git diff --staged --name-only || true)

  # Optional: include working tree diff (unstaged)
  if [ "$SHOW_WORKTREE_DIFF" = "1" ]; then
    echo "## 6) worktree diff (UNSTAGED) - trimmed to ${MAX_DIFF_LINES} lines"
    echo "WARNING: This includes changes not yet staged."
    echo
    git --no-pager diff | sed -n "1,${MAX_DIFF_LINES}p" || true
    echo
  fi

  # Optional project context file
  if [ -f "PROJECT_CONTEXT.md" ]; then
    echo "## 7) PROJECT_CONTEXT.md"
    sed -n "1,260p" "PROJECT_CONTEXT.md" || true
    echo
  else
    echo "## 7) PROJECT_CONTEXT.md"
    echo "(missing) — consider adding a short context file (goal, current state, rules, next steps)."
    echo
  fi

} > "$OUT"

echo "Wrote $OUT"
echo "Tips:"
echo "- Include unstaged changes: SHOW_WORKTREE_DIFF=1 tools/relay_collect.sh"
echo "- Adjust size: MAX_DIFF_LINES=5000 MAX_FILE_PREVIEW_LINES=300 tools/relay_collect.sh"
