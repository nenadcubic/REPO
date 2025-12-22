# Project Context — element-redis

## Goal
Implement a Redis-backed bitmap universe (4096-bit attributes) with fast, composable,
and atomic set operations, usable both from CLI and future GUI.

The system must support:
- exact match
- all / any / not queries
- universe-wide negation
- atomic store + expire for derived sets

## Current State
- Core bitmap model implemented (4096-bit flags).
- Composite queries implemented:
  - find_all
  - find_any
  - find_not
  - universe_not
  - all_not
- Atomic store + expire implemented via Lua (SINTERSTORE / SUNIONSTORE / SDIFFSTORE + EXPIRE).
- CLI tool functional and tested manually.
- Relay review workflow added (relay_collect.sh + relay_clip.sh).

## Design Rules (Hard)
- Redis is the source of truth.
- Atomicity must be preserved for composite operations.
- Prefer explicit logic over clever abstractions.
- No hidden state; keys and naming must stay predictable.
- Git operations are done on host (not inside Docker).
- Docker containers must run with host UID/GID (no root-owned files).

## Non-Goals (For Now)
- No GUI yet (planned next).
- No distributed Redis cluster logic yet.
- No persistence beyond Redis primitives.
- No permissions / auth layer yet.

## Naming & Structure
- C++17
- CLI-oriented entry points
- Lua scripts embedded or versioned explicitly
- Avoid magic constants; document any unavoidable ones.

## Known Risks
- Large bitmap sets causing unexpected Redis memory growth.
- Lua scripts drifting from C++ logic if duplicated.
- CLI API becoming unstable before GUI layer is defined.

## Next Steps (Short-Term)
1. Define 2–3 concrete test use-cases (realistic scenarios).
2. Add minimal automated tests (even if CLI-driven).
3. Freeze CLI surface (arguments & semantics).
4. Start sketching GUI requirements based on real queries.

## Review Expectations
When reviewing changes, prioritize:
1. Semantic correctness of set logic.
2. Preservation of atomicity.
3. API stability.
4. Future GUI compatibility.
