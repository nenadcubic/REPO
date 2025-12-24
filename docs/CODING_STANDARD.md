# Coding Standard — core-first

## Hard Rules (Core)
- `core` has no exceptions for control flow.
- All fallible functions return `er::Result<T>` and are `[[nodiscard]]`.
- Redis operations go only through `er::RedisClient` (or the engine facade built on it).
- All Redis key strings go only through `include/er/keys.hpp`.
- Lua scripts that provide guarantees (atomic store+ttl) must have a single source of truth.
- `RedisClient` is single-thread / not thread-safe (no internal mutex as a default).

## Naming
- Files: `snake_case.hpp/.cpp` for new core code.
- Types: `PascalCase`
- Functions/methods: `snake_case`
- Members: `snake_case_`
- Constants: `kNameLikeThis`
- Enums: `enum class PascalCase { kValueLikeThis }`

## API Design
- Prefer `std::string_view` for read-only inputs.
- Prefer “take by value and move” when ownership is natural (`std::string key`).
- Keep query APIs logically `const`.
- Use batching/pipelining for ingest and multi-key operations when it reduces RTT.

## Adapters (CLI / Scripts / Examples)
- Keep them thin: parse → call core → print.
- Bash/Python are allowed as download/test drivers, but not as alternate implementations of core semantics.

