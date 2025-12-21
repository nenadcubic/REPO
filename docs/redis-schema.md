Redis keys used
===============

**Elements**
- `er:element:<name>` (HASH)
  - `name` (string)
  - `flags_bin` (512 bytes, big-endian 4096-bit bitmap)

**Per-bit indexes**
- `er:idx:bit:<bit>` (SET of element names)

**Universe**
- `er:all` (SET of all element names, used for NOT queries)

**Temporary results**
- `er:tmp:<tag>:<ns>` (SET)
  - created by `*_store` commands and expired automatically via TTL
