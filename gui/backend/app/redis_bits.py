from __future__ import annotations

from .errors import ApiError


def decode_flags_bin(flags_bin: bytes) -> list[int]:
    if len(flags_bin) != 512:
        raise ApiError(
            "INVALID_FLAGS",
            "flags_bin must be 512 bytes",
            status_code=502,
            details={"len": len(flags_bin)},
        )

    bits: list[int] = []
    for i in range(511, -1, -1):
        byte = flags_bin[i]
        if byte == 0:
            continue
        base = (511 - i) * 8
        for b in range(8):
            if byte & (1 << b):
                bits.append(base + b)
    return bits


def element_key(name: str) -> str:
    return f"er:element:{name}"


def element_key_with_prefix(prefix: str, name: str) -> str:
    prefix = (prefix or "er").strip(":")
    return f"{prefix}:element:{name}"
