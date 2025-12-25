from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from .errors import ApiError

PROFILE_ID = "northwind_meta_v0"

# Bit-profile: northwind_meta_v0
# - 0–255: identification bits
# - 256–1023: descriptive bits
# - 1024–1791: relational bits
# - 1792–4095: reserved

# Identification bits
BIT_IS_TABLE = 0
BIT_IS_COLUMN = 1
BIT_IS_FK_REL = 5

BIT_TABLE_META = 17
BIT_COLUMN_META = 18
BIT_REL_META = 19

# Column data type family
BIT_TYPE_TEXT = 256
BIT_TYPE_INTEGER = 257
BIT_TYPE_REAL = 258
BIT_TYPE_NUMERIC = 259
BIT_TYPE_DATETIME = 260
BIT_TYPE_BLOB = 261

# Column attributes
BIT_NOT_NULL = 272
BIT_NULL_ALLOWED = 273
BIT_HAS_DEFAULT = 274
BIT_PART_OF_PK = 275
BIT_PART_OF_FK = 276
BIT_HAS_INDEX = 278

# Column length buckets
BIT_LEN_SMALL = 288
BIT_LEN_MEDIUM = 289
BIT_LEN_LARGE = 290
BIT_LEN_HUGE = 291

# Relation bits
BIT_RELATION = 1024
BIT_CARD_1_1 = 1030
BIT_CARD_1_N = 1031
BIT_CHILD_MANDATORY = 1040
BIT_CHILD_OPTIONAL = 1041

# FK actions
BIT_DEL_CASCADE = 1050
BIT_DEL_SET_NULL = 1051
BIT_DEL_RESTRICT = 1052
BIT_UPD_CASCADE = 1053
BIT_UPD_SET_NULL = 1054
BIT_UPD_RESTRICT = 1055


def encode_flags_bin(bits: Iterable[int]) -> bytes:
    buf = bytearray(512)
    for b in bits:
        if not isinstance(b, int) or b < 0 or b > 4095:
            raise ApiError("INVALID_BIT", "bit must be 0..4095", status_code=422, details={"bit": b})
        byte_from_end = b // 8
        i = 511 - byte_from_end
        buf[i] |= 1 << (b % 8)
    return bytes(buf)


@dataclass(frozen=True)
class DecodedColumnMeta:
    type_family: str | None
    not_null: bool | None
    has_default: bool
    is_pk: bool
    is_fk: bool
    has_index: bool
    length_bucket: str | None


@dataclass(frozen=True)
class DecodedRelationMeta:
    cardinality: str | None
    child_required: bool | None
    on_delete: str | None
    on_update: str | None


_LEN_RE = re.compile(r"\((\s*\d+\s*)\)")


def _normalize_declared_type(declared_type: str) -> str:
    return " ".join((declared_type or "").strip().upper().split())


def sqlite_type_family_bits(declared_type: str) -> tuple[int | None, int | None]:
    t = _normalize_declared_type(declared_type)
    length: int | None = None
    m = _LEN_RE.search(t)
    if m:
        try:
            length = int(m.group(1).strip())
        except ValueError:
            length = None

    if "INT" in t:
        return BIT_TYPE_INTEGER, length
    if any(x in t for x in ("CHAR", "CLOB", "TEXT", "VARCHAR")):
        return BIT_TYPE_TEXT, length
    if any(x in t for x in ("REAL", "FLOA", "DOUB")):
        return BIT_TYPE_REAL, length
    if any(x in t for x in ("DATE", "TIME")):
        return BIT_TYPE_DATETIME, length
    if "BLOB" in t:
        return BIT_TYPE_BLOB, length
    if any(x in t for x in ("NUMERIC", "DECIMAL", "BOOLEAN")):
        return BIT_TYPE_NUMERIC, length
    return None, length


def sqlite_length_bucket_bits(*, declared_type: str, family_bit: int | None, length: int | None) -> int | None:
    if family_bit not in (BIT_TYPE_TEXT, BIT_TYPE_BLOB, BIT_TYPE_NUMERIC, BIT_TYPE_REAL, BIT_TYPE_INTEGER, BIT_TYPE_DATETIME):
        return None

    t = _normalize_declared_type(declared_type)
    is_sized = "(" in t and ")" in t
    if length is None and not is_sized:
        if family_bit == BIT_TYPE_TEXT:
            return BIT_LEN_HUGE
        return None

    if length is None:
        return BIT_LEN_HUGE
    if length <= 64:
        return BIT_LEN_SMALL
    if length <= 255:
        return BIT_LEN_MEDIUM
    if length <= 4000:
        return BIT_LEN_LARGE
    return BIT_LEN_HUGE


def bits_for_table() -> set[int]:
    return {BIT_IS_TABLE, BIT_TABLE_META}


def bits_for_column(
    *,
    declared_type: str,
    not_null: bool,
    has_default: bool,
    is_pk: bool,
    is_fk: bool,
    has_index: bool,
) -> set[int]:
    bits: set[int] = {BIT_IS_COLUMN, BIT_COLUMN_META}
    family_bit, length = sqlite_type_family_bits(declared_type)
    if family_bit is not None:
        bits.add(family_bit)
    len_bit = sqlite_length_bucket_bits(declared_type=declared_type, family_bit=family_bit, length=length)
    if len_bit is not None:
        bits.add(len_bit)

    bits.add(BIT_NOT_NULL if not_null else BIT_NULL_ALLOWED)
    if has_default:
        bits.add(BIT_HAS_DEFAULT)
    if is_pk:
        bits.add(BIT_PART_OF_PK)
    if is_fk:
        bits.add(BIT_PART_OF_FK)
    if has_index:
        bits.add(BIT_HAS_INDEX)
    return bits


def _is_restrict(action: str) -> bool:
    a = (action or "").strip().upper()
    return not a or a in ("RESTRICT", "NO ACTION", "SET DEFAULT")


def bits_for_relation(
    *,
    is_unique_child: bool,
    child_mandatory: bool,
    on_delete: str,
    on_update: str,
) -> set[int]:
    bits: set[int] = {BIT_IS_FK_REL, BIT_REL_META, BIT_RELATION}
    bits.add(BIT_CARD_1_1 if is_unique_child else BIT_CARD_1_N)
    bits.add(BIT_CHILD_MANDATORY if child_mandatory else BIT_CHILD_OPTIONAL)

    od = (on_delete or "").strip().upper()
    if od == "CASCADE":
        bits.add(BIT_DEL_CASCADE)
    elif od == "SET NULL":
        bits.add(BIT_DEL_SET_NULL)
    elif _is_restrict(od):
        bits.add(BIT_DEL_RESTRICT)

    ou = (on_update or "").strip().upper()
    if ou == "CASCADE":
        bits.add(BIT_UPD_CASCADE)
    elif ou == "SET NULL":
        bits.add(BIT_UPD_SET_NULL)
    elif _is_restrict(ou):
        bits.add(BIT_UPD_RESTRICT)

    return bits


def decode_column_meta(bits: set[int]) -> DecodedColumnMeta:
    family = (
        "TEXT"
        if BIT_TYPE_TEXT in bits
        else "INTEGER"
        if BIT_TYPE_INTEGER in bits
        else "REAL"
        if BIT_TYPE_REAL in bits
        else "NUMERIC"
        if BIT_TYPE_NUMERIC in bits
        else "DATETIME"
        if BIT_TYPE_DATETIME in bits
        else "BLOB"
        if BIT_TYPE_BLOB in bits
        else None
    )
    not_null = True if BIT_NOT_NULL in bits else False if BIT_NULL_ALLOWED in bits else None
    length_bucket = (
        "small"
        if BIT_LEN_SMALL in bits
        else "medium"
        if BIT_LEN_MEDIUM in bits
        else "large"
        if BIT_LEN_LARGE in bits
        else "huge"
        if BIT_LEN_HUGE in bits
        else None
    )
    return DecodedColumnMeta(
        type_family=family,
        not_null=not_null,
        has_default=(BIT_HAS_DEFAULT in bits),
        is_pk=(BIT_PART_OF_PK in bits),
        is_fk=(BIT_PART_OF_FK in bits),
        has_index=(BIT_HAS_INDEX in bits),
        length_bucket=length_bucket,
    )


def decode_relation_meta(bits: set[int]) -> DecodedRelationMeta:
    card = "1:1" if BIT_CARD_1_1 in bits else "1:N" if BIT_CARD_1_N in bits else None
    required = True if BIT_CHILD_MANDATORY in bits else False if BIT_CHILD_OPTIONAL in bits else None

    on_delete = (
        "CASCADE"
        if BIT_DEL_CASCADE in bits
        else "SET NULL"
        if BIT_DEL_SET_NULL in bits
        else "RESTRICT"
        if BIT_DEL_RESTRICT in bits
        else None
    )
    on_update = (
        "CASCADE"
        if BIT_UPD_CASCADE in bits
        else "SET NULL"
        if BIT_UPD_SET_NULL in bits
        else "RESTRICT"
        if BIT_UPD_RESTRICT in bits
        else None
    )
    return DecodedRelationMeta(cardinality=card, child_required=required, on_delete=on_delete, on_update=on_update)

