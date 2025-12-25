from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Literal

from .errors import ApiError

# Northwind DATA bit-profile v1 (row-level bitsets)
#
# Stored as 4096-bit integers (decimal string) under:
#   {pfx}:data:<TableName>:<RowId>
#
# Layout (coarse):
# - 0–63        : row/type flags (reserved / optional)
# - 256–1023    : Customers-related buckets
# - 1024–1791   : Products/Categories buckets
# - 1792–2047   : Orders/OrderDetails buckets
# - 2048–4095   : reserved
#
# This is intentionally simple and lossy (bucketed) so the demo can compare:
#   - exact SQL predicates on SQLite
#   - approximate bitset predicates in Redis
#
# The UI exposes only predicates that map cleanly to these buckets.

SUPPORTED_TABLE_TOKENS = ["Customers", "Orders", "OrderDetails", "Products", "Categories"]


def data_key(prefix: str, table: str, row_id: str) -> str:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)
    table = (table or "").strip()
    row_id = (row_id or "").strip()
    if not table or not row_id:
        raise ApiError("INVALID_INPUT", "table and row_id are required", status_code=422)
    return f"{pfx}:data:{table}:{row_id}"


def data_registry_key(prefix: str) -> str:
    pfx = (prefix or "").strip(":")
    return f"{pfx}:import:northwind_compare:data_bits"


def _int_from_bits(bits: Iterable[int]) -> int:
    x = 0
    for b in bits:
        if not isinstance(b, int) or b < 0 or b > 4095:
            raise ApiError("INVALID_BIT", "bit must be 0..4095", status_code=422, details={"bit": b})
        x |= 1 << b
    return x


def _norm(s: Any) -> str:
    if s is None:
        return ""
    out = str(s).strip()
    out = " ".join(out.split())
    return out


def _norm_upper(s: Any) -> str:
    return _norm(s).upper()


def _parse_int(s: Any) -> int | None:
    if s is None:
        return None
    if isinstance(s, (int, float)) and int(s) == s:
        return int(s)
    st = _norm(s)
    if not st:
        return None
    try:
        return int(st, 10)
    except ValueError:
        return None


def _parse_decimal(s: Any) -> Decimal | None:
    if s is None:
        return None
    if isinstance(s, Decimal):
        return s
    if isinstance(s, (int, float)):
        return Decimal(str(s))
    st = _norm(s)
    if not st:
        return None
    try:
        return Decimal(st)
    except InvalidOperation:
        return None


def _parse_date(s: Any) -> date | None:
    st = _norm(s)
    if not st:
        return None
    # Common SQLite northwind formats: "1996-07-04 00:00:00", "1996-07-04"
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(st, fmt).date()
        except ValueError:
            continue
    return None


# Customers buckets (256+)
BIT_CUST_COUNTRY_USA = 256
BIT_CUST_COUNTRY_UK = 257
BIT_CUST_COUNTRY_GERMANY = 258
BIT_CUST_COUNTRY_FRANCE = 259
BIT_CUST_COUNTRY_OTHER = 260

BIT_CUST_CITY_LONDON = 264
BIT_CUST_CITY_PARIS = 265
BIT_CUST_CITY_BERLIN = 266
BIT_CUST_CITY_SEATTLE = 267
BIT_CUST_CITY_MEXICO_DF = 268

COUNTRY_BITS: dict[str, int] = {
    "USA": BIT_CUST_COUNTRY_USA,
    "U.S.A.": BIT_CUST_COUNTRY_USA,
    "UK": BIT_CUST_COUNTRY_UK,
    "U.K.": BIT_CUST_COUNTRY_UK,
    "UNITED KINGDOM": BIT_CUST_COUNTRY_UK,
    "GERMANY": BIT_CUST_COUNTRY_GERMANY,
    "FRANCE": BIT_CUST_COUNTRY_FRANCE,
}

CITY_BITS: dict[str, int] = {
    "LONDON": BIT_CUST_CITY_LONDON,
    "PARIS": BIT_CUST_CITY_PARIS,
    "BERLIN": BIT_CUST_CITY_BERLIN,
    "SEATTLE": BIT_CUST_CITY_SEATTLE,
    "MÉXICO D.F.": BIT_CUST_CITY_MEXICO_DF,
    "MEXICO D.F.": BIT_CUST_CITY_MEXICO_DF,
}


def encode_customer_row(row: dict[str, Any]) -> int:
    bits: list[int] = []

    ctry = _norm_upper(row.get("Country"))
    if ctry:
        bits.append(COUNTRY_BITS.get(ctry, BIT_CUST_COUNTRY_OTHER))

    city = _norm_upper(row.get("City"))
    b = CITY_BITS.get(city)
    if b is not None:
        bits.append(b)

    return _int_from_bits(bits)


# Products/Categories buckets (1024+)
BIT_PROD_CATEGORY_BASE = 1024  # 1..32 => 1024..1055
BIT_PROD_PRICE_LT_10 = 1060
BIT_PROD_PRICE_10_20 = 1061
BIT_PROD_PRICE_20_50 = 1062
BIT_PROD_PRICE_GE_50 = 1063


def _price_bucket_bit(price: Decimal | None) -> int | None:
    if price is None:
        return None
    if price < 10:
        return BIT_PROD_PRICE_LT_10
    if price < 20:
        return BIT_PROD_PRICE_10_20
    if price < 50:
        return BIT_PROD_PRICE_20_50
    return BIT_PROD_PRICE_GE_50


def encode_product_row(row: dict[str, Any]) -> int:
    bits: list[int] = []
    cat_id = _parse_int(row.get("CategoryID"))
    if cat_id is not None and 1 <= cat_id <= 32:
        bits.append(BIT_PROD_CATEGORY_BASE + (cat_id - 1))

    price = _parse_decimal(row.get("UnitPrice"))
    pb = _price_bucket_bit(price)
    if pb is not None:
        bits.append(pb)

    return _int_from_bits(bits)


def encode_category_row(row: dict[str, Any]) -> int:
    bits: list[int] = []
    cat_id = _parse_int(row.get("CategoryID"))
    if cat_id is not None and 1 <= cat_id <= 32:
        bits.append(BIT_PROD_CATEGORY_BASE + (cat_id - 1))
    return _int_from_bits(bits)


# Orders / OrderDetails buckets (1792+)
BIT_ORD_YEAR_1996 = 1792
BIT_ORD_YEAR_1997 = 1793
BIT_ORD_YEAR_1998 = 1794
BIT_ORD_YEAR_OTHER = 1795


def _order_year_bit(d: date | None) -> int | None:
    if d is None:
        return None
    if d.year == 1996:
        return BIT_ORD_YEAR_1996
    if d.year == 1997:
        return BIT_ORD_YEAR_1997
    if d.year == 1998:
        return BIT_ORD_YEAR_1998
    return BIT_ORD_YEAR_OTHER


def encode_order_row(row: dict[str, Any]) -> int:
    bits: list[int] = []
    d = _parse_date(row.get("OrderDate"))
    yb = _order_year_bit(d)
    if yb is not None:
        bits.append(yb)
    return _int_from_bits(bits)


BIT_OD_QTY_LT_5 = 1856
BIT_OD_QTY_5_10 = 1857
BIT_OD_QTY_11_20 = 1858
BIT_OD_QTY_GT_20 = 1859
BIT_OD_DISCOUNT_GT_0 = 1864


def _qty_bucket_bit(qty: int | None) -> int | None:
    if qty is None:
        return None
    if qty < 5:
        return BIT_OD_QTY_LT_5
    if qty <= 10:
        return BIT_OD_QTY_5_10
    if qty <= 20:
        return BIT_OD_QTY_11_20
    return BIT_OD_QTY_GT_20


def encode_order_details_row(row: dict[str, Any]) -> int:
    bits: list[int] = []
    qty = _parse_int(row.get("Quantity"))
    qb = _qty_bucket_bit(qty)
    if qb is not None:
        bits.append(qb)

    disc = _parse_decimal(row.get("Discount"))
    if disc is not None and disc > 0:
        bits.append(BIT_OD_DISCOUNT_GT_0)

    return _int_from_bits(bits)


def encode_row_bits(*, table: str, row: dict[str, Any]) -> int:
    t = (table or "").strip()
    if t == "Customers":
        return encode_customer_row(row)
    if t == "Products":
        return encode_product_row(row)
    if t == "Categories":
        return encode_category_row(row)
    if t == "Orders":
        return encode_order_row(row)
    if t == "OrderDetails":
        return encode_order_details_row(row)
    raise ApiError("INVALID_INPUT", "unsupported table", status_code=422, details={"table": t})


def _mask_from_bits(bits: Iterable[int]) -> int:
    return _int_from_bits(bits)


@dataclass(frozen=True)
class BitCondition:
    kind: Literal["all", "any"]
    mask: int
    bits: list[int]
    label: str


def sql_expr_for(*, table: str, column: str) -> str:
    t = (table or "").strip()
    col = (column or "").strip()
    if not col:
        raise ApiError("INVALID_INPUT", "column is required", status_code=422)
    if t == "Orders" and col == "OrderYear":
        # SQLite: year from OrderDate.
        return 'CAST(strftime(\'%Y\', "OrderDate") AS INTEGER)'
    return f'"{col}"'


def bit_conditions_for(*, table: str, conditions: list[dict[str, Any]]) -> tuple[list[BitCondition], list[int]]:
    """
    Translate structured predicate into per-condition bit masks.

    Returns:
      - list of BitCondition (AND-composed)
      - flat list of referenced bit indices (for UI/debug)
    """
    t = (table or "").strip()
    if t not in SUPPORTED_TABLE_TOKENS:
        raise ApiError("INVALID_INPUT", "unsupported table", status_code=422, details={"table": t})

    out: list[BitCondition] = []
    referenced_bits: set[int] = set()

    for raw in conditions:
        if not isinstance(raw, dict):
            continue
        col = _norm(raw.get("column"))
        op = _norm(raw.get("op")) or "="
        val = raw.get("value")
        if not col:
            raise ApiError("INVALID_INPUT", "condition column is required", status_code=422)
        if op not in ("=", "<", "<=", ">", ">="):
            raise ApiError("INVALID_INPUT", "unsupported operator", status_code=422, details={"op": op})

        if t == "Customers":
            if col == "Country" and op == "=":
                ctry = _norm_upper(val)
                if not ctry:
                    raise ApiError("INVALID_INPUT", "Country value is required", status_code=422)
                b = COUNTRY_BITS.get(ctry)
                if b is None:
                    raise ApiError(
                        "INVALID_INPUT",
                        "unsupported Country (only demo buckets are supported)",
                        status_code=422,
                        details={"value": ctry, "supported": sorted(COUNTRY_BITS.keys())},
                    )
                out.append(BitCondition(kind="all", mask=_mask_from_bits([b]), bits=[b], label=f"Country={ctry}"))
                referenced_bits.add(b)
                continue
            if col == "City" and op == "=":
                city = _norm_upper(val)
                if not city:
                    raise ApiError("INVALID_INPUT", "City value is required", status_code=422)
                b = CITY_BITS.get(city)
                if b is None:
                    raise ApiError(
                        "INVALID_INPUT",
                        "unsupported City (only demo buckets are supported)",
                        status_code=422,
                        details={"value": city, "supported": sorted(CITY_BITS.keys())},
                    )
                out.append(BitCondition(kind="all", mask=_mask_from_bits([b]), bits=[b], label=f"City={city}"))
                referenced_bits.add(b)
                continue

        if t in ("Products", "Categories"):
            if col == "CategoryID" and op == "=":
                cat = _parse_int(val)
                if cat is None:
                    raise ApiError("INVALID_INPUT", "CategoryID must be an integer", status_code=422, details={"value": val})
                if not (1 <= cat <= 32):
                    raise ApiError("INVALID_INPUT", "CategoryID out of range", status_code=422, details={"value": cat})
                b = BIT_PROD_CATEGORY_BASE + (cat - 1)
                out.append(BitCondition(kind="all", mask=_mask_from_bits([b]), bits=[b], label=f"CategoryID={cat}"))
                referenced_bits.add(b)
                continue
            if t == "Products" and col == "UnitPrice":
                price = _parse_decimal(val)
                if price is None:
                    raise ApiError("INVALID_INPUT", "UnitPrice must be numeric", status_code=422, details={"value": val})
                # Bucketed comparisons.
                buckets = [
                    (BIT_PROD_PRICE_LT_10, Decimal("0"), Decimal("10")),
                    (BIT_PROD_PRICE_10_20, Decimal("10"), Decimal("20")),
                    (BIT_PROD_PRICE_20_50, Decimal("20"), Decimal("50")),
                    (BIT_PROD_PRICE_GE_50, Decimal("50"), None),
                ]
                allowed: list[int] = []
                for bit, lo, hi in buckets:
                    if op == "=":
                        if (hi is None and price >= lo) or (hi is not None and price >= lo and price < hi):
                            allowed = [bit]
                            break
                    elif op in (">", ">="):
                        if hi is None:
                            if price <= lo:
                                allowed.append(bit)
                        else:
                            if op == ">=" and hi <= price:
                                continue
                            if op == ">" and hi <= price:
                                continue
                            if hi > price:
                                allowed.append(bit)
                    elif op in ("<", "<="):
                        if hi is None:
                            # 50+ bucket only matches if predicate is < 50, which excludes it.
                            if price < lo or (op == "<=" and price <= lo):
                                continue
                        else:
                            if op == "<" and lo >= price:
                                continue
                            if op == "<=" and lo > price:
                                continue
                            if lo < price or (op == "<=" and lo <= price):
                                allowed.append(bit)

                if not allowed:
                    raise ApiError("INVALID_INPUT", "UnitPrice predicate not supported by buckets", status_code=422, details={"op": op, "value": str(price)})
                label = f"UnitPrice{op}{price}"
                kind: Literal["all", "any"] = "all" if len(allowed) == 1 else "any"
                out.append(BitCondition(kind=kind, mask=_mask_from_bits(allowed), bits=allowed, label=label))
                referenced_bits.update(allowed)
                continue

        if t == "Orders":
            if col == "OrderYear" and op == "=":
                y = _parse_int(val)
                if y is None:
                    raise ApiError("INVALID_INPUT", "OrderYear must be an integer", status_code=422, details={"value": val})
                bit = BIT_ORD_YEAR_OTHER
                if y == 1996:
                    bit = BIT_ORD_YEAR_1996
                elif y == 1997:
                    bit = BIT_ORD_YEAR_1997
                elif y == 1998:
                    bit = BIT_ORD_YEAR_1998
                out.append(BitCondition(kind="all", mask=_mask_from_bits([bit]), bits=[bit], label=f"OrderYear={y}"))
                referenced_bits.add(bit)
                continue

        if t == "OrderDetails":
            if col == "Quantity":
                qty = _parse_int(val)
                if qty is None:
                    raise ApiError("INVALID_INPUT", "Quantity must be an integer", status_code=422, details={"value": val})
                # Bucketed comparisons (approximate).
                buckets = [
                    (BIT_OD_QTY_LT_5, None, 5),
                    (BIT_OD_QTY_5_10, 5, 11),
                    (BIT_OD_QTY_11_20, 11, 21),
                    (BIT_OD_QTY_GT_20, 21, None),
                ]
                allowed: list[int] = []
                for bit, lo, hi in buckets:
                    if op == "=":
                        if (lo is None and qty < int(hi)) or (hi is None and qty >= int(lo)) or (lo is not None and hi is not None and qty >= int(lo) and qty < int(hi)):
                            allowed = [bit]
                            break
                    elif op in (">", ">="):
                        if hi is None:
                            if qty <= int(lo):
                                allowed.append(bit)
                        else:
                            if int(hi) > qty:
                                allowed.append(bit)
                    elif op in ("<", "<="):
                        if lo is None:
                            if qty < int(hi):
                                allowed.append(bit)
                        else:
                            if qty < int(lo) or (op == "<=" and qty <= int(lo)):
                                continue
                            allowed.append(bit)
                if not allowed:
                    raise ApiError("INVALID_INPUT", "Quantity predicate not supported by buckets", status_code=422, details={"op": op, "value": qty})
                kind: Literal["all", "any"] = "all" if len(allowed) == 1 else "any"
                out.append(BitCondition(kind=kind, mask=_mask_from_bits(allowed), bits=allowed, label=f"Quantity{op}{qty}"))
                referenced_bits.update(allowed)
                continue
            if col == "Discount" and op in (">", ">=", "="):
                disc = _parse_decimal(val)
                if disc is None:
                    raise ApiError("INVALID_INPUT", "Discount must be numeric", status_code=422, details={"value": val})
                if disc <= 0 and op != "=":
                    # "> 0" is the only meaningful bucket for v1.
                    raise ApiError("INVALID_INPUT", "only Discount > 0 is supported", status_code=422, details={"op": op, "value": str(disc)})
                out.append(BitCondition(kind="all", mask=_mask_from_bits([BIT_OD_DISCOUNT_GT_0]), bits=[BIT_OD_DISCOUNT_GT_0], label="Discount>0"))
                referenced_bits.add(BIT_OD_DISCOUNT_GT_0)
                continue

        raise ApiError(
            "INVALID_INPUT",
            "predicate not supported by bitset encoding",
            status_code=422,
            details={"table": t, "column": col, "op": op},
        )

    return out, sorted(referenced_bits)

