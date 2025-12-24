from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field


Name = Annotated[str, Field(min_length=1, max_length=100)]
Bit = Annotated[int, Field(ge=0, le=4095)]
TTL = Annotated[int, Field(gt=0)]


class PutRequest(BaseModel):
    ns: str | None = None
    name: Name
    bits: list[Bit] = Field(min_length=1)


class QueryFind(BaseModel):
    ns: str | None = None
    type: Literal["find"]
    bit: Bit
    limit: Annotated[int, Field(default=200, ge=1, le=5000)]


class QueryFindAll(BaseModel):
    ns: str | None = None
    type: Literal["find_all"]
    bits: list[Bit] = Field(min_length=2)
    limit: Annotated[int, Field(default=200, ge=1, le=5000)]


class QueryFindAny(BaseModel):
    ns: str | None = None
    type: Literal["find_any"]
    bits: list[Bit] = Field(min_length=2)
    limit: Annotated[int, Field(default=200, ge=1, le=5000)]


class QueryFindNot(BaseModel):
    ns: str | None = None
    type: Literal["find_not"]
    include_bit: Bit
    exclude_bits: list[Bit] = Field(min_length=1)
    limit: Annotated[int, Field(default=200, ge=1, le=5000)]


class QueryFindUniverseNot(BaseModel):
    ns: str | None = None
    type: Literal["find_universe_not"]
    exclude_bits: list[Bit] = Field(min_length=1)
    limit: Annotated[int, Field(default=200, ge=1, le=5000)]


QueryRequest = QueryFind | QueryFindAll | QueryFindAny | QueryFindNot | QueryFindUniverseNot


class StoreFindAll(BaseModel):
    ns: str | None = None
    type: Literal["find_all_store"]
    ttl_sec: TTL
    bits: list[Bit] = Field(min_length=2)


class StoreFindAny(BaseModel):
    ns: str | None = None
    type: Literal["find_any_store"]
    ttl_sec: TTL
    bits: list[Bit] = Field(min_length=2)


class StoreFindNot(BaseModel):
    ns: str | None = None
    type: Literal["find_not_store"]
    ttl_sec: TTL
    include_bit: Bit
    exclude_bits: list[Bit] = Field(min_length=1)


StoreRequest = StoreFindAll | StoreFindAny | StoreFindNot


class ExamplesRunRequest(BaseModel):
    ns: str | None = None
    reset: bool | None = None
