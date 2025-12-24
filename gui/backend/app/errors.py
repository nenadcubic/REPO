from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException


@dataclass(frozen=True)
class ApiError(Exception):
    code: str
    message: str
    status_code: int = 400
    details: dict[str, Any] | None = None

    def to_http(self) -> HTTPException:
        return HTTPException(
            status_code=self.status_code,
            detail={"code": self.code, "message": self.message, "details": self.details or {}},
        )


def ok(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data}


def err(code: str, message: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"ok": False, "error": {"code": code, "message": message, "details": details or {}}}

