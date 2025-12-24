from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values
from pydantic import Field
from pydantic.aliases import AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict


def apply_preset_env(*, preset: str, presets_dir: str) -> Path | None:
    preset_path = Path(presets_dir) / f"{preset}.env"
    if not preset_path.exists():
        return None

    values = dotenv_values(preset_path)
    for key, value in values.items():
        if value is None:
            continue
        if key not in os.environ or os.environ[key] == "":
            os.environ[key] = value

    return preset_path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    gui_preset: str = Field(default="default", validation_alias=AliasChoices("GUI_PRESET", "ER_GUI_PRESET"))
    presets_dir: str = Field(default="/app/presets", validation_alias=AliasChoices("ER_GUI_PRESETS_DIR"))

    er_prefix: str = Field(default="er", validation_alias=AliasChoices("ER_PREFIX", "ER_REDIS_PREFIX"))

    redis_host: str = Field(default="redis", validation_alias=AliasChoices("ER_REDIS_HOST", "REDIS_HOST"))
    redis_port: int = Field(default=6379, validation_alias=AliasChoices("ER_REDIS_PORT", "REDIS_PORT"))

    er_cli_path: str = Field(default="/usr/local/bin/er_cli", validation_alias=AliasChoices("ER_CLI_PATH"))

    log_path: str = Field(default="/app/logs/backend.log", validation_alias=AliasChoices("ER_GUI_LOG_PATH"))
    log_level: str = Field(default="info", validation_alias=AliasChoices("ER_GUI_LOG_LEVEL"))

    store_preview_limit: int = Field(default=25, validation_alias=AliasChoices("ER_GUI_STORE_PREVIEW_LIMIT"))
    ttl_max_sec: int = Field(default=86400, validation_alias=AliasChoices("ER_GUI_TTL_MAX_SEC"))


def load_settings() -> tuple[Settings, Path | None]:
    preset = os.getenv("GUI_PRESET", "default")
    presets_dir = os.getenv("ER_GUI_PRESETS_DIR", "/app/presets")
    preset_path = apply_preset_env(preset=preset, presets_dir=presets_dir)
    return Settings(), preset_path
