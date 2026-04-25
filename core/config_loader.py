"""
Config loader module
Reads and validates YAML config files using Pydantic.
Each YAML file describes one API data source.
"""

from __future__ import annotations
from typing import Optional, Literal, Any
from pydantic import BaseModel, Field, field_validator, HttpUrl, model_validator
import yaml
from pathlib import Path
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


class AuthConfig(BaseModel):
    type: Literal["none", "api_key", "bearer"] = "none"
    token: Optional[str] = None  # for bearer
    api_key: Optional[str] = None  # for api_key
    api_key_header: str = "X-API-Key"  # header name for api_key auth

    @model_validator(mode="after")
    def validate_auth(self):
        if self.type == "bearer" and not self.token:
            raise ValueError("Bearer auth requires a token")
        if self.type == "api_key" and not self.api_key:
            raise ValueError("API key auth requires an 'api_key'")
        return self


class PaginationConfig(BaseModel):
    type: Literal["none", "offset", "page", "cursor"] = "none"
    results_path: str = "results"  # JSON path to results array
    limit_param: str = "limit"
    offset_param: str = "offset"
    page_param: str = "page"
    cursor_param: str = "cursor"
    next_cursor_path: Optional[str] = None  # path in response that holds next cursor
    limit: int = 100
    max_pages: int = 10  # safety cap


class ScheduleConfig(BaseModel):
    interval_mins: int = 60  # how often to run in minutes


class StorageConfig(BaseModel):
    table: str  # database table name
    upsert_key: Optional[str] = None  # column name to use for upsert, if any


class TransformConfig(BaseModel):
    rename: dict[str, str] = Field(
        default_factory=dict
    )  # mapping of old field names to new field names : {"old_name": "new_name"}
    exclude: list[str] = Field(default_factory=list)  # field to drop
    include: list[str] = Field(
        default_factory=list
    )  # if set, only include these fields


class SourceConfig(BaseModel):
    name: str
    url: HttpUrl
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    pagination: PaginationConfig = Field(default_factory=PaginationConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    storage: StorageConfig
    transform: TransformConfig = Field(default_factory=TransformConfig)

    @field_validator("name")
    @classmethod
    def name_must_be_slug(cls, v: str) -> str:
        import re

        if not re.match(r"^[a-z0-9_]+$", v):
            raise ValueError(
                "Name must be lowercase and can only contain letters, numbers, and underscores"
            )
        return v


def load_config(path: str | Path) -> SourceConfig:
    """Load and validate a YAML config file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        raw = yaml.safe_load(f)
    return SourceConfig(**raw)


def load_all_configs(configs_dir: str | Path) -> list[SourceConfig]:
    """Load all .yaml config files from a directory."""
    configs_dir = Path(configs_dir)
    configs: list[SourceConfig] = []

    for yaml_file in sorted(configs_dir.glob("*.yaml")):
        try:
            config = load_config(yaml_file)
            configs.append(config)
            logger.info(f"Loaded config: {yaml_file.name}")
        except Exception as e:
            logger.warning(f"[config_loader] Skipping {yaml_file.name}: {e}")
    return configs
