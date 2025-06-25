from typing import Tuple, Type, Optional

import os
import logging
import logging.config

from pydantic import BaseModel, PositiveInt
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


class DatabaseSettings(BaseModel):
    host: str
    port: PositiveInt
    user: str
    database: str
    password: str = ""


class LookupSettings(BaseModel):
    duckdb_search: str = "search.duckdb"
    sqlite_fts: str = "search_fts.sqlite"
    local_taxonomy: str = "local_taxonomy.duckdb"
    build_taxonomy_fts: bool = False


class TomlSettings(BaseSettings):
    model_config = SettingsConfigDict(toml_file="config.toml")
    source_database: Optional[DatabaseSettings] = None
    lookups: LookupSettings = LookupSettings()
    log_config: Optional[str] = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (TomlConfigSettingsSource(settings_cls),)

    def enable_logging(self):
        if self.log_config:
            logging.config.fileConfig(self.log_config)
        else:
            logging.basicConfig(level=logging.INFO)


def get_config(path: str = None):
    """Create a config dictionary from the given path, take the location from env variable SEARCH_CONFIG or defaults to config.toml"""
    config_path = None
    if path:
        config_path = path
    elif "SEARCH_CONFIG" in os.environ:
        config_path = os.environ.get("SEARCH_CONFIG")
    return TomlSettings(toml_file=config_path)
