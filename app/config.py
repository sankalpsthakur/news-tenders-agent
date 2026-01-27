"""
Configuration management for Hygenco News & Tenders Monitor.
"""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # App settings
    app_name: str = "Hygenco News & Tenders Monitor"
    app_version: str = "1.0.0"
    debug: bool = False

    # Database
    database_url: str = "sqlite:///./data/hygenco.db"

    # Teams webhook
    teams_webhook_url: Optional[str] = None

    # Scraping settings
    request_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    rate_limit_delay: float = 2.0  # Delay between requests to same domain

    # Scheduler settings
    default_schedule: str = "06:00"  # Daily at 6 AM
    timezone: str = "Asia/Kolkata"

    # Paths
    base_dir: Path = Path(__file__).parent.parent
    data_dir: Path = base_dir / "data"
    templates_dir: Path = base_dir / "templates"
    static_dir: Path = base_dir / "static"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
