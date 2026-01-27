"""
Database layer with SQLAlchemy ORM.
Supports both SQLite and PostgreSQL.
Manages runs, news items, notifications, sources, and settings.
"""

import json
from datetime import datetime
from typing import Optional, List
from contextlib import contextmanager

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Boolean,
    DateTime, Float, ForeignKey, Index, event, Enum as SQLEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.pool import StaticPool

from app.config import settings

# Create engine - conditional based on database type
if settings.database_url.startswith("sqlite"):
    # SQLite-specific configuration
    engine = create_engine(
        settings.database_url,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=settings.debug
    )

    # Enable foreign keys for SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()
else:
    # PostgreSQL configuration
    engine = create_engine(
        settings.database_url,
        echo=settings.debug,
        pool_pre_ping=True
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ============== Models ==============

class Source(Base):
    """Configurable scraping sources."""
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    code = Column(String(50), unique=True, nullable=False)  # 'mnre', 'seci', etc.
    url = Column(String(500), nullable=False)
    scraper_type = Column(String(50), default="generic")  # 'mnre', 'seci', 'generic'
    selectors = Column(Text)  # JSON config for generic scraper
    enabled = Column(Boolean, default=True)
    last_scraped_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    news_items = relationship("NewsItem", back_populates="source_rel")

    def get_selectors(self) -> dict:
        """Parse selectors JSON."""
        if self.selectors:
            return json.loads(self.selectors)
        return {}

    def set_selectors(self, config: dict):
        """Set selectors from dict."""
        self.selectors = json.dumps(config)


class Run(Base):
    """Execution run history."""
    __tablename__ = "runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(20), nullable=False, default="running")  # running, success, failed, partial
    sources_scraped = Column(Text)  # JSON array of source codes
    items_found = Column(Integer, default=0)
    new_items = Column(Integer, default=0)
    error_message = Column(Text)
    triggered_by = Column(String(20), default="schedule")  # schedule, manual, api
    duration_seconds = Column(Float)

    # Relationships
    news_items = relationship("NewsItem", back_populates="run")
    notifications = relationship("Notification", back_populates="run")

    __table_args__ = (
        Index("idx_runs_status", "status"),
        Index("idx_runs_started", "started_at"),
    )

    def get_sources_scraped(self) -> list:
        """Parse sources JSON."""
        if self.sources_scraped:
            return json.loads(self.sources_scraped)
        return []

    def set_sources_scraped(self, sources: list):
        """Set sources from list."""
        self.sources_scraped = json.dumps(sources)


class NewsItem(Base):
    """Scraped news and tender items."""
    __tablename__ = "news_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), ForeignKey("sources.code"), nullable=False)
    title = Column(Text, nullable=False)
    url = Column(String(1000))
    published_date = Column(String(50))
    content_hash = Column(String(64), unique=True)  # SHA256 for deduplication
    run_id = Column(Integer, ForeignKey("runs.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    is_new = Column(Boolean, default=True)
    item_type = Column(String(20), nullable=True, default='news')  # 'tender' or 'news'

    # Relationships
    source_rel = relationship("Source", back_populates="news_items")
    run = relationship("Run", back_populates="news_items")

    __table_args__ = (
        Index("idx_news_source", "source"),
        Index("idx_news_created", "created_at"),
        Index("idx_news_hash", "content_hash"),
        Index("ix_news_items_item_type", "item_type"),
    )


class Notification(Base):
    """Notification history (Teams, email, etc.)."""
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("runs.id"))
    channel = Column(String(50), default="teams")  # teams, email, slack
    message = Column(Text)
    status = Column(String(20), default="pending")  # pending, sent, failed
    sent_at = Column(DateTime)
    error_message = Column(Text)

    # Relationships
    run = relationship("Run", back_populates="notifications")


class Setting(Base):
    """Application settings stored in database."""
    __tablename__ = "settings"

    key = Column(String(100), primary_key=True)
    value = Column(Text)
    description = Column(String(500))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Subscriber(Base):
    """Newsletter subscribers for notifications."""
    __tablename__ = "subscribers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    channels = Column(Text)  # JSON list of subscribed channels like ["teams", "whatsapp", "email"]
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_subscribers_email", "email"),
        Index("idx_subscribers_active", "active"),
    )

    def get_channels(self) -> list:
        """Parse channels JSON."""
        if self.channels:
            return json.loads(self.channels)
        return []

    def set_channels(self, channels: list):
        """Set channels from list."""
        self.channels = json.dumps(channels)


class Connector(Base):
    """Notification connectors configuration (Teams, WhatsApp, Email, etc.)."""
    __tablename__ = "connectors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_type = Column(String(50), unique=True, nullable=False)  # teams, whatsapp, email
    name = Column(String(100), nullable=False)
    config = Column(Text)  # JSON for credentials/webhook URLs
    enabled = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_connectors_channel_type", "channel_type"),
        Index("idx_connectors_enabled", "enabled"),
    )

    def get_config(self) -> dict:
        """Parse config JSON."""
        if self.config:
            return json.loads(self.config)
        return {}

    def set_config(self, config: dict):
        """Set config from dict."""
        self.config = json.dumps(config)


# ============== Database Functions ==============

def init_db():
    """Initialize database and create tables."""
    Base.metadata.create_all(bind=engine)

    # Seed default sources if empty
    with get_db() as db:
        if db.query(Source).count() == 0:
            default_sources = [
                Source(
                    name="Ministry of New and Renewable Energy",
                    code="mnre",
                    url="https://www.mnre.gov.in/tenders/recent",
                    scraper_type="mnre",
                    enabled=True
                ),
                Source(
                    name="Solar Energy Corporation of India",
                    code="seci",
                    url="https://seci.co.in/whats-new",
                    scraper_type="seci",
                    enabled=True
                )
            ]
            db.add_all(default_sources)
            db.commit()

        # Seed default settings if empty
        if db.query(Setting).count() == 0:
            default_settings = [
                Setting(key="schedule_time", value="06:00", description="Scheduled run time (HH:MM)"),
                Setting(key="schedule_enabled", value="true", description="Enable scheduled runs"),
                Setting(key="schedule_frequency", value="daily", description="Schedule frequency (daily or weekly)"),
                Setting(key="schedule_day_of_week", value="0", description="Day of week for weekly schedule (0=Monday, 6=Sunday)"),
                Setting(key="teams_webhook_url", value="", description="Microsoft Teams webhook URL"),
                Setting(key="notification_enabled", value="true", description="Enable notifications"),
            ]
            db.add_all(default_settings)
            db.commit()
        else:
            # Add new schedule settings if they don't exist (for existing databases)
            if not db.query(Setting).filter(Setting.key == "schedule_frequency").first():
                db.add(Setting(key="schedule_frequency", value="daily", description="Schedule frequency (daily or weekly)"))
            if not db.query(Setting).filter(Setting.key == "schedule_day_of_week").first():
                db.add(Setting(key="schedule_day_of_week", value="0", description="Day of week for weekly schedule (0=Monday, 6=Sunday)"))
            db.commit()

        # Seed default connectors if empty
        if db.query(Connector).count() == 0:
            default_connectors = [
                Connector(
                    channel_type="teams",
                    name="Microsoft Teams",
                    config=json.dumps({"webhook_url": ""}),
                    enabled=False
                ),
                Connector(
                    channel_type="whatsapp",
                    name="WhatsApp Business",
                    config=json.dumps({
                        "api_url": "",
                        "phone_number_id": "",
                        "access_token": ""
                    }),
                    enabled=False
                ),
                Connector(
                    channel_type="email",
                    name="Email (SMTP)",
                    config=json.dumps({
                        "smtp_host": "",
                        "smtp_port": 587,
                        "smtp_user": "",
                        "smtp_password": "",
                        "from_address": "",
                        "use_tls": True
                    }),
                    enabled=False
                )
            ]
            db.add_all(default_connectors)
            db.commit()


@contextmanager
def get_db():
    """Database session context manager."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session() -> Session:
    """Get database session for FastAPI dependency injection."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
