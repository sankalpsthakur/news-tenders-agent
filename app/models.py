"""
Pydantic schemas for API validation and serialization.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, HttpUrl, model_validator
from enum import Enum


# ============== Enums ==============

class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class TriggerType(str, Enum):
    SCHEDULE = "schedule"
    MANUAL = "manual"
    API = "api"


class NotificationStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class NotificationChannel(str, Enum):
    TEAMS = "teams"
    EMAIL = "email"
    SLACK = "slack"
    WHATSAPP = "whatsapp"


class ScheduleFrequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"


class DayOfWeek(int, Enum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


class ConnectorType(str, Enum):
    TEAMS = "teams"
    WHATSAPP = "whatsapp"
    EMAIL = "email"


# ============== Source Schemas ==============

class SourceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    url: str = Field(..., min_length=1)
    scraper_type: str = Field(default="generic")
    selectors: Optional[Dict[str, Any]] = None
    enabled: bool = True


class SourceCreate(SourceBase):
    code: str = Field(..., min_length=1, max_length=50, pattern=r"^[a-z0-9_]+$")


class SourceUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    url: Optional[str] = None
    scraper_type: Optional[str] = None
    selectors: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None


class SourceResponse(SourceBase):
    id: int
    code: str
    last_scraped_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ============== News Item Schemas ==============

class NewsItemBase(BaseModel):
    source: str
    title: str
    url: Optional[str] = None
    published_date: Optional[str] = None
    item_type: Optional[str] = None


class NewsItemCreate(NewsItemBase):
    content_hash: str
    run_id: Optional[int] = None


class NewsItemResponse(NewsItemBase):
    id: int
    content_hash: str
    run_id: Optional[int] = None
    created_at: datetime
    is_new: bool
    item_type: Optional[str] = None

    class Config:
        from_attributes = True


# ============== Run Schemas ==============

class RunBase(BaseModel):
    triggered_by: TriggerType = TriggerType.MANUAL


class RunCreate(RunBase):
    pass


class RunResponse(BaseModel):
    id: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: RunStatus
    sources_scraped: Optional[List[str]] = None
    items_found: int = 0
    new_items: int = 0
    error_message: Optional[str] = None
    triggered_by: str
    duration_seconds: Optional[float] = None

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def parse_sources_scraped(cls, data):
        """Parse JSON string for sources_scraped field."""
        import json
        if hasattr(data, '__dict__'):
            # SQLAlchemy model object
            result = {
                'id': data.id,
                'started_at': data.started_at,
                'completed_at': data.completed_at,
                'status': data.status,
                'sources_scraped': json.loads(data.sources_scraped) if isinstance(data.sources_scraped, str) and data.sources_scraped else data.sources_scraped,
                'items_found': data.items_found or 0,
                'new_items': data.new_items or 0,
                'error_message': data.error_message,
                'triggered_by': data.triggered_by,
                'duration_seconds': data.duration_seconds
            }
            return result
        elif isinstance(data, dict) and 'sources_scraped' in data:
            if isinstance(data['sources_scraped'], str):
                data['sources_scraped'] = json.loads(data['sources_scraped']) if data['sources_scraped'] else None
        return data


class RunDetail(RunResponse):
    news_items: List[NewsItemResponse] = []
    notifications: List["NotificationResponse"] = []


# ============== Notification Schemas ==============

class NotificationResponse(BaseModel):
    id: int
    run_id: Optional[int] = None
    channel: str
    message: Optional[str] = None
    status: str
    sent_at: Optional[datetime] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


# ============== Settings Schemas ==============

class SettingUpdate(BaseModel):
    value: str


class ScheduleConfigUpdate(BaseModel):
    """Schema for updating schedule configuration."""
    schedule_time: Optional[str] = Field(None, pattern=r"^([01]?[0-9]|2[0-3]):[0-5][0-9]$", description="Schedule time in HH:MM format")
    schedule_frequency: Optional[ScheduleFrequency] = Field(None, description="Schedule frequency (daily or weekly)")
    schedule_day_of_week: Optional[int] = Field(None, ge=0, le=6, description="Day of week for weekly schedule (0=Monday, 6=Sunday)")


class ScheduleConfigResponse(BaseModel):
    """Schema for schedule configuration response."""
    schedule_time: str
    schedule_frequency: str
    schedule_day_of_week: int
    schedule_enabled: bool
    next_run_at: Optional[datetime] = None


class SettingResponse(BaseModel):
    key: str
    value: Optional[str] = None
    description: Optional[str] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ============== Dashboard Schemas ==============

class DashboardStats(BaseModel):
    total_runs: int
    successful_runs: int
    failed_runs: int
    success_rate: float
    total_items: int
    items_today: int
    items_this_week: int
    items_this_month: int
    active_sources: int
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None


class RecentRun(BaseModel):
    id: int
    status: str
    items_found: int
    new_items: int
    started_at: datetime
    duration_seconds: Optional[float] = None


class DashboardResponse(BaseModel):
    stats: DashboardStats
    recent_runs: List[RecentRun]
    latest_items: List[NewsItemResponse]


# ============== Report Schemas ==============

class ReportPeriod(str, Enum):
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    CUSTOM = "custom"


class ReportRequest(BaseModel):
    period: ReportPeriod
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    sources: Optional[List[str]] = None  # Filter by source codes


class ReportSummary(BaseModel):
    period: str
    start_date: datetime
    end_date: datetime
    total_runs: int
    successful_runs: int
    total_items: int
    new_items: int
    items_by_source: Dict[str, int]
    runs_by_status: Dict[str, int]
    daily_breakdown: List[Dict[str, Any]]


class ReportEmailRequest(BaseModel):
    """Request schema for sending a report via email."""
    period: ReportPeriod
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    sources: Optional[List[str]] = None
    recipient_emails: List[str] = Field(..., min_length=1, description="List of recipient email addresses")


# ============== Subscriber Schemas ==============

class SubscriberBase(BaseModel):
    email: str = Field(..., min_length=1, max_length=255)
    name: str = Field(..., min_length=1, max_length=100)
    channels: List[str] = Field(default_factory=lambda: ["email"])
    active: bool = True


class SubscriberCreate(SubscriberBase):
    pass


class SubscriberUpdate(BaseModel):
    email: Optional[str] = Field(None, min_length=1, max_length=255)
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    channels: Optional[List[str]] = None
    active: Optional[bool] = None


class SubscriberResponse(SubscriberBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

    @classmethod
    def model_validate(cls, obj, **kwargs):
        """Custom validation to handle JSON string for channels."""
        import json
        if hasattr(obj, 'channels') and isinstance(obj.channels, str):
            try:
                obj_dict = {
                    'id': obj.id,
                    'email': obj.email,
                    'name': obj.name,
                    'channels': json.loads(obj.channels) if obj.channels else [],
                    'active': obj.active,
                    'created_at': obj.created_at,
                    'updated_at': obj.updated_at
                }
                return cls(**obj_dict)
            except (json.JSONDecodeError, TypeError):
                pass
        return super().model_validate(obj, **kwargs)


# ============== Connector Schemas ==============

class ConnectorBase(BaseModel):
    channel_type: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=100)
    config: Dict[str, Any] = Field(default_factory=dict)
    enabled: bool = False


class ConnectorCreate(ConnectorBase):
    pass


class ConnectorUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    config: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None


class ConnectorResponse(ConnectorBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

    @classmethod
    def model_validate(cls, obj, **kwargs):
        """Custom validation to handle JSON string for config."""
        import json
        if hasattr(obj, 'config') and isinstance(obj.config, str):
            try:
                obj_dict = {
                    'id': obj.id,
                    'channel_type': obj.channel_type,
                    'name': obj.name,
                    'config': json.loads(obj.config) if obj.config else {},
                    'enabled': obj.enabled,
                    'created_at': obj.created_at
                }
                return cls(**obj_dict)
            except (json.JSONDecodeError, TypeError):
                pass
        return super().model_validate(obj, **kwargs)


# ============== API Response Wrappers ==============

class APIResponse(BaseModel):
    success: bool
    message: Optional[str] = None
    data: Optional[Any] = None


class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    per_page: int
    pages: int


# Resolve forward references
RunDetail.model_rebuild()
