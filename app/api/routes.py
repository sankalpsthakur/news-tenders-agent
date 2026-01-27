"""
FastAPI routes for Hygenco News & Tenders Monitor.
Provides API endpoints and serves the dashboard UI.
"""

import json
from datetime import datetime, timedelta
from typing import Optional, List
import io
from fastapi import APIRouter, Depends, HTTPException, Query, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.database import get_db_session, Source, Run, NewsItem, Notification, Setting, Subscriber, Connector
from app.models import (
    SourceCreate, SourceUpdate, SourceResponse,
    RunResponse, RunDetail, NewsItemResponse,
    DashboardStats, DashboardResponse, RecentRun,
    SettingResponse, SettingUpdate,
    ScheduleConfigUpdate, ScheduleConfigResponse,
    ReportRequest, ReportSummary, ReportPeriod, ReportEmailRequest,
    APIResponse, PaginatedResponse,
    SubscriberCreate, SubscriberUpdate, SubscriberResponse,
    ConnectorCreate, ConnectorUpdate, ConnectorResponse
)
from app.config import settings

router = APIRouter()
templates = Jinja2Templates(directory=str(settings.templates_dir))


# ============== Dashboard Pages ==============

@router.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request, db: Session = Depends(get_db_session)):
    """Main dashboard page."""
    stats = get_dashboard_stats(db)
    recent_runs = get_recent_runs(db, limit=5)
    latest_items = get_latest_items(db, limit=10)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats,
        "recent_runs": recent_runs,
        "latest_items": latest_items,
        "page": "dashboard"
    })


@router.get("/runs", response_class=HTMLResponse)
async def runs_page(request: Request, db: Session = Depends(get_db_session)):
    """Run history page."""
    runs = db.query(Run).order_by(desc(Run.started_at)).limit(50).all()
    return templates.TemplateResponse("runs.html", {
        "request": request,
        "runs": runs,
        "page": "runs"
    })


@router.get("/news", response_class=HTMLResponse)
async def news_page(
    request: Request,
    item_type: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """News browser page - shows only news items by default."""
    query = db.query(NewsItem)
    # Filter for news items only (exclude tenders) unless item_type is explicitly specified
    if item_type:
        query = query.filter(NewsItem.item_type == item_type)
    else:
        query = query.filter(NewsItem.item_type == 'news')
    items = query.order_by(desc(NewsItem.created_at)).limit(100).all()
    sources = db.query(Source).filter(Source.enabled == True).all()
    return templates.TemplateResponse("news.html", {
        "request": request,
        "items": items,
        "sources": sources,
        "page": "news",
        "item_type": item_type
    })


@router.get("/tenders", response_class=HTMLResponse)
async def tenders_page(request: Request, db: Session = Depends(get_db_session)):
    """Tenders browser page - shows only tender items."""
    items = db.query(NewsItem).filter(NewsItem.item_type == 'tender').order_by(desc(NewsItem.created_at)).limit(100).all()
    sources = db.query(Source).filter(Source.enabled == True).all()
    return templates.TemplateResponse("tenders.html", {
        "request": request,
        "items": items,
        "sources": sources,
        "page": "tenders"
    })


@router.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request, db: Session = Depends(get_db_session)):
    """Sources management page."""
    sources = db.query(Source).order_by(Source.name).all()

    # Convert to serializable dicts for template
    sources_list = []
    for s in sources:
        sources_list.append({
            'id': s.id,
            'name': s.name,
            'code': s.code,
            'url': s.url,
            'scraper_type': s.scraper_type,
            'selectors': s.selectors,
            'enabled': s.enabled,
            'last_scraped_at': s.last_scraped_at.isoformat() if s.last_scraped_at else None,
            'created_at': s.created_at.isoformat() if s.created_at else None,
            'updated_at': s.updated_at.isoformat() if s.updated_at else None
        })

    return templates.TemplateResponse("sources.html", {
        "request": request,
        "sources": sources_list,
        "page": "sources"
    })


@router.get("/reports", response_class=HTMLResponse)
async def reports_page(request: Request, db: Session = Depends(get_db_session)):
    """Reports page."""
    return templates.TemplateResponse("reports.html", {
        "request": request,
        "page": "reports"
    })


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, db: Session = Depends(get_db_session)):
    """Settings page."""
    settings_list = db.query(Setting).all()
    settings_dict = {s.key: s.value for s in settings_list}
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "settings": settings_dict,
        "page": "settings"
    })


@router.get("/connectors", response_class=HTMLResponse)
async def connectors_page(request: Request, db: Session = Depends(get_db_session)):
    """Connectors management page."""
    import json
    connectors = db.query(Connector).all()
    subscribers = db.query(Subscriber).order_by(Subscriber.name).all()

    # Convert to serializable dicts
    connectors_list = []
    for c in connectors:
        connectors_list.append({
            'id': c.id,
            'channel_type': c.channel_type,
            'name': c.name,
            'config': json.loads(c.config) if isinstance(c.config, str) and c.config else (c.config or {}),
            'enabled': c.enabled,
            'created_at': c.created_at.isoformat() if c.created_at else None
        })

    subscribers_list = []
    for s in subscribers:
        subscribers_list.append({
            'id': s.id,
            'email': s.email,
            'name': s.name,
            'channels': json.loads(s.channels) if isinstance(s.channels, str) and s.channels else (s.channels or []),
            'active': s.active,
            'created_at': s.created_at.isoformat() if s.created_at else None,
            'updated_at': s.updated_at.isoformat() if s.updated_at else None
        })

    return templates.TemplateResponse("connectors.html", {
        "request": request,
        "connectors": connectors_list,
        "subscribers": subscribers_list,
        "page": "connectors"
    })


# ============== API Endpoints ==============

# --- Dashboard API ---

@router.get("/api/dashboard", response_model=DashboardResponse)
async def get_dashboard_data(db: Session = Depends(get_db_session)):
    """Get dashboard data."""
    stats = get_dashboard_stats(db)
    recent_runs = get_recent_runs(db, limit=5)
    latest_items = get_latest_items(db, limit=10)

    return DashboardResponse(
        stats=stats,
        recent_runs=[RecentRun(
            id=r.id,
            status=r.status,
            items_found=r.items_found,
            new_items=r.new_items,
            started_at=r.started_at,
            duration_seconds=r.duration_seconds
        ) for r in recent_runs],
        latest_items=[NewsItemResponse.model_validate(i) for i in latest_items]
    )


# --- Runs API ---

@router.get("/api/runs", response_model=List[RunResponse])
async def list_runs(
    status: Optional[str] = None,
    limit: int = Query(default=20, le=100),
    offset: int = 0,
    db: Session = Depends(get_db_session)
):
    """List runs with optional filtering."""
    query = db.query(Run)
    if status:
        query = query.filter(Run.status == status)
    runs = query.order_by(desc(Run.started_at)).offset(offset).limit(limit).all()
    return [RunResponse.model_validate(r) for r in runs]


@router.get("/api/runs/{run_id}", response_model=RunDetail)
async def get_run(run_id: int, db: Session = Depends(get_db_session)):
    """Get run details."""
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    items = db.query(NewsItem).filter(NewsItem.run_id == run_id).all()
    notifications = db.query(Notification).filter(Notification.run_id == run_id).all()

    run_dict = RunResponse.model_validate(run).model_dump()
    run_dict["news_items"] = [NewsItemResponse.model_validate(i) for i in items]
    run_dict["notifications"] = notifications

    return RunDetail(**run_dict)


@router.post("/api/runs/trigger", response_model=APIResponse)
async def trigger_run(
    background_tasks: BackgroundTasks,
    sources: Optional[List[str]] = Query(default=None),
    db: Session = Depends(get_db_session)
):
    """Trigger a manual run."""
    from app.services.runner import RunnerService

    # Create run record
    run = Run(
        started_at=datetime.utcnow(),
        status="running",
        triggered_by="manual"
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # Execute in background
    background_tasks.add_task(execute_run_task, run.id, sources)

    return APIResponse(
        success=True,
        message=f"Run #{run.id} started",
        data={"run_id": run.id}
    )


async def execute_run_task(run_id: int, source_codes: Optional[List[str]] = None):
    """Background task to execute a run."""
    from app.services.runner import RunnerService
    from app.database import get_db

    with get_db() as db:
        try:
            runner = RunnerService(db)
            runner.execute_run(run_id, source_codes)
        except Exception as e:
            run = db.query(Run).filter(Run.id == run_id).first()
            if run:
                run.status = "failed"
                run.error_message = str(e)
                run.completed_at = datetime.utcnow()
                db.commit()


# --- Sources API ---

@router.get("/api/sources", response_model=List[SourceResponse])
async def list_sources(db: Session = Depends(get_db_session)):
    """List all sources."""
    sources = db.query(Source).order_by(Source.name).all()
    return [SourceResponse.model_validate(s) for s in sources]


@router.post("/api/sources", response_model=SourceResponse)
async def create_source(source: SourceCreate, db: Session = Depends(get_db_session)):
    """Create a new source."""
    existing = db.query(Source).filter(Source.code == source.code).first()
    if existing:
        raise HTTPException(status_code=400, detail="Source code already exists")

    db_source = Source(
        name=source.name,
        code=source.code,
        url=source.url,
        scraper_type=source.scraper_type,
        selectors=str(source.selectors) if source.selectors else None,
        enabled=source.enabled
    )
    db.add(db_source)
    db.commit()
    db.refresh(db_source)

    return SourceResponse.model_validate(db_source)


@router.put("/api/sources/{source_id}", response_model=SourceResponse)
async def update_source(
    source_id: int,
    source: SourceUpdate,
    db: Session = Depends(get_db_session)
):
    """Update a source."""
    db_source = db.query(Source).filter(Source.id == source_id).first()
    if not db_source:
        raise HTTPException(status_code=404, detail="Source not found")

    update_data = source.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        if key == "selectors" and value:
            value = str(value)
        setattr(db_source, key, value)

    db.commit()
    db.refresh(db_source)

    return SourceResponse.model_validate(db_source)


@router.delete("/api/sources/{source_id}", response_model=APIResponse)
async def delete_source(source_id: int, db: Session = Depends(get_db_session)):
    """Delete a source."""
    db_source = db.query(Source).filter(Source.id == source_id).first()
    if not db_source:
        raise HTTPException(status_code=404, detail="Source not found")

    db.delete(db_source)
    db.commit()

    return APIResponse(success=True, message="Source deleted")


@router.post("/api/sources/{source_id}/toggle", response_model=SourceResponse)
async def toggle_source(source_id: int, db: Session = Depends(get_db_session)):
    """Toggle source enabled/disabled."""
    db_source = db.query(Source).filter(Source.id == source_id).first()
    if not db_source:
        raise HTTPException(status_code=404, detail="Source not found")

    db_source.enabled = not db_source.enabled
    db.commit()
    db.refresh(db_source)

    return SourceResponse.model_validate(db_source)


# --- News API ---

@router.get("/api/news", response_model=List[NewsItemResponse])
async def list_news(
    source: Optional[str] = None,
    search: Optional[str] = None,
    item_type: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    db: Session = Depends(get_db_session)
):
    """List news items with filtering."""
    query = db.query(NewsItem)

    if source:
        query = query.filter(NewsItem.source == source)
    if search:
        query = query.filter(NewsItem.title.ilike(f"%{search}%"))
    if item_type:
        query = query.filter(NewsItem.item_type == item_type)

    items = query.order_by(desc(NewsItem.created_at)).offset(offset).limit(limit).all()
    return [NewsItemResponse.model_validate(i) for i in items]


# --- Settings API ---

@router.get("/api/settings", response_model=List[SettingResponse])
async def list_settings(db: Session = Depends(get_db_session)):
    """List all settings."""
    settings_list = db.query(Setting).all()
    return [SettingResponse.model_validate(s) for s in settings_list]


@router.put("/api/settings/{key}", response_model=SettingResponse)
async def update_setting(
    key: str,
    setting: SettingUpdate,
    db: Session = Depends(get_db_session)
):
    """Update a setting."""
    db_setting = db.query(Setting).filter(Setting.key == key).first()
    if not db_setting:
        db_setting = Setting(key=key, value=setting.value)
        db.add(db_setting)
    else:
        db_setting.value = setting.value
        db_setting.updated_at = datetime.utcnow()

    db.commit()
    db.refresh(db_setting)

    # If schedule changed, update scheduler
    if key == "schedule_time":
        try:
            from app.services.scheduler import SchedulerService
            scheduler = SchedulerService()
            scheduler.update_schedule(setting.value)
        except Exception:
            pass  # Scheduler might not be initialized

    return SettingResponse.model_validate(db_setting)


# --- Schedule Configuration API ---

@router.get("/api/schedule", response_model=ScheduleConfigResponse)
async def get_schedule_config(db: Session = Depends(get_db_session)):
    """Get current schedule configuration."""
    from app.services.scheduler import scheduler_service

    settings_dict = {s.key: s.value for s in db.query(Setting).all()}
    next_run = scheduler_service.get_next_run()

    return ScheduleConfigResponse(
        schedule_time=settings_dict.get("schedule_time", "06:00"),
        schedule_frequency=settings_dict.get("schedule_frequency", "daily"),
        schedule_day_of_week=int(settings_dict.get("schedule_day_of_week", "0")),
        schedule_enabled=settings_dict.get("schedule_enabled", "true").lower() == "true",
        next_run_at=next_run
    )


@router.put("/api/schedule", response_model=ScheduleConfigResponse)
async def update_schedule_config(
    config: ScheduleConfigUpdate,
    db: Session = Depends(get_db_session)
):
    """Update schedule configuration (time, frequency, day of week)."""
    from app.services.scheduler import scheduler_service

    # Update schedule time if provided
    if config.schedule_time is not None:
        setting = db.query(Setting).filter(Setting.key == "schedule_time").first()
        if setting:
            setting.value = config.schedule_time
            setting.updated_at = datetime.utcnow()
        else:
            db.add(Setting(key="schedule_time", value=config.schedule_time, description="Scheduled run time (HH:MM)"))

    # Update schedule frequency if provided
    if config.schedule_frequency is not None:
        setting = db.query(Setting).filter(Setting.key == "schedule_frequency").first()
        if setting:
            setting.value = config.schedule_frequency.value
            setting.updated_at = datetime.utcnow()
        else:
            db.add(Setting(key="schedule_frequency", value=config.schedule_frequency.value, description="Schedule frequency (daily or weekly)"))

    # Update schedule day of week if provided
    if config.schedule_day_of_week is not None:
        setting = db.query(Setting).filter(Setting.key == "schedule_day_of_week").first()
        if setting:
            setting.value = str(config.schedule_day_of_week)
            setting.updated_at = datetime.utcnow()
        else:
            db.add(Setting(key="schedule_day_of_week", value=str(config.schedule_day_of_week), description="Day of week for weekly schedule (0=Monday, 6=Sunday)"))

    db.commit()

    # Reschedule the job with new settings
    try:
        scheduler_service._reschedule_job()
    except Exception:
        pass  # Scheduler might not be initialized

    # Return updated configuration
    settings_dict = {s.key: s.value for s in db.query(Setting).all()}
    next_run = scheduler_service.get_next_run()

    return ScheduleConfigResponse(
        schedule_time=settings_dict.get("schedule_time", "06:00"),
        schedule_frequency=settings_dict.get("schedule_frequency", "daily"),
        schedule_day_of_week=int(settings_dict.get("schedule_day_of_week", "0")),
        schedule_enabled=settings_dict.get("schedule_enabled", "true").lower() == "true",
        next_run_at=next_run
    )


@router.put("/api/schedule/frequency", response_model=APIResponse)
async def update_schedule_frequency(
    frequency: str = Query(..., pattern="^(daily|weekly)$", description="Schedule frequency (daily or weekly)"),
    db: Session = Depends(get_db_session)
):
    """Update schedule frequency (daily or weekly)."""
    from app.services.scheduler import scheduler_service

    success = scheduler_service.update_frequency(frequency)
    if success:
        return APIResponse(success=True, message=f"Schedule frequency updated to {frequency}")
    else:
        raise HTTPException(status_code=400, detail="Invalid frequency value")


@router.put("/api/schedule/day-of-week", response_model=APIResponse)
async def update_schedule_day_of_week(
    day: int = Query(..., ge=0, le=6, description="Day of week (0=Monday, 6=Sunday)"),
    db: Session = Depends(get_db_session)
):
    """Update day of week for weekly schedule."""
    from app.services.scheduler import scheduler_service

    success = scheduler_service.update_day_of_week(day)
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    if success:
        return APIResponse(success=True, message=f"Schedule day updated to {day_names[day]}")
    else:
        raise HTTPException(status_code=400, detail="Invalid day of week value")


@router.get("/api/schedule/status", response_model=APIResponse)
async def get_schedule_status():
    """Get detailed scheduler status."""
    from app.services.scheduler import scheduler_service

    status = scheduler_service.get_status()
    return APIResponse(success=True, data=status)


# --- Reports API ---

@router.post("/api/reports/generate", response_model=ReportSummary)
async def generate_report(request: ReportRequest):
    """Generate a report for the specified period."""
    from app.services.reports import ReportService

    report_service = ReportService()
    return report_service.generate_summary(
        period=request.period,
        start_date=request.start_date,
        end_date=request.end_date,
        sources=request.sources
    )


@router.post("/api/reports/download-pdf")
async def download_pdf_report(request: ReportRequest):
    """
    Generate and download a PDF report for the specified period.

    Returns a PDF file as a downloadable attachment.
    """
    from app.services.reports import ReportService

    try:
        report_service = ReportService()
        pdf_bytes = report_service.generate_pdf_report(
            period=request.period,
            start_date=request.start_date,
            end_date=request.end_date,
            sources=request.sources
        )

        # Generate filename based on period and date
        period_name = request.period.value
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"hygenco_{period_name}_report_{timestamp}.pdf"

        # Return PDF as streaming response with download headers
        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(pdf_bytes))
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate PDF report: {str(e)}"
        )


@router.post("/api/reports/send-email", response_model=APIResponse)
async def send_report_email_endpoint(request: ReportEmailRequest):
    """
    Generate a PDF report and send it via email.

    This endpoint generates a PDF report for the specified period and sends it
    to the provided email addresses using the configured SMTP email connector.

    Args:
        request: ReportEmailRequest containing period, dates, sources, and recipient emails

    Returns:
        APIResponse with success status and message
    """
    from app.services.reports import ReportService
    from app.services.notifier import send_report_email

    # Validate recipient emails
    if not request.recipient_emails:
        raise HTTPException(
            status_code=400,
            detail="At least one recipient email address is required"
        )

    try:
        # Generate the PDF report
        report_service = ReportService()
        pdf_bytes = report_service.generate_pdf_report(
            period=request.period,
            start_date=request.start_date,
            end_date=request.end_date,
            sources=request.sources
        )

        # Calculate date range for email subject
        start, end = report_service._calculate_date_range(
            request.period,
            request.start_date,
            request.end_date
        )

        # Send the email with PDF attachment
        result = await send_report_email(
            report_pdf_bytes=pdf_bytes,
            recipient_emails=request.recipient_emails,
            report_period=request.period.value,
            report_start_date=start.strftime("%Y-%m-%d"),
            report_end_date=end.strftime("%Y-%m-%d")
        )

        if result.get("success"):
            return APIResponse(
                success=True,
                message=f"Report sent successfully to {len(request.recipient_emails)} recipient(s)",
                data={
                    "recipients": request.recipient_emails,
                    "period": request.period.value,
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat()
                }
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Failed to send email")
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate or send report: {str(e)}"
        )


# --- Connectors API ---

@router.get("/api/connectors", response_model=List[ConnectorResponse])
async def list_connectors(db: Session = Depends(get_db_session)):
    """List all connectors."""
    connectors = db.query(Connector).all()
    return [ConnectorResponse.model_validate(c) for c in connectors]


@router.post("/api/connectors", response_model=ConnectorResponse)
async def create_connector(connector: ConnectorCreate, db: Session = Depends(get_db_session)):
    """Create a new connector."""
    existing = db.query(Connector).filter(Connector.channel_type == connector.channel_type).first()
    if existing:
        raise HTTPException(status_code=400, detail="Connector for this channel type already exists")

    db_connector = Connector(
        channel_type=connector.channel_type,
        name=connector.name,
        config=json.dumps(connector.config) if connector.config else "{}",
        enabled=connector.enabled
    )
    db.add(db_connector)
    db.commit()
    db.refresh(db_connector)

    return ConnectorResponse.model_validate(db_connector)


@router.put("/api/connectors/{connector_id}", response_model=ConnectorResponse)
async def update_connector(
    connector_id: int,
    connector: ConnectorUpdate,
    db: Session = Depends(get_db_session)
):
    """Update a connector."""
    db_connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not db_connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    update_data = connector.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        if key == "config" and value is not None:
            value = json.dumps(value)
        setattr(db_connector, key, value)

    db.commit()
    db.refresh(db_connector)

    return ConnectorResponse.model_validate(db_connector)


@router.delete("/api/connectors/{connector_id}", response_model=APIResponse)
async def delete_connector(connector_id: int, db: Session = Depends(get_db_session)):
    """Delete a connector."""
    db_connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not db_connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    db.delete(db_connector)
    db.commit()

    return APIResponse(success=True, message="Connector deleted")


@router.post("/api/connectors/{connector_id}/toggle", response_model=ConnectorResponse)
async def toggle_connector(connector_id: int, db: Session = Depends(get_db_session)):
    """Toggle connector enabled/disabled."""
    db_connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not db_connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    db_connector.enabled = not db_connector.enabled
    db.commit()
    db.refresh(db_connector)

    return ConnectorResponse.model_validate(db_connector)


# --- Subscribers API ---

@router.get("/api/subscribers", response_model=List[SubscriberResponse])
async def list_subscribers(
    active: Optional[bool] = None,
    db: Session = Depends(get_db_session)
):
    """List all subscribers."""
    query = db.query(Subscriber)
    if active is not None:
        query = query.filter(Subscriber.active == active)
    subscribers = query.order_by(Subscriber.name).all()
    return [SubscriberResponse.model_validate(s) for s in subscribers]


@router.post("/api/subscribers", response_model=SubscriberResponse)
async def create_subscriber(subscriber: SubscriberCreate, db: Session = Depends(get_db_session)):
    """Create a new subscriber."""
    existing = db.query(Subscriber).filter(Subscriber.email == subscriber.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Subscriber with this email already exists")

    db_subscriber = Subscriber(
        email=subscriber.email,
        name=subscriber.name,
        channels=json.dumps(subscriber.channels) if subscriber.channels else "[]",
        active=subscriber.active
    )
    db.add(db_subscriber)
    db.commit()
    db.refresh(db_subscriber)

    return SubscriberResponse.model_validate(db_subscriber)


@router.put("/api/subscribers/{subscriber_id}", response_model=SubscriberResponse)
async def update_subscriber(
    subscriber_id: int,
    subscriber: SubscriberUpdate,
    db: Session = Depends(get_db_session)
):
    """Update a subscriber."""
    db_subscriber = db.query(Subscriber).filter(Subscriber.id == subscriber_id).first()
    if not db_subscriber:
        raise HTTPException(status_code=404, detail="Subscriber not found")

    # Check email uniqueness if being changed
    update_data = subscriber.model_dump(exclude_unset=True)
    if "email" in update_data and update_data["email"] != db_subscriber.email:
        existing = db.query(Subscriber).filter(Subscriber.email == update_data["email"]).first()
        if existing:
            raise HTTPException(status_code=400, detail="Subscriber with this email already exists")

    for key, value in update_data.items():
        if key == "channels" and value is not None:
            value = json.dumps(value)
        setattr(db_subscriber, key, value)

    db_subscriber.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(db_subscriber)

    return SubscriberResponse.model_validate(db_subscriber)


@router.delete("/api/subscribers/{subscriber_id}", response_model=APIResponse)
async def delete_subscriber(subscriber_id: int, db: Session = Depends(get_db_session)):
    """Delete a subscriber."""
    db_subscriber = db.query(Subscriber).filter(Subscriber.id == subscriber_id).first()
    if not db_subscriber:
        raise HTTPException(status_code=404, detail="Subscriber not found")

    db.delete(db_subscriber)
    db.commit()

    return APIResponse(success=True, message="Subscriber deleted")


@router.post("/api/subscribers/{subscriber_id}/toggle", response_model=SubscriberResponse)
async def toggle_subscriber(subscriber_id: int, db: Session = Depends(get_db_session)):
    """Toggle subscriber active/inactive."""
    db_subscriber = db.query(Subscriber).filter(Subscriber.id == subscriber_id).first()
    if not db_subscriber:
        raise HTTPException(status_code=404, detail="Subscriber not found")

    db_subscriber.active = not db_subscriber.active
    db_subscriber.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(db_subscriber)

    return SubscriberResponse.model_validate(db_subscriber)


# ============== Helper Functions ==============

def get_dashboard_stats(db: Session) -> DashboardStats:
    """Calculate dashboard statistics."""
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())
    month_start = today_start.replace(day=1)

    total_runs = db.query(Run).count()
    successful_runs = db.query(Run).filter(Run.status == "success").count()
    failed_runs = db.query(Run).filter(Run.status == "failed").count()

    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0

    total_items = db.query(NewsItem).count()
    items_today = db.query(NewsItem).filter(NewsItem.created_at >= today_start).count()
    items_this_week = db.query(NewsItem).filter(NewsItem.created_at >= week_start).count()
    items_this_month = db.query(NewsItem).filter(NewsItem.created_at >= month_start).count()

    active_sources = db.query(Source).filter(Source.enabled == True).count()

    last_run = db.query(Run).order_by(desc(Run.started_at)).first()
    last_run_at = last_run.started_at if last_run else None

    # Calculate next run time from settings
    schedule_time = db.query(Setting).filter(Setting.key == "schedule_time").first()
    next_run_at = None
    if schedule_time and schedule_time.value:
        try:
            hour, minute = map(int, schedule_time.value.split(":"))
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            next_run_at = next_run
        except Exception:
            pass

    return DashboardStats(
        total_runs=total_runs,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        success_rate=round(success_rate, 1),
        total_items=total_items,
        items_today=items_today,
        items_this_week=items_this_week,
        items_this_month=items_this_month,
        active_sources=active_sources,
        last_run_at=last_run_at,
        next_run_at=next_run_at
    )


def get_recent_runs(db: Session, limit: int = 5) -> List[Run]:
    """Get recent runs."""
    return db.query(Run).order_by(desc(Run.started_at)).limit(limit).all()


def get_latest_items(db: Session, limit: int = 10) -> List[NewsItem]:
    """Get latest news items."""
    return db.query(NewsItem).order_by(desc(NewsItem.created_at)).limit(limit).all()
