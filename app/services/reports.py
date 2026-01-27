"""
Report generation service for Hygenco News & Tenders Monitor.

Provides statistical summaries and reports for scraping runs and
collected news items over various time periods.
"""

import io
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from collections import defaultdict
from pathlib import Path

from sqlalchemy import func, desc
from jinja2 import Environment, FileSystemLoader

from app.database import get_db, Run, NewsItem, Source
from app.models import ReportPeriod, ReportSummary
from app.config import settings

logger = logging.getLogger(__name__)


class ReportService:
    """
    Generates statistical reports and summaries for monitoring data.

    Supports weekly, monthly, quarterly, and annual report periods,
    with customizable date ranges and source filtering.
    """

    def __init__(self):
        """Initialize the report service."""
        pass

    def _calculate_date_range(
        self,
        period: ReportPeriod,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> tuple[datetime, datetime]:
        """
        Calculate the date range for a report period.

        Args:
            period: Report period type
            start_date: Optional custom start date
            end_date: Optional custom end date

        Returns:
            Tuple of (start_date, end_date)
        """
        now = datetime.utcnow()

        if period == ReportPeriod.CUSTOM:
            if not start_date:
                start_date = now - timedelta(days=7)
            if not end_date:
                end_date = now
            return start_date, end_date

        elif period == ReportPeriod.WEEKLY:
            # Start from beginning of current week (Monday)
            days_since_monday = now.weekday()
            start = now - timedelta(days=days_since_monday)
            start = start.replace(hour=0, minute=0, second=0, microsecond=0)
            return start, now

        elif period == ReportPeriod.MONTHLY:
            # Start from beginning of current month
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            return start, now

        elif period == ReportPeriod.QUARTERLY:
            # Start from beginning of current quarter
            quarter_month = ((now.month - 1) // 3) * 3 + 1
            start = now.replace(
                month=quarter_month,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
            return start, now

        elif period == ReportPeriod.ANNUAL:
            # Start from beginning of current year
            start = now.replace(
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
            return start, now

        else:
            # Default to last 7 days
            return now - timedelta(days=7), now

    def generate_summary(
        self,
        period: ReportPeriod = ReportPeriod.WEEKLY,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sources: Optional[List[str]] = None
    ) -> ReportSummary:
        """
        Generate a summary report for the specified period.

        Args:
            period: Report period type (weekly, monthly, quarterly, annual, custom)
            start_date: Custom start date (required for custom period)
            end_date: Custom end date (required for custom period)
            sources: Optional list of source codes to filter by

        Returns:
            ReportSummary with statistics and breakdowns
        """
        # Calculate date range
        start, end = self._calculate_date_range(period, start_date, end_date)

        logger.info(
            f"Generating {period.value} report from {start} to {end}"
            + (f" for sources: {sources}" if sources else "")
        )

        with get_db() as db:
            # Base queries for runs in period
            runs_query = db.query(Run).filter(
                Run.started_at >= start,
                Run.started_at <= end
            )

            # Base query for news items in period
            items_query = db.query(NewsItem).filter(
                NewsItem.created_at >= start,
                NewsItem.created_at <= end
            )

            # Apply source filter if specified
            if sources:
                items_query = items_query.filter(NewsItem.source.in_(sources))

            # Get total runs
            total_runs = runs_query.count()

            # Get successful runs
            successful_runs = runs_query.filter(Run.status == "success").count()

            # Get runs by status
            runs_by_status = {}
            status_counts = db.query(
                Run.status,
                func.count(Run.id)
            ).filter(
                Run.started_at >= start,
                Run.started_at <= end
            ).group_by(Run.status).all()

            for status, count in status_counts:
                runs_by_status[status] = count

            # Get total items
            total_items = items_query.count()

            # Get new items count
            new_items = items_query.filter(NewsItem.is_new == True).count()

            # Get items by source
            items_by_source = {}
            source_counts = db.query(
                NewsItem.source,
                func.count(NewsItem.id)
            ).filter(
                NewsItem.created_at >= start,
                NewsItem.created_at <= end
            )

            if sources:
                source_counts = source_counts.filter(NewsItem.source.in_(sources))

            source_counts = source_counts.group_by(NewsItem.source).all()

            for source_code, count in source_counts:
                items_by_source[source_code] = count

            # Generate daily breakdown
            daily_breakdown = self._generate_daily_breakdown(
                db, start, end, sources
            )

            return ReportSummary(
                period=period.value,
                start_date=start,
                end_date=end,
                total_runs=total_runs,
                successful_runs=successful_runs,
                total_items=total_items,
                new_items=new_items,
                items_by_source=items_by_source,
                runs_by_status=runs_by_status,
                daily_breakdown=daily_breakdown
            )

    def _generate_daily_breakdown(
        self,
        db,
        start_date: datetime,
        end_date: datetime,
        sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate a day-by-day breakdown of items and runs.

        Args:
            db: Database session
            start_date: Start of period
            end_date: End of period
            sources: Optional source filter

        Returns:
            List of daily statistics dictionaries
        """
        breakdown = []
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        while current <= end:
            day_start = current
            day_end = current.replace(hour=23, minute=59, second=59, microsecond=999999)

            # Count runs for this day
            runs_count = db.query(Run).filter(
                Run.started_at >= day_start,
                Run.started_at <= day_end
            ).count()

            # Count items for this day
            items_query = db.query(NewsItem).filter(
                NewsItem.created_at >= day_start,
                NewsItem.created_at <= day_end
            )

            if sources:
                items_query = items_query.filter(NewsItem.source.in_(sources))

            items_count = items_query.count()

            # Count new items for this day
            new_items_count = items_query.filter(NewsItem.is_new == True).count()

            # Get items by source for this day
            source_breakdown = {}
            source_day_counts = db.query(
                NewsItem.source,
                func.count(NewsItem.id)
            ).filter(
                NewsItem.created_at >= day_start,
                NewsItem.created_at <= day_end
            )

            if sources:
                source_day_counts = source_day_counts.filter(
                    NewsItem.source.in_(sources)
                )

            source_day_counts = source_day_counts.group_by(NewsItem.source).all()

            for source_code, count in source_day_counts:
                source_breakdown[source_code] = count

            breakdown.append({
                "date": current.strftime("%Y-%m-%d"),
                "runs": runs_count,
                "total_items": items_count,
                "new_items": new_items_count,
                "by_source": source_breakdown
            })

            current += timedelta(days=1)

        return breakdown

    def get_source_statistics(
        self,
        source_code: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get statistics for one or all sources.

        Args:
            source_code: Optional specific source to get stats for
            days: Number of days to look back

        Returns:
            Dictionary of source statistics
        """
        start_date = datetime.utcnow() - timedelta(days=days)

        with get_db() as db:
            # Get sources
            sources_query = db.query(Source)
            if source_code:
                sources_query = sources_query.filter(Source.code == source_code)
            sources = sources_query.all()

            result = {}

            for source in sources:
                # Total items for this source
                total_items = db.query(NewsItem).filter(
                    NewsItem.source == source.code
                ).count()

                # Items in period
                recent_items = db.query(NewsItem).filter(
                    NewsItem.source == source.code,
                    NewsItem.created_at >= start_date
                ).count()

                # Average items per day
                avg_per_day = recent_items / days if days > 0 else 0

                result[source.code] = {
                    "name": source.name,
                    "enabled": source.enabled,
                    "total_items": total_items,
                    "items_last_n_days": recent_items,
                    "avg_per_day": round(avg_per_day, 2),
                    "last_scraped_at": source.last_scraped_at.isoformat() if source.last_scraped_at else None
                }

            return result

    def get_run_statistics(self, days: int = 30) -> Dict[str, Any]:
        """
        Get overall run statistics.

        Args:
            days: Number of days to look back

        Returns:
            Dictionary of run statistics
        """
        start_date = datetime.utcnow() - timedelta(days=days)

        with get_db() as db:
            # Total runs in period
            total_runs = db.query(Run).filter(
                Run.started_at >= start_date
            ).count()

            # Runs by status
            runs_by_status = {}
            status_counts = db.query(
                Run.status,
                func.count(Run.id)
            ).filter(
                Run.started_at >= start_date
            ).group_by(Run.status).all()

            for status, count in status_counts:
                runs_by_status[status] = count

            # Success rate
            successful = runs_by_status.get("success", 0)
            success_rate = (successful / total_runs * 100) if total_runs > 0 else 0

            # Average duration
            avg_duration = db.query(
                func.avg(Run.duration_seconds)
            ).filter(
                Run.started_at >= start_date,
                Run.duration_seconds.isnot(None)
            ).scalar()

            # Average items per run
            avg_items = db.query(
                func.avg(Run.items_found)
            ).filter(
                Run.started_at >= start_date
            ).scalar()

            # Average new items per run
            avg_new_items = db.query(
                func.avg(Run.new_items)
            ).filter(
                Run.started_at >= start_date
            ).scalar()

            # Runs by trigger type
            runs_by_trigger = {}
            trigger_counts = db.query(
                Run.triggered_by,
                func.count(Run.id)
            ).filter(
                Run.started_at >= start_date
            ).group_by(Run.triggered_by).all()

            for trigger, count in trigger_counts:
                runs_by_trigger[trigger] = count

            return {
                "period_days": days,
                "total_runs": total_runs,
                "runs_by_status": runs_by_status,
                "success_rate": round(success_rate, 2),
                "avg_duration_seconds": round(avg_duration, 2) if avg_duration else None,
                "avg_items_per_run": round(avg_items, 2) if avg_items else None,
                "avg_new_items_per_run": round(avg_new_items, 2) if avg_new_items else None,
                "runs_by_trigger": runs_by_trigger
            }


    def generate_pdf_report(
        self,
        period: ReportPeriod = ReportPeriod.QUARTERLY,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sources: Optional[List[str]] = None
    ) -> bytes:
        """
        Generate a PDF report for the specified period.

        Args:
            period: Report period type (weekly, monthly, quarterly, annual, custom)
            start_date: Custom start date (required for custom period)
            end_date: Custom end date (required for custom period)
            sources: Optional list of source codes to filter by

        Returns:
            PDF file as bytes
        """
        from weasyprint import HTML, CSS

        # Get the summary data first
        summary = self.generate_summary(period, start_date, end_date, sources)

        # Calculate date range
        start, end = self._calculate_date_range(period, start_date, end_date)

        # Get news items for the period
        news_items = self._get_news_items_for_period(start, end, sources)

        # Generate weekly breakdown
        weekly_breakdown = self._generate_weekly_breakdown(start, end, sources)

        # Determine report title based on period
        period_titles = {
            ReportPeriod.WEEKLY: "Weekly Report",
            ReportPeriod.MONTHLY: "Monthly Report",
            ReportPeriod.QUARTERLY: "Quarterly Report",
            ReportPeriod.ANNUAL: "Annual Report",
            ReportPeriod.CUSTOM: "Custom Period Report"
        }
        report_title = period_titles.get(period, "Report")

        # Set up Jinja2 environment
        template_dir = Path(settings.templates_dir) / "reports"
        env = Environment(loader=FileSystemLoader(str(template_dir)))
        template = env.get_template("quarterly_report.html")

        # Render HTML with data
        html_content = template.render(
            report_title=report_title,
            start_date=start,
            end_date=end,
            generated_at=datetime.utcnow(),
            total_items=summary.total_items,
            new_items=summary.new_items,
            total_runs=summary.total_runs,
            successful_runs=summary.successful_runs,
            items_by_source=summary.items_by_source,
            runs_by_status=summary.runs_by_status,
            weekly_breakdown=weekly_breakdown,
            news_items=news_items,
            sources_filter=sources
        )

        # Convert HTML to PDF
        html = HTML(string=html_content, base_url=str(template_dir))
        pdf_bytes = html.write_pdf()

        logger.info(
            f"Generated PDF report for {period.value} period: "
            f"{len(pdf_bytes)} bytes, {len(news_items)} items"
        )

        return pdf_bytes

    def _get_news_items_for_period(
        self,
        start_date: datetime,
        end_date: datetime,
        sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all news items for a specific period.

        Args:
            start_date: Start of period
            end_date: End of period
            sources: Optional source filter

        Returns:
            List of news item dictionaries
        """
        with get_db() as db:
            query = db.query(NewsItem).filter(
                NewsItem.created_at >= start_date,
                NewsItem.created_at <= end_date
            )

            if sources:
                query = query.filter(NewsItem.source.in_(sources))

            items = query.order_by(desc(NewsItem.created_at)).all()

            return [
                {
                    "id": item.id,
                    "source": item.source,
                    "title": item.title,
                    "url": item.url,
                    "published_date": item.published_date,
                    "created_at": item.created_at,
                    "is_new": item.is_new
                }
                for item in items
            ]

    def _generate_weekly_breakdown(
        self,
        start_date: datetime,
        end_date: datetime,
        sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate a week-by-week breakdown of items and runs.

        Args:
            start_date: Start of period
            end_date: End of period
            sources: Optional source filter

        Returns:
            List of weekly statistics dictionaries
        """
        breakdown = []
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # Align to start of week (Monday)
        days_since_monday = current.weekday()
        current = current - timedelta(days=days_since_monday)

        week_num = 1

        with get_db() as db:
            while current <= end_date:
                week_start = current
                week_end = current + timedelta(days=6, hours=23, minutes=59, seconds=59)

                # Ensure we don't go beyond the report period
                effective_start = max(week_start, start_date)
                effective_end = min(week_end, end_date)

                # Count runs for this week
                runs_count = db.query(Run).filter(
                    Run.started_at >= effective_start,
                    Run.started_at <= effective_end
                ).count()

                # Count items for this week
                items_query = db.query(NewsItem).filter(
                    NewsItem.created_at >= effective_start,
                    NewsItem.created_at <= effective_end
                )

                if sources:
                    items_query = items_query.filter(NewsItem.source.in_(sources))

                total_items = items_query.count()
                new_items = items_query.filter(NewsItem.is_new == True).count()

                breakdown.append({
                    "week_num": week_num,
                    "label": f"Week {week_num} ({week_start.strftime('%b %d')} - {week_end.strftime('%b %d')})",
                    "start_date": week_start,
                    "end_date": week_end,
                    "total_items": total_items,
                    "new_items": new_items,
                    "runs": runs_count
                })

                current += timedelta(days=7)
                week_num += 1

        return breakdown


# Singleton instance
report_service = ReportService()
