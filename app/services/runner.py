"""
Run orchestration service for Hygenco News & Tenders Monitor.

Handles the execution of scraping runs, managing database records,
deduplication, and tracking run statistics.
"""

import hashlib
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.database import get_db, Source, Run, NewsItem, Setting
from app.models import RunResponse, RunStatus, TriggerType
from app.scrapers import get_scraper

logger = logging.getLogger(__name__)


class RunnerService:
    """
    Orchestrates scraping runs across multiple sources.

    Manages the full lifecycle of a run: creating records, executing scrapers,
    storing results, deduplicating content, and updating run statistics.
    """

    def __init__(self):
        """Initialize the runner service."""
        self._current_run: Optional[Run] = None

    @staticmethod
    def calculate_content_hash(source: str, title: str, url: Optional[str] = None) -> str:
        """
        Calculate SHA256 hash for content deduplication.

        Args:
            source: Source code (e.g., 'mnre', 'seci')
            title: Item title
            url: Optional URL

        Returns:
            SHA256 hex digest
        """
        content = f"{source}:{title}:{url or ''}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def execute_run(
        self,
        triggered_by: TriggerType = TriggerType.MANUAL,
        source_codes: Optional[List[str]] = None
    ) -> RunResponse:
        """
        Execute a scraping run across enabled sources.

        Args:
            triggered_by: What triggered this run (schedule, manual, api)
            source_codes: Optional list of specific source codes to scrape.
                         If None, scrapes all enabled sources.

        Returns:
            RunResponse with run details and statistics
        """
        start_time = datetime.utcnow()
        items_found = 0
        new_items = 0
        scraped_sources: List[str] = []
        errors: List[str] = []
        source_results: Dict[str, Dict[str, Any]] = {}

        with get_db() as db:
            # Create run record
            run = Run(
                started_at=start_time,
                status=RunStatus.RUNNING.value,
                triggered_by=triggered_by.value
            )
            db.add(run)
            db.commit()
            db.refresh(run)
            run_id = run.id

            logger.info(f"Started run {run_id} triggered by {triggered_by.value}")

            # Get sources to scrape
            query = db.query(Source).filter(Source.enabled == True)
            if source_codes:
                query = query.filter(Source.code.in_(source_codes))
            sources = query.all()

            if not sources:
                run.status = RunStatus.FAILED.value
                run.error_message = "No enabled sources found"
                run.completed_at = datetime.utcnow()
                run.duration_seconds = (run.completed_at - start_time).total_seconds()
                db.commit()

                return self._create_run_response(run)

            # Process each source
            for source in sources:
                source_items_found = 0
                source_new_items = 0

                try:
                    logger.info(f"Scraping source: {source.code} ({source.name})")

                    # Get appropriate scraper
                    scraper = get_scraper(
                        source.scraper_type,
                        url=source.url,
                        selectors=source.get_selectors() if source.selectors else None
                    )

                    # Execute scraping
                    scraped_items = scraper.scrape()
                    source_items_found = len(scraped_items)
                    items_found += source_items_found

                    # Process and deduplicate items
                    for item in scraped_items:
                        content_hash = self.calculate_content_hash(
                            source.code,
                            item.get("title", ""),
                            item.get("url")
                        )

                        # Check for duplicate
                        existing = db.query(NewsItem).filter(
                            NewsItem.content_hash == content_hash
                        ).first()

                        if not existing:
                            news_item = NewsItem(
                                source=source.code,
                                title=item.get("title", ""),
                                url=item.get("url"),
                                published_date=item.get("published_date"),
                                content_hash=content_hash,
                                run_id=run_id,
                                is_new=True
                            )
                            db.add(news_item)
                            source_new_items += 1
                            new_items += 1

                    # Update source last_scraped_at
                    source.last_scraped_at = datetime.utcnow()
                    scraped_sources.append(source.code)

                    source_results[source.code] = {
                        "status": "success",
                        "items_found": source_items_found,
                        "new_items": source_new_items
                    }

                    logger.info(
                        f"Source {source.code}: found {source_items_found} items, "
                        f"{source_new_items} new"
                    )

                except Exception as e:
                    error_msg = f"Error scraping {source.code}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    errors.append(error_msg)
                    source_results[source.code] = {
                        "status": "failed",
                        "error": str(e)
                    }

            # Determine final status
            if not scraped_sources:
                final_status = RunStatus.FAILED.value
            elif errors:
                final_status = RunStatus.PARTIAL.value
            else:
                final_status = RunStatus.SUCCESS.value

            # Update run record
            end_time = datetime.utcnow()
            run.completed_at = end_time
            run.status = final_status
            run.set_sources_scraped(scraped_sources)
            run.items_found = items_found
            run.new_items = new_items
            run.duration_seconds = (end_time - start_time).total_seconds()

            if errors:
                run.error_message = "; ".join(errors)

            db.commit()
            db.refresh(run)

            logger.info(
                f"Run {run_id} completed with status {final_status}: "
                f"{items_found} items found, {new_items} new"
            )

            return self._create_run_response(run)

    def _create_run_response(self, run: Run) -> RunResponse:
        """
        Create a RunResponse from a Run database object.

        Args:
            run: Run database model instance

        Returns:
            RunResponse pydantic model
        """
        return RunResponse(
            id=run.id,
            started_at=run.started_at,
            completed_at=run.completed_at,
            status=RunStatus(run.status),
            sources_scraped=run.get_sources_scraped(),
            items_found=run.items_found or 0,
            new_items=run.new_items or 0,
            error_message=run.error_message,
            triggered_by=run.triggered_by,
            duration_seconds=run.duration_seconds
        )

    def get_run(self, run_id: int) -> Optional[RunResponse]:
        """
        Get a specific run by ID.

        Args:
            run_id: The run ID to retrieve

        Returns:
            RunResponse if found, None otherwise
        """
        with get_db() as db:
            run = db.query(Run).filter(Run.id == run_id).first()
            if run:
                return self._create_run_response(run)
            return None

    def get_recent_runs(self, limit: int = 10) -> List[RunResponse]:
        """
        Get recent runs ordered by start time.

        Args:
            limit: Maximum number of runs to return

        Returns:
            List of RunResponse objects
        """
        with get_db() as db:
            runs = db.query(Run).order_by(
                Run.started_at.desc()
            ).limit(limit).all()

            return [self._create_run_response(run) for run in runs]


# Singleton instance
runner_service = RunnerService()
