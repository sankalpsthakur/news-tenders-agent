"""
Scheduler service for Hygenco News & Tenders Monitor.

Manages scheduled scraping runs using APScheduler with SQLite job store
for persistence across restarts.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, JobExecutionEvent
import pytz

from app.database import get_db, Setting
from app.config import settings
from app.models import TriggerType

logger = logging.getLogger(__name__)

# Job ID for the main scraping job
MAIN_JOB_ID = "hygenco_daily_scrape"


class SchedulerService:
    """
    Manages scheduled scraping runs using APScheduler.

    Uses SQLite job store for persistence, supports dynamic schedule
    updates, and provides control over scheduler state.
    """

    def __init__(self):
        """Initialize the scheduler service."""
        self._scheduler: Optional[BackgroundScheduler] = None
        self._is_initialized = False
        self._timezone = pytz.timezone(settings.timezone)

    @property
    def scheduler(self) -> BackgroundScheduler:
        """Get the scheduler instance, initializing if needed."""
        if self._scheduler is None:
            self._initialize_scheduler()
        return self._scheduler

    def _initialize_scheduler(self):
        """Initialize the APScheduler with SQLite job store."""
        # Ensure data directory exists
        data_dir = settings.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)

        # Configure job store
        jobstore_path = data_dir / "scheduler.db"
        jobstores = {
            "default": SQLAlchemyJobStore(url=f"sqlite:///{jobstore_path}")
        }

        # Configure executors
        executors = {
            "default": {"type": "threadpool", "max_workers": 3}
        }

        # Configure job defaults
        job_defaults = {
            "coalesce": True,  # Combine missed runs into one
            "max_instances": 1,  # Only one instance of the job at a time
            "misfire_grace_time": 3600  # Allow 1 hour grace for misfires
        }

        self._scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=self._timezone
        )

        # Add event listeners
        self._scheduler.add_listener(
            self._job_executed_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        )

        logger.info("Scheduler initialized with SQLite job store")

    def _job_executed_listener(self, event: JobExecutionEvent):
        """
        Handle job execution events.

        Args:
            event: APScheduler job execution event
        """
        if event.exception:
            logger.error(
                f"Job {event.job_id} failed with exception: {event.exception}",
                exc_info=event.exception
            )
        else:
            logger.info(f"Job {event.job_id} executed successfully")

    def _get_schedule_time(self) -> str:
        """
        Get the scheduled time from database settings.

        Returns:
            Time string in HH:MM format
        """
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_time").first()
            if setting and setting.value:
                return setting.value
        return settings.default_schedule

    def _get_schedule_frequency(self) -> str:
        """
        Get the schedule frequency from database settings.

        Returns:
            Frequency string ('daily' or 'weekly')
        """
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_frequency").first()
            if setting and setting.value:
                return setting.value
        return "daily"

    def _get_schedule_day_of_week(self) -> int:
        """
        Get the day of week for weekly schedule from database settings.

        Returns:
            Day of week (0=Monday, 6=Sunday)
        """
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_day_of_week").first()
            if setting and setting.value:
                try:
                    return int(setting.value)
                except ValueError:
                    return 0
        return 0

    def _create_trigger(self, hour: int, minute: int, frequency: str = "daily", day_of_week: int = 0) -> CronTrigger:
        """
        Create a CronTrigger based on schedule settings.

        Args:
            hour: Hour of the day (0-23)
            minute: Minute of the hour (0-59)
            frequency: 'daily' or 'weekly'
            day_of_week: Day of week for weekly schedule (0=Monday, 6=Sunday)

        Returns:
            CronTrigger configured for the schedule
        """
        if frequency == "weekly":
            # APScheduler uses 0-6 for mon-sun, same as our convention
            return CronTrigger(
                day_of_week=day_of_week,
                hour=hour,
                minute=minute,
                timezone=self._timezone
            )
        else:
            # Daily schedule
            return CronTrigger(
                hour=hour,
                minute=minute,
                timezone=self._timezone
            )

    def _is_schedule_enabled(self) -> bool:
        """
        Check if scheduled runs are enabled.

        Returns:
            True if scheduling is enabled
        """
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_enabled").first()
            if setting:
                return setting.value.lower() == "true"
        return True

    def _execute_scheduled_run(self):
        """
        Execute a scheduled scraping run.

        This is the function called by APScheduler.
        """
        logger.info("Executing scheduled scraping run")

        # Import here to avoid circular imports
        from app.services.runner import runner_service
        from app.services.notifier import notifier_service

        try:
            # Execute the run
            result = runner_service.execute_run(triggered_by=TriggerType.SCHEDULE)

            # Send notification if there are new items or if run failed
            if result.new_items > 0 or result.status.value in ["failed", "partial"]:
                notifier_service.send_teams_notification(result.id)

            logger.info(
                f"Scheduled run completed: {result.status.value}, "
                f"{result.new_items} new items"
            )

        except Exception as e:
            logger.error(f"Scheduled run failed: {str(e)}", exc_info=True)

    def start(self):
        """
        Start the scheduler and initialize jobs from database settings.

        Adds or updates the scraping job based on current settings (daily or weekly).
        """
        if self._is_initialized and self.scheduler.running:
            logger.warning("Scheduler is already running")
            return

        # Get schedule settings
        schedule_time = self._get_schedule_time()
        schedule_enabled = self._is_schedule_enabled()
        schedule_frequency = self._get_schedule_frequency()
        schedule_day_of_week = self._get_schedule_day_of_week()

        # Parse time
        try:
            hour, minute = map(int, schedule_time.split(":"))
        except ValueError:
            logger.error(f"Invalid schedule time format: {schedule_time}")
            hour, minute = 6, 0  # Default to 6:00 AM

        # Remove existing job if present
        try:
            self.scheduler.remove_job(MAIN_JOB_ID)
        except Exception:
            pass  # Job doesn't exist

        # Add the job if scheduling is enabled
        if schedule_enabled:
            trigger = self._create_trigger(hour, minute, schedule_frequency, schedule_day_of_week)

            # Determine job name based on frequency
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            if schedule_frequency == "weekly":
                job_name = f"Weekly News & Tenders Scrape ({day_names[schedule_day_of_week]})"
                log_message = f"Scheduled weekly run on {day_names[schedule_day_of_week]} at {schedule_time} ({settings.timezone})"
            else:
                job_name = "Daily News & Tenders Scrape"
                log_message = f"Scheduled daily run at {schedule_time} ({settings.timezone})"

            self.scheduler.add_job(
                self._execute_scheduled_run,
                trigger=trigger,
                id=MAIN_JOB_ID,
                name=job_name,
                replace_existing=True
            )

            logger.info(log_message)

        # Start the scheduler
        if not self.scheduler.running:
            self.scheduler.start()
            self._is_initialized = True
            logger.info("Scheduler started")

    def stop(self):
        """Stop the scheduler."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped")

    def update_schedule(self, time_str: str) -> bool:
        """
        Update the run time.

        Args:
            time_str: New time in HH:MM format

        Returns:
            True if update successful
        """
        # Validate time format
        try:
            hour, minute = map(int, time_str.split(":"))
            if not (0 <= hour < 24 and 0 <= minute < 60):
                raise ValueError("Invalid time values")
        except ValueError as e:
            logger.error(f"Invalid time format '{time_str}': {e}")
            return False

        # Update database setting
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_time").first()
            if setting:
                setting.value = time_str
                setting.updated_at = datetime.utcnow()
            else:
                setting = Setting(
                    key="schedule_time",
                    value=time_str,
                    description="Scheduled run time (HH:MM)"
                )
                db.add(setting)
            db.commit()

        # Reschedule the job with current frequency settings
        self._reschedule_job()
        logger.info(f"Schedule time updated to {time_str}")

        return True

    def update_frequency(self, frequency: str) -> bool:
        """
        Update the schedule frequency (daily or weekly).

        Args:
            frequency: 'daily' or 'weekly'

        Returns:
            True if update successful
        """
        if frequency not in ("daily", "weekly"):
            logger.error(f"Invalid frequency '{frequency}': must be 'daily' or 'weekly'")
            return False

        # Update database setting
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_frequency").first()
            if setting:
                setting.value = frequency
                setting.updated_at = datetime.utcnow()
            else:
                setting = Setting(
                    key="schedule_frequency",
                    value=frequency,
                    description="Schedule frequency (daily or weekly)"
                )
                db.add(setting)
            db.commit()

        # Reschedule the job
        self._reschedule_job()
        logger.info(f"Schedule frequency updated to {frequency}")

        return True

    def update_day_of_week(self, day_of_week: int) -> bool:
        """
        Update the day of week for weekly schedule.

        Args:
            day_of_week: Day of week (0=Monday, 6=Sunday)

        Returns:
            True if update successful
        """
        if not (0 <= day_of_week <= 6):
            logger.error(f"Invalid day_of_week '{day_of_week}': must be 0-6")
            return False

        # Update database setting
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_day_of_week").first()
            if setting:
                setting.value = str(day_of_week)
                setting.updated_at = datetime.utcnow()
            else:
                setting = Setting(
                    key="schedule_day_of_week",
                    value=str(day_of_week),
                    description="Day of week for weekly schedule (0=Monday, 6=Sunday)"
                )
                db.add(setting)
            db.commit()

        # Reschedule the job (only matters if frequency is weekly)
        self._reschedule_job()
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        logger.info(f"Schedule day of week updated to {day_names[day_of_week]}")

        return True

    def _reschedule_job(self):
        """
        Reschedule the main job with current settings from database.
        """
        if not self._scheduler or not self._scheduler.running:
            return

        # Get current settings
        schedule_time = self._get_schedule_time()
        schedule_frequency = self._get_schedule_frequency()
        schedule_day_of_week = self._get_schedule_day_of_week()

        try:
            hour, minute = map(int, schedule_time.split(":"))
        except ValueError:
            hour, minute = 6, 0

        try:
            trigger = self._create_trigger(hour, minute, schedule_frequency, schedule_day_of_week)
            self._scheduler.reschedule_job(MAIN_JOB_ID, trigger=trigger)
        except Exception as e:
            logger.error(f"Failed to reschedule job: {e}")
            # Try to restart with new settings
            self.start()

    def get_next_run(self) -> Optional[datetime]:
        """
        Get the next scheduled run datetime.

        Returns:
            Next run datetime or None if not scheduled
        """
        if not self._scheduler or not self._scheduler.running:
            return None

        try:
            job = self._scheduler.get_job(MAIN_JOB_ID)
            if job:
                return job.next_run_time
        except Exception as e:
            logger.error(f"Failed to get next run time: {e}")

        return None

    def pause(self) -> bool:
        """
        Pause the scheduler (disable scheduled runs).

        Returns:
            True if successful
        """
        # Update database setting
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_enabled").first()
            if setting:
                setting.value = "false"
                setting.updated_at = datetime.utcnow()
            else:
                setting = Setting(
                    key="schedule_enabled",
                    value="false",
                    description="Enable scheduled runs"
                )
                db.add(setting)
            db.commit()

        # Pause the job
        if self._scheduler and self._scheduler.running:
            try:
                self._scheduler.pause_job(MAIN_JOB_ID)
                logger.info("Scheduler paused")
                return True
            except Exception as e:
                logger.error(f"Failed to pause scheduler: {e}")
                return False

        return True

    def resume(self) -> bool:
        """
        Resume the scheduler (enable scheduled runs).

        Returns:
            True if successful
        """
        # Update database setting
        with get_db() as db:
            setting = db.query(Setting).filter(Setting.key == "schedule_enabled").first()
            if setting:
                setting.value = "true"
                setting.updated_at = datetime.utcnow()
            else:
                setting = Setting(
                    key="schedule_enabled",
                    value="true",
                    description="Enable scheduled runs"
                )
                db.add(setting)
            db.commit()

        # Resume the job
        if self._scheduler and self._scheduler.running:
            try:
                self._scheduler.resume_job(MAIN_JOB_ID)
                logger.info("Scheduler resumed")
                return True
            except Exception as e:
                logger.error(f"Failed to resume scheduler: {e}")
                # Try to restart
                self.start()
                return True
        else:
            # Start the scheduler if not running
            self.start()
            return True

    def is_running(self) -> bool:
        """
        Check if the scheduler is running.

        Returns:
            True if scheduler is active
        """
        return self._scheduler is not None and self._scheduler.running

    def get_status(self) -> dict:
        """
        Get scheduler status information.

        Returns:
            Dictionary with scheduler status details
        """
        next_run = self.get_next_run()
        frequency = self._get_schedule_frequency()
        day_of_week = self._get_schedule_day_of_week()
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        return {
            "running": self.is_running(),
            "enabled": self._is_schedule_enabled(),
            "schedule_time": self._get_schedule_time(),
            "schedule_frequency": frequency,
            "schedule_day_of_week": day_of_week,
            "schedule_day_name": day_names[day_of_week],
            "timezone": settings.timezone,
            "next_run": next_run.isoformat() if next_run else None
        }


# Singleton instance
scheduler_service = SchedulerService()
