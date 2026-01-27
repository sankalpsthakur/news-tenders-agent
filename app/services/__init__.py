"""
Services module for Hygenco News & Tenders Monitor.

This module provides the core business logic services for:
- Running scraping operations (runner)
- Sending notifications (notifier)
- Scheduling automated runs (scheduler)
- Generating reports and statistics (reports)
"""

from .runner import RunnerService, runner_service
from .notifier import NotifierService, notifier_service
from .scheduler import SchedulerService, scheduler_service
from .reports import ReportService, report_service

__all__ = [
    # Classes
    "RunnerService",
    "NotifierService",
    "SchedulerService",
    "ReportService",
    # Singleton instances
    "runner_service",
    "notifier_service",
    "scheduler_service",
    "report_service",
]
