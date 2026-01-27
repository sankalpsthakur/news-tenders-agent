"""
Scrapers module for Hygenco News & Tenders Monitor.

This module provides web scrapers for various government and industry sources
to collect news and tender information related to renewable energy.
"""

from .base import BaseScraper
from .mnre import MNREScraper
from .seci import SECIScraper
from .generic import GenericScraper

__all__ = [
    "BaseScraper",
    "MNREScraper",
    "SECIScraper",
    "GenericScraper",
]

# Scraper registry for dynamic lookup
SCRAPER_REGISTRY = {
    "mnre": MNREScraper,
    "seci": SECIScraper,
    "generic": GenericScraper,
}


def get_scraper(scraper_type: str, **kwargs):
    """
    Factory function to get a scraper instance by type.

    Args:
        scraper_type: The type of scraper ('mnre', 'seci', 'generic')
        **kwargs: Additional arguments to pass to the scraper constructor

    Returns:
        An instance of the requested scraper

    Raises:
        ValueError: If the scraper type is not recognized
    """
    scraper_class = SCRAPER_REGISTRY.get(scraper_type.lower())
    if scraper_class is None:
        raise ValueError(
            f"Unknown scraper type: {scraper_type}. "
            f"Available types: {list(SCRAPER_REGISTRY.keys())}"
        )
    return scraper_class(**kwargs)
