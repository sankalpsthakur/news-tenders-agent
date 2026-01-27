"""
Generic configurable scraper.

A flexible scraper that can be configured with CSS selectors
to scrape various websites without writing custom code.
"""

import logging
from typing import List, Tuple, Optional, Dict, Any

from .base import BaseScraper, ScraperError

logger = logging.getLogger(__name__)


class GenericScraper(BaseScraper):
    """
    A configurable generic scraper that uses CSS selectors.

    This scraper can be configured to scrape any website by providing
    appropriate CSS selectors for the container, items, title, URL, and date.

    Selector Configuration:
        - container_selector: CSS selector for the main container (optional)
        - item_selector: CSS selector for individual items within the container
        - title_selector: CSS selector for the title within each item
        - url_selector: CSS selector for the URL within each item (defaults to 'a')
        - date_selector: CSS selector for the date within each item (optional)

    Example selectors config:
        {
            "container_selector": "div.news-list",
            "item_selector": "div.news-item",
            "title_selector": "h3.title",
            "url_selector": "a.read-more",
            "date_selector": "span.date"
        }
    """

    def __init__(
        self,
        url: str,
        name: str = "Generic",
        selectors: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        """
        Initialize the generic scraper.

        Args:
            url: The URL to scrape
            name: Human-readable name for this scraper instance
            selectors: Dictionary of CSS selectors for scraping
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(
            name=name,
            base_url=url,
            **kwargs
        )

        self.selectors = selectors or {}
        self._validate_selectors()

    def _validate_selectors(self) -> None:
        """Validate that required selectors are provided."""
        if not self.selectors:
            logger.warning(f"[{self.name}] No selectors provided, will use defaults")
            return

        # item_selector is the minimum required
        if "item_selector" not in self.selectors:
            logger.warning(f"[{self.name}] No item_selector provided, will try to auto-detect")

    def scrape(self) -> List[Tuple[str, str, Optional[str]]]:
        """
        Scrape the configured URL using the provided selectors.

        Returns:
            List of tuples containing (title, url, date)

        Raises:
            ScraperError: If scraping fails
        """
        results = []

        try:
            logger.info(f"[{self.name}] Starting scrape of {self.base_url}")
            soup = self.fetch_page(self.base_url)

            # Get the container (or use the whole page)
            container = self._find_container(soup)

            # Find items
            items = self._find_items(container)

            if not items:
                logger.warning(f"[{self.name}] No items found")
                return results

            logger.debug(f"[{self.name}] Found {len(items)} items")

            for item in items:
                try:
                    result = self._extract_item(item)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.debug(f"[{self.name}] Error extracting item: {e}")
                    continue

            logger.info(f"[{self.name}] Scraped {len(results)} items")
            return results

        except ScraperError:
            raise
        except Exception as e:
            logger.error(f"[{self.name}] Scraping failed: {e}")
            raise ScraperError(
                f"Failed to scrape {self.name}: {e}",
                url=self.base_url,
                cause=e
            )

    def _find_container(self, soup):
        """
        Find the container element.

        Args:
            soup: BeautifulSoup object

        Returns:
            The container element or the soup itself
        """
        container_selector = self.selectors.get("container_selector")

        if container_selector:
            container = soup.select_one(container_selector)
            if container:
                logger.debug(f"[{self.name}] Found container with selector: {container_selector}")
                return container
            else:
                logger.warning(f"[{self.name}] Container not found with selector: {container_selector}")

        return soup

    def _find_items(self, container) -> list:
        """
        Find item elements within the container.

        Args:
            container: BeautifulSoup element to search within

        Returns:
            List of item elements
        """
        item_selector = self.selectors.get("item_selector")

        if item_selector:
            items = container.select(item_selector)
            if items:
                return items
            logger.warning(f"[{self.name}] No items found with selector: {item_selector}")

        # Try common patterns
        fallback_selectors = [
            "article",
            "div.item",
            "div.news-item",
            "li.item",
            "tr",
            "div.card",
            "div.list-item",
        ]

        for selector in fallback_selectors:
            items = container.select(selector)
            if items and len(items) > 1:  # Need at least 2 items to be considered a list
                logger.debug(f"[{self.name}] Using fallback selector: {selector}")
                return items

        return []

    def _extract_item(self, item) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Extract title, URL, and date from an item element.

        Args:
            item: BeautifulSoup element containing the item

        Returns:
            Tuple of (title, url, date) or None if extraction fails
        """
        title = self._extract_title(item)
        if not title:
            return None

        url = self._extract_url(item)
        date = self._extract_date(item)

        return (title, url, date)

    def _extract_title(self, item) -> Optional[str]:
        """
        Extract the title from an item.

        Args:
            item: BeautifulSoup element

        Returns:
            The extracted title or None
        """
        title_selector = self.selectors.get("title_selector")

        if title_selector:
            elem = item.select_one(title_selector)
            if elem:
                return self.clean_text(elem.get_text())

        # Fallback: try common title elements
        fallback_selectors = [
            "h1", "h2", "h3", "h4", "h5", "h6",
            ".title", ".heading",
            "a",  # Often the link text is the title
        ]

        for selector in fallback_selectors:
            elem = item.select_one(selector)
            if elem:
                text = self.clean_text(elem.get_text())
                if text and len(text) > 5:  # Avoid tiny fragments
                    return text

        # Last resort: get any text content
        text = self.clean_text(item.get_text())
        if text and len(text) > 5:
            # Truncate if too long
            return text[:200] + "..." if len(text) > 200 else text

        return None

    def _extract_url(self, item) -> str:
        """
        Extract the URL from an item.

        Args:
            item: BeautifulSoup element

        Returns:
            The extracted URL or the base URL
        """
        url_selector = self.selectors.get("url_selector")

        if url_selector:
            elem = item.select_one(url_selector)
            if elem:
                href = elem.get("href")
                if href:
                    return self.normalize_url(href)

        # Fallback: find any anchor
        anchor = item.find("a", href=True)
        if anchor:
            href = anchor.get("href")
            if href:
                return self.normalize_url(href)

        return self.base_url

    def _extract_date(self, item) -> Optional[str]:
        """
        Extract the date from an item.

        Args:
            item: BeautifulSoup element

        Returns:
            The parsed date or None
        """
        date_selector = self.selectors.get("date_selector")

        if date_selector:
            elem = item.select_one(date_selector)
            if elem:
                date_text = elem.get("datetime") or elem.get_text()
                if date_text:
                    return self.parse_date(date_text)

        # Fallback: try common date elements
        fallback_selectors = [
            "time",
            ".date",
            ".published",
            ".timestamp",
            "span.date",
            "p.date",
        ]

        for selector in fallback_selectors:
            elem = item.select_one(selector)
            if elem:
                date_text = elem.get("datetime") or elem.get_text()
                if date_text:
                    parsed = self.parse_date(date_text)
                    if parsed:
                        return parsed

        return None

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "GenericScraper":
        """
        Create a GenericScraper from a configuration dictionary.

        Args:
            config: Configuration dictionary with keys:
                - url (required): The URL to scrape
                - name (optional): Name for the scraper
                - selectors (optional): Selector configuration
                - timeout (optional): Request timeout
                - max_retries (optional): Max retry attempts
                - rate_limit_delay (optional): Delay between requests

        Returns:
            A configured GenericScraper instance

        Raises:
            ValueError: If required configuration is missing
        """
        if "url" not in config:
            raise ValueError("Configuration must include 'url'")

        return cls(
            url=config["url"],
            name=config.get("name", "Generic"),
            selectors=config.get("selectors"),
            timeout=config.get("timeout"),
            max_retries=config.get("max_retries"),
            rate_limit_delay=config.get("rate_limit_delay"),
        )
