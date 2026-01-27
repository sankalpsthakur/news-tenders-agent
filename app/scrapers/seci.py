"""
SECI (Solar Energy Corporation of India) scraper.

Scrapes news/tenders from https://seci.co.in/whats-new
"""

import logging
from typing import List, Tuple, Optional

from .base import BaseScraper, ScraperError

logger = logging.getLogger(__name__)


class SECIScraper(BaseScraper):
    """
    Scraper for SECI (Solar Energy Corporation of India) news and tenders.

    Targets the "What's New" page at https://seci.co.in/whats-new
    which displays items in a grid layout with td.td_grid elements.
    """

    DEFAULT_URL = "https://seci.co.in/whats-new"

    def __init__(self, url: str = None, **kwargs):
        """
        Initialize the SECI scraper.

        Args:
            url: Override URL (defaults to SECI what's new page)
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(
            name="SECI",
            base_url=url or self.DEFAULT_URL,
            **kwargs
        )

    def scrape(self) -> List[Tuple[str, str, Optional[str]]]:
        """
        Scrape SECI what's new page.

        Finds all `td.td_grid` elements, extracts anchor hrefs,
        then fetches each link to get the heading from `h3.inner-page-heading`.

        Returns:
            List of tuples containing (title, url, date)

        Raises:
            ScraperError: If scraping fails
        """
        results = []

        try:
            logger.info(f"[{self.name}] Starting scrape of {self.base_url}")
            soup = self.fetch_page(self.base_url)

            # Find all td.td_grid elements
            grid_cells = soup.find_all("td", class_="td_grid")

            if not grid_cells:
                # Try alternative: find table cells in the grid
                grid_cells = soup.find_all("td", class_=lambda x: x and "grid" in str(x).lower())

            if not grid_cells:
                # Try finding the table directly
                table = soup.find("table", class_=lambda x: x and "grid" in str(x).lower())
                if table:
                    grid_cells = table.find_all("td")

            logger.debug(f"[{self.name}] Found {len(grid_cells)} grid cells")

            # Extract links from grid cells
            links = []
            for cell in grid_cells:
                anchor = cell.find("a", href=True)
                if anchor:
                    href = anchor.get("href", "")
                    if href:
                        full_url = self.normalize_url(href)
                        # Get link text as preliminary title
                        link_text = self.clean_text(anchor.get_text())
                        links.append((full_url, link_text))

            if not links:
                # Fallback: find any links on the page in common containers
                links = self._fallback_link_extraction(soup)

            logger.info(f"[{self.name}] Found {len(links)} links to process")

            # Fetch each link to get the full heading
            for url, preliminary_title in links:
                try:
                    result = self._fetch_item_details(url, preliminary_title)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(f"[{self.name}] Error fetching {url}: {e}")
                    # Use preliminary data if available
                    if preliminary_title:
                        results.append((preliminary_title, url, None))

            logger.info(f"[{self.name}] Scraped {len(results)} items")
            return results

        except ScraperError:
            raise
        except Exception as e:
            logger.error(f"[{self.name}] Scraping failed: {e}")
            raise ScraperError(
                f"Failed to scrape SECI: {e}",
                url=self.base_url,
                cause=e
            )

    def _fetch_item_details(self, url: str, preliminary_title: str) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Fetch a linked page to get the full heading.

        Args:
            url: The URL to fetch
            preliminary_title: Title text from the link (fallback)

        Returns:
            Tuple of (title, url, date) or None if extraction fails
        """
        try:
            logger.debug(f"[{self.name}] Fetching details from {url}")
            soup = self.fetch_page(url)

            # Look for h3.inner-page-heading
            heading = soup.find("h3", class_="inner-page-heading")

            if not heading:
                # Try alternative selectors
                heading = soup.find("h3", class_=lambda x: x and "heading" in str(x).lower())

            if not heading:
                # Try any h3 in the main content area
                main_content = soup.find(["main", "article", "div"], class_=lambda x: x and (
                    "content" in str(x).lower() or "main" in str(x).lower()
                ))
                if main_content:
                    heading = main_content.find("h3")

            if not heading:
                # Try h1 or h2 as alternatives
                heading = soup.find(["h1", "h2"], class_=lambda x: x and "heading" in str(x).lower())

            if not heading:
                heading = soup.find("h1")

            title = self.clean_text(heading.get_text()) if heading else preliminary_title

            if not title:
                return None

            # Try to find a date on the detail page
            date = self._extract_date(soup)

            return (title, url, date)

        except Exception as e:
            logger.debug(f"[{self.name}] Could not fetch details from {url}: {e}")
            # Return with preliminary title if available
            if preliminary_title:
                return (preliminary_title, url, None)
            return None

    def _extract_date(self, soup) -> Optional[str]:
        """
        Try to extract a date from the page.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            Parsed date string or None
        """
        # Common date patterns
        date_selectors = [
            ("span", {"class": "date"}),
            ("p", {"class": "date"}),
            ("div", {"class": "date"}),
            ("span", {"class": lambda x: x and "date" in str(x).lower()}),
            ("time", {}),
        ]

        for tag, attrs in date_selectors:
            elem = soup.find(tag, attrs)
            if elem:
                date_text = elem.get("datetime") or elem.get_text()
                if date_text:
                    return self.parse_date(date_text)

        return None

    def _fallback_link_extraction(self, soup) -> List[Tuple[str, str]]:
        """
        Fallback method to extract links when td_grid isn't found.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            List of (url, link_text) tuples
        """
        links = []

        # Try common content containers
        containers = soup.find_all(["div", "ul", "table"], class_=lambda x: x and any(
            term in str(x).lower() for term in ["news", "tender", "list", "content", "grid"]
        ))

        if not containers:
            # Use the main content area
            containers = [soup.find("main") or soup.find("body")]

        for container in containers:
            if not container:
                continue

            for anchor in container.find_all("a", href=True):
                href = anchor.get("href", "")
                # Filter out navigation/utility links
                if any(term in href.lower() for term in ["login", "register", "contact", "about", "#"]):
                    continue

                if href:
                    full_url = self.normalize_url(href)
                    link_text = self.clean_text(anchor.get_text())
                    if link_text and len(link_text) > 10:  # Filter out short nav links
                        links.append((full_url, link_text))

        # Remove duplicates while preserving order
        seen = set()
        unique_links = []
        for link in links:
            if link[0] not in seen:
                seen.add(link[0])
                unique_links.append(link)

        return unique_links[:20]  # Limit to first 20 items
