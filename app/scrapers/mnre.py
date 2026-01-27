"""
MNRE (Ministry of New and Renewable Energy) scraper.

Scrapes tenders from https://www.mnre.gov.in/tenders/recent
"""

import logging
from typing import List, Tuple, Optional

from .base import BaseScraper, ScraperError

logger = logging.getLogger(__name__)


class MNREScraper(BaseScraper):
    """
    Scraper for MNRE (Ministry of New and Renewable Energy) tenders.

    Targets the tenders page at https://www.mnre.gov.in/tenders/recent
    which uses an accordion-style layout to display tender information.
    """

    DEFAULT_URL = "https://www.mnre.gov.in/tenders/recent"

    def __init__(self, url: str = None, **kwargs):
        """
        Initialize the MNRE scraper.

        Args:
            url: Override URL (defaults to MNRE tenders page)
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(
            name="MNRE",
            base_url=url or self.DEFAULT_URL,
            **kwargs
        )

    def scrape(self) -> List[Tuple[str, str, Optional[str]]]:
        """
        Scrape MNRE tenders page.

        Finds the accordion container with class 'accordion' and id 'accordionExample',
        then extracts dates from `p.date` elements and titles from
        `button.btn.btn-link.english_title` elements.

        Returns:
            List of tuples containing (title, url, date)

        Raises:
            ScraperError: If scraping fails
        """
        results = []

        try:
            logger.info(f"[{self.name}] Starting scrape of {self.base_url}")
            soup = self.fetch_page(self.base_url)

            # Find the accordion container
            accordion = soup.find("div", {"id": "accordionExample", "class": "accordion"})

            if not accordion:
                # Try alternative: just find by id
                accordion = soup.find("div", {"id": "accordionExample"})

            if not accordion:
                # Try alternative: just find accordion class
                accordion = soup.find("div", {"class": "accordion"})

            if not accordion:
                logger.warning(f"[{self.name}] Could not find accordion container")
                # Fall back to searching the entire page
                accordion = soup

            # Find all accordion items (usually in card or accordion-item divs)
            items = accordion.find_all("div", class_=lambda x: x and (
                "card" in x or "accordion-item" in x
            ))

            if not items:
                # Try finding items by looking for the title buttons directly
                items = accordion.find_all("div", class_="card-header")
                if not items:
                    items = [accordion]  # Process entire container as one item

            logger.debug(f"[{self.name}] Found {len(items)} potential tender items")

            for item in items:
                try:
                    result = self._extract_item(item)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.debug(f"[{self.name}] Error extracting item: {e}")
                    continue

            # If no items found through accordion structure, try direct button search
            if not results:
                results = self._fallback_extraction(soup)

            logger.info(f"[{self.name}] Scraped {len(results)} items")
            return results

        except ScraperError:
            raise
        except Exception as e:
            logger.error(f"[{self.name}] Scraping failed: {e}")
            raise ScraperError(
                f"Failed to scrape MNRE: {e}",
                url=self.base_url,
                cause=e
            )

    def _extract_item(self, item) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Extract title, URL, and date from an accordion item.

        Args:
            item: BeautifulSoup element containing the tender item

        Returns:
            Tuple of (title, url, date) or None if extraction fails
        """
        # Extract title from button.btn.btn-link.english_title
        title_elem = item.find("button", class_=lambda x: x and "english_title" in x)

        if not title_elem:
            # Try alternative selectors
            title_elem = item.find("button", class_="btn-link")

        if not title_elem:
            # Try finding any button in the header
            title_elem = item.find("button")

        if not title_elem:
            # Try finding a link or heading
            title_elem = item.find(["a", "h4", "h5", "h6"])

        if not title_elem:
            return None

        title = self.clean_text(title_elem.get_text())
        if not title:
            return None

        # Extract URL - look for link in the item
        url_elem = item.find("a", href=True)
        if url_elem:
            url = self.normalize_url(url_elem.get("href", ""))
        else:
            # Use the page URL as fallback
            url = self.base_url

        # Extract date from p.date
        date_elem = item.find("p", class_="date")

        if not date_elem:
            # Try alternative selectors for date
            date_elem = item.find("span", class_="date")

        if not date_elem:
            # Look for any element with date-like content
            date_elem = item.find(["p", "span", "small"], class_=lambda x: x and "date" in str(x).lower())

        date = None
        if date_elem:
            date = self.parse_date(date_elem.get_text())

        return (title, url, date)

    def _fallback_extraction(self, soup) -> List[Tuple[str, str, Optional[str]]]:
        """
        Fallback extraction method when accordion structure isn't found.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            List of extracted items
        """
        results = []

        # Find all buttons with english_title class
        title_buttons = soup.find_all("button", class_=lambda x: x and "english_title" in str(x))

        for button in title_buttons:
            title = self.clean_text(button.get_text())
            if not title:
                continue

            # Find nearest date element
            parent = button.find_parent()
            date = None
            while parent:
                date_elem = parent.find("p", class_="date")
                if date_elem:
                    date = self.parse_date(date_elem.get_text())
                    break
                parent = parent.find_parent()

            # Find nearest link
            url = self.base_url
            parent = button.find_parent()
            while parent:
                link_elem = parent.find("a", href=True)
                if link_elem:
                    url = self.normalize_url(link_elem.get("href"))
                    break
                parent = parent.find_parent()

            results.append((title, url, date))

        logger.debug(f"[{self.name}] Fallback extraction found {len(results)} items")
        return results
