"""
Base scraper class with common functionality for all scrapers.

Provides:
- Retry logic with exponential backoff
- Request timeout handling
- Rate limiting between requests
- Content hashing for deduplication
- Error handling and logging
"""

import hashlib
import logging
import time
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Any
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from app.config import settings

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Custom exception for scraper errors."""

    def __init__(self, message: str, url: Optional[str] = None, cause: Optional[Exception] = None):
        self.message = message
        self.url = url
        self.cause = cause
        super().__init__(self.message)


class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.

    Attributes:
        name: Human-readable name of the scraper
        base_url: The base URL to scrape
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (uses exponential backoff)
        rate_limit_delay: Delay between consecutive requests
    """

    def __init__(
        self,
        name: str,
        base_url: str,
        timeout: int = None,
        max_retries: int = None,
        retry_delay: float = None,
        rate_limit_delay: float = None,
    ):
        """
        Initialize the base scraper.

        Args:
            name: Human-readable name of the scraper
            base_url: The base URL to scrape
            timeout: Request timeout in seconds (default from settings)
            max_retries: Maximum retry attempts (default from settings)
            retry_delay: Initial retry delay in seconds (default from settings)
            rate_limit_delay: Delay between requests in seconds (default from settings)
        """
        self.name = name
        self.base_url = base_url
        self.timeout = timeout or settings.request_timeout
        self.max_retries = max_retries if max_retries is not None else settings.max_retries
        self.retry_delay = retry_delay or settings.retry_delay
        self.rate_limit_delay = rate_limit_delay or settings.rate_limit_delay

        self._last_request_time: Optional[float] = None
        self._session: Optional[requests.Session] = None

        # Default headers to mimic a browser
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    @property
    def session(self) -> requests.Session:
        """Get or create a requests session."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(self.headers)
        return self._session

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting by waiting if necessary."""
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - elapsed
                logger.debug(f"[{self.name}] Rate limiting: sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)

    def _make_request(self, url: str, method: str = "GET", **kwargs) -> requests.Response:
        """
        Make an HTTP request with retry logic and exponential backoff.

        Args:
            url: The URL to request
            method: HTTP method (GET, POST, etc.)
            **kwargs: Additional arguments to pass to requests

        Returns:
            The response object

        Raises:
            ScraperError: If all retry attempts fail
        """
        # Apply rate limiting
        self._apply_rate_limit()

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"[{self.name}] Request attempt {attempt + 1}/{self.max_retries}: {url}")

                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.timeout,
                    **kwargs
                )
                response.raise_for_status()

                self._last_request_time = time.time()
                logger.debug(f"[{self.name}] Request successful: {url}")
                return response

            except requests.exceptions.Timeout as e:
                last_exception = e
                logger.warning(f"[{self.name}] Request timeout (attempt {attempt + 1}): {url}")

            except requests.exceptions.ConnectionError as e:
                last_exception = e
                logger.warning(f"[{self.name}] Connection error (attempt {attempt + 1}): {url}")

            except requests.exceptions.HTTPError as e:
                last_exception = e
                status_code = e.response.status_code if e.response else "unknown"
                logger.warning(f"[{self.name}] HTTP error {status_code} (attempt {attempt + 1}): {url}")

                # Don't retry on client errors (4xx) except 429 (rate limit)
                if e.response and 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                    raise ScraperError(
                        f"HTTP error {e.response.status_code}: {url}",
                        url=url,
                        cause=e
                    )

            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.warning(f"[{self.name}] Request error (attempt {attempt + 1}): {url} - {e}")

            # Exponential backoff before retry
            if attempt < self.max_retries - 1:
                delay = self.retry_delay * (2 ** attempt)
                logger.debug(f"[{self.name}] Retrying in {delay:.2f}s...")
                time.sleep(delay)

        # All retries exhausted
        raise ScraperError(
            f"Failed after {self.max_retries} attempts: {url}",
            url=url,
            cause=last_exception
        )

    def fetch_page(self, url: str) -> BeautifulSoup:
        """
        Fetch a page and parse it with BeautifulSoup.

        Args:
            url: The URL to fetch

        Returns:
            A BeautifulSoup object of the parsed page

        Raises:
            ScraperError: If the request fails
        """
        response = self._make_request(url)
        return BeautifulSoup(response.content, "html.parser")

    @staticmethod
    def compute_hash(content: str) -> str:
        """
        Compute SHA256 hash of content for deduplication.

        Args:
            content: The content to hash

        Returns:
            Hexadecimal string of the SHA256 hash
        """
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    @staticmethod
    def compute_item_hash(title: str, url: str) -> str:
        """
        Compute a hash for a news item based on title and URL.

        Args:
            title: The item title
            url: The item URL

        Returns:
            Hexadecimal string of the SHA256 hash
        """
        content = f"{title.strip().lower()}|{url.strip().lower()}"
        return BaseScraper.compute_hash(content)

    def normalize_url(self, url: str) -> str:
        """
        Normalize a URL by making it absolute if needed.

        Args:
            url: The URL to normalize (can be relative or absolute)

        Returns:
            An absolute URL
        """
        if not url:
            return ""

        url = url.strip()

        # Already absolute
        if url.startswith(("http://", "https://")):
            return url

        # Protocol-relative URL
        if url.startswith("//"):
            return f"https:{url}"

        # Relative URL - join with base
        from urllib.parse import urljoin
        return urljoin(self.base_url, url)

    def clean_text(self, text: Optional[str]) -> str:
        """
        Clean and normalize text content.

        Args:
            text: The text to clean

        Returns:
            Cleaned text with normalized whitespace
        """
        if not text:
            return ""
        # Replace multiple whitespace with single space
        import re
        return re.sub(r"\s+", " ", text).strip()

    def parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Parse a date string and return it in a standardized format.

        Args:
            date_str: The date string to parse

        Returns:
            Date in YYYY-MM-DD format, or the original string if parsing fails
        """
        if not date_str:
            return None

        date_str = self.clean_text(date_str)

        # Common date formats to try
        formats = [
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d %b %Y",
            "%d %B %Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d.%m.%Y",
        ]

        for fmt in formats:
            try:
                parsed = datetime.strptime(date_str, fmt)
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Return original if no format matched
        logger.debug(f"[{self.name}] Could not parse date: {date_str}")
        return date_str

    @abstractmethod
    def scrape(self) -> List[Tuple[str, str, Optional[str]]]:
        """
        Scrape the source and return a list of items.

        Returns:
            List of tuples containing (title, url, date)
            - title: The title/heading of the news item
            - url: The URL of the news item
            - date: The publication date (if available), or None

        Raises:
            ScraperError: If scraping fails
        """
        pass

    def close(self) -> None:
        """Close the session and clean up resources."""
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
