"""
Base API client with retry logic, rate limiting, and caching.
"""
import json
import time
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests
import requests_cache
from loguru import logger
from ratelimit import limits, sleep_and_retry


class APIError(Exception):
    """Custom exception for API-related errors."""

    pass


class RateLimitExceeded(APIError):
    """Exception raised when rate limit is exceeded."""

    pass


def exponential_backoff_retry(
    max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0
) -> Callable:
    """
    Decorator for exponential backoff retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        
    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, APIError) as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded: {e}")
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (2**attempt), max_delay)
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)

            raise last_exception

        return wrapper

    return decorator


class BaseAPIHarvester(ABC):
    """
    Abstract base class for API harvesters.
    
    Provides:
    - Session management with connection pooling
    - Response caching to disk
    - Rate limiting
    - Error handling and logging
    - Standardized response parsing
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        cache_expire_after: int = 86400,  # 24 hours
        timeout: int = 30,
        max_retries: int = 3,
    ):
        """
        Initialize API harvester.
        
        Args:
            cache_dir: Directory for caching responses
            cache_expire_after: Cache expiration time in seconds
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.source_name = self.__class__.__name__.replace("Harvester", "").lower()

        # Setup cache directory
        if cache_dir is None:
            cache_dir = Path("data/raw/api_harvest") / self.source_name
        else:
            cache_dir = Path(cache_dir) / self.source_name

        cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = cache_dir

        # Setup requests session with caching
        cache_name = str(cache_dir / "http_cache")
        self.session = requests_cache.CachedSession(
            cache_name=cache_name,
            backend="sqlite",
            expire_after=cache_expire_after,
            allowable_methods=["GET", "POST"],
            allowable_codes=[200],
            stale_if_error=True,
        )

        # Configure session headers
        self.session.headers.update(
            {
                "User-Agent": "Chemical-Matcher-Bootstrap/1.0",
                "Accept": "application/json",
            }
        )

        logger.info(f"Initialized {self.source_name} harvester with cache at {cache_dir}")

    @abstractmethod
    def harvest_synonyms(self, cas_number: str, chemical_name: str) -> List[str]:
        """
        Harvest synonyms for a chemical.
        
        Args:
            cas_number: CAS registry number
            chemical_name: Chemical name
            
        Returns:
            List of synonym strings
        """
        pass

    @abstractmethod
    def get_rate_limit(self) -> tuple[int, int]:
        """
        Get rate limit parameters.
        
        Returns:
            Tuple of (calls, period_seconds)
        """
        pass

    @exponential_backoff_retry(max_retries=3)
    def _make_request(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
    ) -> requests.Response:
        """
        Make HTTP request with error handling.
        
        Args:
            url: Request URL
            method: HTTP method (GET or POST)
            params: URL parameters
            json_data: JSON payload for POST requests
            
        Returns:
            Response object
            
        Raises:
            APIError: On request failure
        """
        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params, timeout=self.timeout)
            elif method.upper() == "POST":
                response = self.session.post(
                    url, params=params, json=json_data, timeout=self.timeout
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Check if response came from cache
            is_cached = getattr(response, "from_cache", False)
            if is_cached:
                logger.debug(f"Cache hit for {url}")

            response.raise_for_status()
            return response

        except requests.Timeout as e:
            raise APIError(f"Request timeout for {url}: {e}")
        except requests.HTTPError as e:
            if response.status_code == 429:
                raise RateLimitExceeded(f"Rate limit exceeded: {e}")
            elif response.status_code == 404:
                logger.debug(f"Resource not found: {url}")
                return response  # Return 404 responses for handling
            else:
                raise APIError(f"HTTP error {response.status_code}: {e}")
        except requests.RequestException as e:
            raise APIError(f"Request failed for {url}: {e}")

    def _parse_json_response(self, response: requests.Response) -> Optional[Dict[str, Any]]:
        """
        Parse JSON response with error handling.
        
        Args:
            response: Response object
            
        Returns:
            Parsed JSON data or None on error
        """
        if response.status_code == 404:
            return None

        try:
            return response.json()
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            return None

    def save_raw_response(self, identifier: str, data: Any, suffix: str = "json"):
        """
        Save raw API response to disk for debugging.
        
        Args:
            identifier: Unique identifier (e.g., CAS number)
            data: Data to save
            suffix: File suffix
        """
        filepath = self.cache_dir / f"{identifier.replace('/', '_')}.{suffix}"
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                if isinstance(data, (dict, list)):
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    f.write(str(data))
        except Exception as e:
            logger.warning(f"Failed to save raw response to {filepath}: {e}")

    def clear_cache(self):
        """Clear the HTTP cache."""
        self.session.cache.clear()
        logger.info(f"Cleared cache for {self.source_name}")

    def get_cache_info(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache info
        """
        return {
            "backend": self.session.cache.db_path if hasattr(self.session.cache, "db_path") else "unknown",
            "responses_cached": len(self.session.cache.responses) if hasattr(self.session.cache, "responses") else 0,
        }

    def close(self):
        """Close the session and cleanup resources."""
        self.session.close()
        logger.debug(f"Closed {self.source_name} harvester session")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
