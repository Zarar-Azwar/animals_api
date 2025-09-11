import asyncio
from typing import List, Dict, Any, Optional
import aiohttp
import json
from dataclasses import dataclass
import random
from Common.logger import CustomLogger
from Common.configs import EnvConfig

env_config = EnvConfig()
logger = CustomLogger().get_logger()


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = int(env_config.get("MAX_ATTEMPTS",5))
    base_delay: float = int(env_config.get("BASE_DELAY", 1))
    max_delay: float = float(env_config.get("MAX_DELAY", 60))
    backoff_factor: float = int(env_config.get("BACKOFF_FACTOR", 2))  # 2.0
    jitter: bool = True


class AnimalAPIClient:
    """
    Asynchronous API client for interacting with the Animal Service.

    This client provides methods to list animals, fetch detailed animal
    information, and load animals into the home endpoint. It is designed 
    with reliability in mind, featuring:
      - **Connection management** with aiohttp sessions.
      - **Automatic retries** with exponential backoff and optional jitter 
        for transient server/network failures.
      - **Async context manager support** for safe resource handling.

    Attributes:
        base_url (str): Base URL of the Animal API (without trailing slash).
        retry_config (RetryConfig): Configuration for retry attempts, delay,
            backoff, and jitter.
        session (Optional[aiohttp.ClientSession]): Active aiohttp session
            used for making requests.

    Class Attributes:
        RETRY_STATUS_CODES (set[int]): HTTP status codes that should trigger
            a retry (default: {500, 502, 503, 504}).

    Usage example:
        ```python
        async with AnimalAPIClient("http://localhost:3123") as client:
            # List animals
            response = await client.list_animals(page=1, per_page=50)
            
            # Fetch details
            details = await client.get_animal_details("123")
            
            # Load animals
            await client.load_animals_home([details])
        ```

    Methods:
        __aenter__() -> AnimalAPIClient:
            Initialize client when used as an async context manager.
        __aexit__():
            Close the client session when context exits.
        close():
            Manually close the HTTP session.
        list_animals(page: int = 1, per_page: int = 50) -> Dict[str, Any]:
            Retrieve paginated list of animals.
        get_animal_details(animal_id: str) -> Dict[str, Any]:
            Get detailed information for a single animal.
        load_animals_home(animals: List[Dict[str, Any]]) -> Dict[str, Any]:
            Load a batch of animals (up to 100) into the home endpoint.
        health_check() -> bool:
            Check if the API is reachable and responsive.
    """
    # HTTP status codes that should trigger retries
    RETRY_STATUS_CODES = {500, 502, 503, 504}
    
    def __init__(self, base_url: str, retry_config: Optional[RetryConfig] = None):
        self.base_url = base_url.rstrip('/')
        self.retry_config = retry_config or RetryConfig()
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        
    async def _ensure_session(self):
        """Ensure aiohttp session is created."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={'Content-Type': 'application/json'}
            )
            
    async def close(self):
        """Close the HTTP session."""
        if self.session and not self.session.closed:
            await self.session.close()
            
    async def _request_with_retry(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """
        Make HTTP request with exponential backoff retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            **kwargs: Additional arguments for aiohttp request
            
        Returns:
            Parsed JSON response
            
        Raises:
            aiohttp.ClientError: After all retry attempts exhausted
        """
        await self._ensure_session()
        
        last_exception = None
        
        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                logger.debug(f"Attempt {attempt}/{self.retry_config.max_attempts}: {method} {url}")
                
                async with self.session.request(method, url, **kwargs) as response:
                    # Log the response status
                    logger.debug(f"Response status: {response.status}")
                    
                    # Handle successful responses
                    if response.status == 200:
                        try:
                            data = await response.json()
                            logger.debug(f"Successful response on attempt {attempt}")
                            return data
                        except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                            logger.error(f"Failed to parse JSON response: {e}")
                            raise aiohttp.ClientError(f"Invalid JSON response: {e}")
                    
                    # Handle client errors (4xx) - don't retry these
                    elif 400 <= response.status < 500:
                        error_text = await response.text()
                        error_msg = f"Client error {response.status}: {error_text}"
                        logger.error(error_msg)
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=error_msg
                        )
                    
                    # Handle server errors (5xx) - retry these
                    elif response.status in self.RETRY_STATUS_CODES:
                        error_text = await response.text()
                        error_msg = f"Server error {response.status}: {error_text}"
                        logger.warning(f"Attempt {attempt} failed with {error_msg}")
                        last_exception = aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=error_msg
                        )
                    
                    # Handle other status codes
                    else:
                        error_text = await response.text()
                        error_msg = f"Unexpected status {response.status}: {error_text}"
                        logger.error(error_msg)
                        raise aiohttp.ClientError(error_msg)
                        
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Attempt {attempt} failed with {type(e).__name__}: {e}")
                last_exception = e
                
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}: {e}")
                last_exception = e
                
            # Calculate delay before next retry
            if attempt < self.retry_config.max_attempts:
                delay = min(
                    self.retry_config.base_delay * (self.retry_config.backoff_factor ** (attempt - 1)),
                    self.retry_config.max_delay
                )
                
                # Add jitter to prevent thundering herd
                if self.retry_config.jitter:
                    delay *= (0.5 + random.random() * 0.5)
                    
                logger.info(f"Waiting {delay:.2f}s before retry...")
                await asyncio.sleep(delay)
        
        # All retries exhausted
        logger.error(f"All {self.retry_config.max_attempts} attempts failed")
        if last_exception:
            raise last_exception
        else:
            raise aiohttp.ClientError("All retry attempts failed")
    
    async def list_animals(self, page: int = 1, per_page: int = 50) -> Dict[str, Any]:
        """
        List animals with pagination.
        
        Args:
            page: Page number (1-based)
            per_page: Number of items per page
            
        Returns:
            API response containing animals list
        """
        url = f"{self.base_url}/animals/v1/animals"
        params = {'page': page, 'per_page': per_page}
        
        logger.info(f"Fetching animals list - page {page}")
        return await self._request_with_retry('GET', url, params=params)
    
    async def get_animal_details(self, animal_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific animal.
        
        Args:
            animal_id: Unique animal identifier
            
        Returns:
            Animal details
        """
        url = f"{self.base_url}/animals/v1/animals/{animal_id}"
        
        logger.debug(f"Fetching details for animal {animal_id}")
        return await self._request_with_retry('GET', url)
    
    async def load_animals_home(self, animals: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load animals to the home endpoint.
        
        Args:
            animals: List of animal data (max 100 items)
            
        Returns:
            API response
            
        Raises:
            ValueError: If animals list exceeds 100 items
        """
        if len(animals) > 100:
            raise ValueError(f"Cannot load more than 100 animals at once. Got {len(animals)}")
            
        url = f"{self.base_url}/animals/v1/home"
        
        logger.info(f"Loading {len(animals)} animals to home endpoint")
        return await self._request_with_retry(
            'POST', 
            url, 
            json=animals,
            headers={'Content-Type': 'application/json'}
        )
    
    async def health_check(self) -> bool:
        """
        Check if the API is responding.
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            url = f"{self.base_url}/docs"
            await self._request_with_retry('GET', url)
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

