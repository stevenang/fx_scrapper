from abc import ABC, abstractmethod
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union
from .exceptions import ScraperException, FetchError, ParseError, ValidationError

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for FX rate scrapers"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    @property
    @abstractmethod
    def institution_name(self) -> str:
        """Return the name of the financial institution"""
        pass

    @property
    @abstractmethod
    def base_url(self) -> str:
        """Return the base URL for scraping"""
        pass

    @abstractmethod
    def fetch_data(self) -> Optional[str]:
        """Fetch raw data from the source"""
        pass

    @abstractmethod
    def parse_rates(self, html: str) -> List[Dict]:
        """Parse the fetched data into structured format"""
        pass

    def validate_rate(self, rate: Dict) -> bool:
        """Validate a single rate entry"""
        required_fields = ['currency', 'rates', 'timestamp']
        try:
            # Check required fields
            if not all(field in rate for field in required_fields):
                return False

            # Check currency structure
            if not isinstance(rate['currency'], dict) or \
                    not all(k in rate['currency'] for k in ['en', 'zh']):
                return False

            # Check rates structure
            if not isinstance(rate['rates'], dict) or \
                    not all(k in rate['rates'] for k in ['cash', 'spot']):
                return False

            # Check rate values
            for rate_type in ['cash', 'spot']:
                if not all(k in rate['rates'][rate_type] for k in ['buy', 'sell']):
                    return False

            return True
        except Exception as e:
            logger.error(f"Rate validation error: {e}")
            return False

    def _parse_rate(self, rate_str: str) -> Optional[float]:
        """Convert rate string to float, handling invalid values"""
        try:
            # Remove any whitespace and replace any dash or N/A with None
            cleaned = rate_str.strip()
            if cleaned in ['-', 'N/A', '', '----']:
                return None
            # Replace any commas and convert to float
            return float(cleaned.replace(',', ''))
        except (ValueError, AttributeError) as e:
            logger.debug(f"Rate parsing error: {e}")
            return None

    def scrape(self) -> List[Dict]:
        """Main method to scrape exchange rates"""
        try:
            # Fetch data
            html = self.fetch_data()
            if not html:
                raise FetchError(f"Failed to fetch data from {self.institution_name}")

            # Parse rates
            rates = self.parse_rates(html)
            if not rates:
                raise ParseError(f"Failed to parse rates from {self.institution_name}")

            # Validate rates
            valid_rates = []
            for rate in rates:
                if self.validate_rate(rate):
                    valid_rates.append(rate)
                else:
                    logger.warning(f"Invalid rate data from {self.institution_name}: {rate}")

            if not valid_rates:
                raise ValidationError(f"No valid rates found from {self.institution_name}")

            logger.info(f"Successfully scraped {len(valid_rates)} rates from {self.institution_name}")
            return valid_rates

        except ScraperException as e:
            logger.error(f"Scraping error for {self.institution_name}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for {self.institution_name}: {str(e)}")
            raise ScraperException(f"Failed to scrape {self.institution_name}: {str(e)}")