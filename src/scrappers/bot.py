from typing import Optional, List, Dict
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging

from .base import BaseScraper
from .exceptions import FetchError, ParseError

logger = logging.getLogger(__name__)


class BOTScraper(BaseScraper):
    """Bank of Taiwan Exchange Rate Scraper"""

    @property
    def institution_name(self) -> str:
        return "Bank of Taiwan"

    @property
    def base_url(self) -> str:
        return "https://rate.bot.com.tw/xrt?Lang=zh-TW"

    def fetch_data(self) -> Optional[str]:
        """Fetch the webpage content"""
        try:
            response = requests.get(self.base_url, headers=self.headers)
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response.text

        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from {self.institution_name}: {e}")
            raise FetchError(f"HTTP request failed: {str(e)}")

    def parse_rates(self, html: str) -> List[Dict]:
        """Parse the HTML and extract exchange rates"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            rates = []

            # Find all rows in the rate table
            rows = soup.select('.table tbody tr')
            if not rows:
                raise ParseError("Could not find exchange rate table")

            for row in rows:
                try:
                    # Extract currency info
                    currency_cell = row.select_one('.currency .print_show')
                    if not currency_cell:
                        continue

                    currency_zh = currency_cell.contents[0].split('(')[0].strip()
                    currency_en = currency_cell.contents[-1].split('(')[1].replace(')', '')

                    # Extract rates
                    cells = row.select('td')
                    if len(cells) < 5:  # We need at least 5 cells for all rates
                        continue

                    rate = {
                        'currency': {
                            'zh': currency_zh,
                            'en': currency_en
                        },
                        'rates': {
                            'cash': {
                                'buy': self._parse_rate(cells[1].text),
                                'sell': self._parse_rate(cells[2].text)
                            },
                            'spot': {
                                'buy': self._parse_rate(cells[3].text),
                                'sell': self._parse_rate(cells[4].text)
                            }
                        },
                        'timestamp': datetime.now().isoformat()
                    }

                    # Validate before adding
                    if self.validate_rate(rate):
                        rates.append(rate)
                    else:
                        logger.warning(f"Invalid rate data: {rate}")

                except Exception as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

            if not rates:
                raise ParseError("No valid rates found in the page")

            return rates

        except Exception as e:
            logger.error(f"Failed to parse HTML from {self.institution_name}: {e}")
            raise ParseError(f"HTML parsing failed: {str(e)}")

    def get_historical_rates(self, currency: str, days: int = 30) -> List[Dict]:
        """Get historical rates for a specific currency (if available)"""
        # This is a placeholder for potential historical data fetching
        # Could be implemented if BOT provides historical data
        logger.warning("Historical rates not implemented for Bank of Taiwan")
        return []