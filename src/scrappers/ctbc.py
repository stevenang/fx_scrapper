from typing import Optional, List, Dict
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging

from .base import BaseScraper
from .exceptions import FetchError, ParseError

logger = logging.getLogger(__name__)


class CTBCScraper(BaseScraper):
    """CTBC Bank Exchange Rate Scraper"""

    @property
    def institution_name(self) -> str:
        return "CTBC Bank"

    @property
    def base_url(self) -> str:
        return "https://www.ctbcbank.com/twrbo/zh_tw/dep_index/dep_ratequery/dep_foreign_rates.html"

    def fetch_data(self) -> Optional[str]:
        """Fetch the webpage content"""
        try:
            response = requests.get(
                self.base_url,
                headers={
                    **self.headers,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Referer': 'https://www.ctbcbank.com/',
                }
            )
            response.raise_for_status()
            response.encoding = 'utf-8'

            # Debug: Log the first part of the response
            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"First 500 characters of response: {response.text[:500]}")

            return response.text

        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from {self.institution_name}: {e}")
            raise FetchError(f"HTTP request failed: {str(e)}")

    def parse_rates(self, html: str) -> List[Dict]:
        """Parse the HTML and extract exchange rates"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            rates = []

            # Debug: Log all table IDs found
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables")
            for table in tables:
                logger.info(f"Table ID: {table.get('id', 'No ID')} Class: {table.get('class', 'No Class')}")

            # Try different selectors
            table = (
                    soup.find('table', {'id': 'table_deposit_fxrate_content'}) or
                    soup.find('table', {'class': 'table_deposit_fxrate_content'}) or
                    soup.find('table', {'class': 'rate-table'}) or
                    soup.find('table', class_=lambda x: x and 'rate' in x.lower())
            )

            if not table:
                # If no table found, log the entire HTML for debugging
                logger.error("Could not find exchange rate table")
                logger.debug(f"Full HTML content: {html}")
                raise ParseError("Could not find exchange rate table")

            # Log the found table
            logger.info(f"Found table: {table.prettify()[:500]}")

            # Process rows
            rows = table.find_all('tr')
            logger.info(f"Found {len(rows)} rows in the table")

            for row in rows[1:]:  # Skip header row
                try:
                    cells = row.find_all('td')
                    if len(cells) < 5:
                        continue

                    # Debug: Log cell contents
                    cell_texts = [cell.text.strip() for cell in cells]
                    logger.info(f"Processing row with cells: {cell_texts}")

                    # Parse currency info
                    currency_text = cells[0].text.strip()
                    logger.info(f"Currency text: {currency_text}")

                    # Handle different currency text formats
                    currency_parts = currency_text.split()
                    if len(currency_parts) >= 2:
                        currency_en = currency_parts[0]
                        currency_zh = currency_parts[1]
                    else:
                        logger.warning(f"Unexpected currency format: {currency_text}")
                        continue

                    rate = {
                        'currency': {
                            'en': currency_en,
                            'zh': currency_zh
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

                    # Debug: Log parsed rate
                    logger.info(f"Parsed rate: {rate}")

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
            logger.error(f"Error details: {str(e)}")
            raise ParseError(f"HTML parsing failed: {str(e)}")

    def _parse_rate(self, rate_str: str) -> Optional[float]:
        """Parse rate string to float"""
        try:
            # Debug: Log the input rate string
            logger.debug(f"Parsing rate string: {rate_str}")

            cleaned = rate_str.strip()
            if cleaned in ['-', 'N/A', '', '----', '---']:
                return None

            # Remove any thousands separators and convert to float
            cleaned = cleaned.replace(',', '')
            result = float(cleaned)

            logger.debug(f"Parsed rate result: {result}")
            return result

        except (ValueError, AttributeError) as e:
            logger.warning(f"Rate parsing error: {e} for string: {rate_str}")
            return None