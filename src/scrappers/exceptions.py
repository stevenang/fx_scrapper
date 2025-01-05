class ScraperException(Exception):
    """Base exception for scraper errors"""
    pass

class FetchError(ScraperException):
    """Raised when unable to fetch data from the source"""
    pass

class ParseError(ScraperException):
    """Raised when unable to parse the fetched data"""
    pass

class ValidationError(ScraperException):
    """Raised when data validation fails"""
    pass