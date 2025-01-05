import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scrappers import BOTScraper as bot
from scrappers import CTBCScraper as ctbc
from pymongo import MongoClient, UpdateOne, InsertOne
import os
import logging
from datetime import datetime

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hello_world():
    logger.info("Hello World")


def goodbye_world():
    logger.info("Goodbye World")


def get_mongo_client():
    """Get MongoDB client with authentication"""
    return MongoClient(
        host=os.getenv('MONGO_HOST', 'mongodb'),
        port=int(os.getenv('MONGO_PORT', 27017)),
        username=os.getenv('MONGO_USER', 'fx_user'),
        password=os.getenv('MONGO_PASSWORD', 'fx_password'),
        authSource=os.getenv('MONGO_DB', 'fx_rates')
    )


def scrape_bot_rates(**context):
    """Scrape FX rates and save to MongoDB"""
    try:
        # Initialize scraper and get rates
        scraper = bot()
        rates = scraper.scrape()

        if not rates:
            raise ValueError("No rates were scraped")

        # Transform rates for MongoDB
        transformed_rates = []
        for rate in rates:
            transformed_rate = {
                'currency_en': rate['currency']['en'],
                'currency_zh': rate['currency']['zh'],
                'cash_buy': rate['rates']['cash']['buy'],
                'cash_sell': rate['rates']['cash']['sell'],
                'spot_buy': rate['rates']['spot']['buy'],
                'spot_sell': rate['rates']['spot']['sell'],
                'rates': rate['rates'],
                'institution': 'Bank of Taiwan',
                'timestamp': datetime.fromisoformat(rate['timestamp'])
            }
            transformed_rates.append(transformed_rate)

        for rate in transformed_rates:
            # Create a copy for logging that handles datetime serialization
            log_rate = rate.copy()
            log_rate['timestamp'] = log_rate['timestamp'].isoformat()
            logger.info(json.dumps(log_rate, indent=4, ensure_ascii=False))

        # Push to XCom for the consolidation task
        context['task_instance'].xcom_push(key='bot_rates', value=transformed_rates)
        return f"Scraped {len(transformed_rates)} rates from BOT"

    except Exception as e:
        logger.error(f"Error in scrape_and_save_rates: {str(e)}")
        raise


def scrape_ctbc_rates(**context):
    """Scrape CTBC rates"""
    try:
        scraper = ctbc()
        rates = scraper.scrape()

        if not rates:
            raise ValueError("No rates were scraped from CTBC")

        # Transform rates
        transformed_rates = []
        for rate in rates:
            transformed_rate = {
                'currency_en': rate['currency']['en'],
                'currency_zh': rate['currency']['zh'],
                'cash_buy': rate['rates']['cash']['buy'],
                'cash_sell': rate['rates']['cash']['sell'],
                'spot_buy': rate['rates']['spot']['buy'],
                'spot_sell': rate['rates']['spot']['sell'],
                'rates': rate['rates'],
                'institution': 'CTBC Bank',
                'timestamp': datetime.fromisoformat(rate['timestamp'])
            }
            transformed_rates.append(transformed_rate)

        # Push to XCom for the consolidation task
        context['task_instance'].xcom_push(key='ctbc_rates', value=transformed_rates)
        return f"Scraped {len(transformed_rates)} rates from CTBC"

    except Exception as e:
        logger.error(f"Error scraping CTBC rates: {str(e)}")
        raise


def save_to_mongodb(**context):
    """Consolidate rates from all sources and save to MongoDB"""
    try:
        ti = context['task_instance']

        # Get rates from XCom
        bot_rates = ti.xcom_pull(task_ids='scrape_bot_rates', key='bot_rates') or []
        ctbc_rates = ti.xcom_pull(task_ids='scrape_ctbc_rates', key='ctbc_rates') or []

        # Combine all rates
        all_rates = bot_rates + ctbc_rates

        if not all_rates:
            raise ValueError("No rates available to save")

        # Log the rates for debugging
        logger.info(f"Total rates to save: {len(all_rates)}")
        for rate in all_rates:
            rate_copy = rate.copy()
            rate_copy['timestamp'] = rate_copy['timestamp'].isoformat()
            logger.info(json.dumps(rate_copy, indent=4, ensure_ascii=False))

        # Save to MongoDB
        with get_mongo_client() as client:
            db = client[os.getenv('MONGO_DB', 'fx_rates')]
            collection = db['exchange_rates']

            # Create insert operations
            operations = [InsertOne(rate) for rate in all_rates]

            # Execute bulk insert
            result = collection.bulk_write(operations)
            logger.info(f"MongoDB operations completed: {result.bulk_api_result}")

            # Log the total count
            total_records = collection.count_documents({})
            logger.info(f"Total records in collection: {total_records}")

            return f"Inserted {len(all_rates)} new rates"

    except Exception as e:
        logger.error(f"Error saving rates to MongoDB: {str(e)}")
        raise


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'fx_rate_scraper',
    default_args=default_args,
    description='A DAG that scrapes fx rates',
    schedule_interval='0,30 * * * *',
    start_date=datetime(2025, 1, 4),
    catchup=False,
) as dag:

    hello_world_task = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world
    )

    # Scraping tasks
    scrape_bot = PythonOperator(
        task_id='scrape_bot_rates',
        python_callable=scrape_bot_rates
    )

    #scrape_ctbc = PythonOperator(
    #    task_id='scrape_ctbc_rates',
    #    python_callable=scrape_ctbc_rates
    #)

    # MongoDB save task
    save_rates = PythonOperator(
        task_id='save_to_mongodb',
        python_callable=save_to_mongodb
    )

    goodbye_world_task = PythonOperator(
        task_id='goodbye_world',
        python_callable=goodbye_world
    )

    #hello_world_task >> [scrape_bot, scrape_ctbc] >> save_rates >> goodbye_world_task
    hello_world_task >> scrape_bot >> save_rates >> goodbye_world_task