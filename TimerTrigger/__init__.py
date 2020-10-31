import sys
from sys import path
import os
import datetime
import logging

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

import azure.functions as func
from news_scraper import NewsScraper


def main(mytimer: func.TimerRequest) -> None:
    
    ns = NewsScraper(['AAPL:US'])
    ns.get_all_articles()
    
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    logging.info('Function was ran at %s', utc_timestamp)
