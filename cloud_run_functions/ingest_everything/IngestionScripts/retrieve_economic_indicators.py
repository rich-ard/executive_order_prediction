#!/usr/bin/env python3

# import modules
from datetime import datetime, timedelta
import pandas as pd
import logging
import requests
from google.cloud import storage

# Configure the logging module
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a logger
logger = logging.getLogger(__name__)

# Set up Google Cloud Storage client
storage_client = storage.Client(project='executive-orders-448515')
bucket_name = 'executive-orders'
bucket = storage_client.bucket(bucket_name)

# economic indicator data is refreshed almost every day. more information here:
# https://www.census.gov/economic-indicators/ see the About page
# 'almost every day' does appear to include every friday. so an ideal pull would happen
# on a saturday morning, pulling the previous friday's data
# in the event that a friday pull is unsuccessful and 404s, we subtract a day and try again
# until we get a good CSV file.

url = 'https://www.census.gov/econ_index/archive_data/Indicator_Input_Values_' # ...YYYYMMDD.csv

# get an YYYYMMDD date that is the most recent friday
def get_most_recent_friday(date_format="%Y%m%d"):
    today = datetime.today()
    # Find the offset to the most recent Friday (0 = Monday, 6 = Sunday)
    offset = (today.weekday() - 4) % 7
    last_friday = today - timedelta(days=offset)
    last_friday_str = last_friday.strftime(date_format)
    logger.info(f'most recent Friday date: {last_friday_str}')
    return last_friday_str

def get_file_from_url():
    friday_date_string = '20250109' #get_most_recent_friday()
    friday_date_int = int(friday_date_string)
    date_int = friday_date_int
    while (friday_date_int-date_int) < 6:
        response = requests.get(f'{url}{str(date_int)}.csv')
        if response.status_code == 200:
            logger.info(f'successfully retrieved data for {str(date_int)}')
            break
        else:
            date_int = date_int-1
            logger.error(f'no file for date {str(date_int)}')
    return response.text, date_int

def retrieve_and_write_csv_to_bucket():
    output, most_recent_date = get_file_from_url()
    blob_name = f'economic_indicators/economic indicators_on_{most_recent_date}.csv' 
    blob = bucket.blob(blob_name)

    try:
        blob.upload_from_string(data=output)
        logger.log(f'Data successfully stored in GCS: {blob.public_url}')

    except Exception as e:
        logger.error(f'Storage in GCS failed')

if __name__ == '__main__':
    retrieve_and_write_csv_to_bucket()