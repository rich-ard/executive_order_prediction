#!/usr/bin/env python3

# import modules
import requests
from datetime import datetime
from google.cloud import storage
import json
import logging
import os

# Configure the logging module
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a logger
logger = logging.getLogger(__name__)

# Set up Google Cloud Storage client
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
storage_client = storage.Client(project_id)
bucket_name = 'executive-orders'
bucket = storage_client.bucket(bucket_name)

# Make API request to Federal Register
# API reference: https://www.federalregister.gov/developers/documentation/api/v1
# currently this allows 1000 page responses so we'll hedge our bets and retrieve 500 at a time
api_url = 'https://www.federalregister.gov/api/v1/documents.json?fields[]=presidential_document_number&fields[]=president&fields[]=publication_date&per_page=500&conditions[presidential_document_type][]=executive_order'
# ideally this would truncate the pull based on the max presidential_document_number in the newest file

def retrieve_from_federal_register(url):
    orders_in_json = {}
    response = requests.get(url)
    morepages = True
    # count = 1
    if response.status_code == 200:
        orders_in_json = json.loads(response.text)['results']
        
        while morepages == True:# and count <3:
            if 'next_page_url' not in json.loads(response.text):
                morepages = False
            else:
                next_url = json.loads(response.text)['next_page_url']
                response = requests.get(next_url)
                orders_in_json = orders_in_json + json.loads(response.text)['results']
        
        num_records = len(orders_in_json)
        # pres_doc_numbers = [record['presidential_document_number'] for record in orders_in_json]
        pres_doc_numbers = [item['presidential_document_number'] for item in orders_in_json if item['presidential_document_number'] != None]
        max_pres_doc_no = max([int(x) for x in pres_doc_numbers])
        pres_dates = [datetime.strptime(item['publication_date'], '%Y-%m-%d') for item in orders_in_json if item['publication_date'] != None]
        max_date = max(pres_dates).strftime("%Y-%m-%d")
        logger.info(f'successful API retrieval of {num_records} records up to order no. {max_pres_doc_no} on {max_date}')
        return json.dumps(orders_in_json), max_pres_doc_no, max_date
    else:
        logger.error(f'API failure with response status {response.status_code}')

def retrieve_and_write_json_to_bucket():
    big_json_file, newest_order, most_recent_date  = retrieve_from_federal_register(api_url)
    # Store data in GCS
    # file structure for GCP Hive partitioning here: https://cloud.google.com/bigquery/docs/hive-partitioned-queries#supported_data_layouts
    blob_name = f'executive_orders/dt={datetime.today().strftime('%Y-%m-%d')}/lang=en/executive_orders_through_{newest_order}_on_{most_recent_date}.json' 
    blob = bucket.blob(blob_name)

    try:
        blob.upload_from_string(data=big_json_file, content_type='application/json')
        logger.info(f'Data successfully stored in GCS: {blob.public_url}')

    except Exception as e:
        logger.error(f'Storage in GCS failed: {e}')

if __name__ == '__main__':
    retrieve_and_write_json_to_bucket()
