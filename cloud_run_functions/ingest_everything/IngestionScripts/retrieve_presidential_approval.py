#!/usr/bin/env python3

# import modules
import urllib.request
from html_table_parser.parser import HTMLTableParser
import pandas as pd
from google.cloud import storage
import logging
import os
import re
from datetime import datetime

# Configure the logging module
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a logger
logger = logging.getLogger(__name__)

# Set up Google Cloud Storage client
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
storage_client = storage.Client(project_id)
bucket_name = 'executive-orders'
bucket = storage_client.bucket(bucket_name)

# Cheating here a little bit. I'm going to just populate a list of presidents rather than scrape for it.
presidents = [
    'joseph-r-biden',
    'donald-j-trump',
    'barack-obama',
    'george-w-bush',
    'william-j-clinton',
    'george-bush',
    'ronald-reagan',
    'jimmy-carter',
    'gerald-r-ford',
    'richard-m-nixon',
    'lyndon-b-johnson',
    'john-f-kennedy',
    'dwight-d-eisenhower',
    'harry-s-truman',
    'franklin-d-roosevelt'
]

generic_link_start = 'https://www.presidency.ucsb.edu/statistics/data/'
generic_link_end = '-public-approval'

# Opens a website and read its
# binary contents (HTTP Response Body)
def url_get_contents(url):

    # Opens a website and read its
    # binary contents (HTTP Response Body)

    #making request to the website
    req = urllib.request.Request(url=url)
    f = urllib.request.urlopen(req)

    #reading contents of the website
    full_site_text =  f.read()

    #decode the site text for parsing
    text_parsed = full_site_text.decode('utf-8')

    return text_parsed

# retrieve a dataframe of tabular data from each of the UCSB pages as they are formatted 2025-1-24
def retrieve_table_from_prez(prez):
    try: 
        # defining the html contents of a URL.
        xhtml = url_get_contents(f'{generic_link_start}{prez}{generic_link_end}')
        logger.info(f'website read for {prez} successful')

        try:
            # Defining the HTMLTableParser object
            p = HTMLTableParser()

            # feeding the html contents in the
            # HTMLTableParser object
            p.feed(xhtml)

            # Now finally obtaining the data of
            # the table required
            df = pd.DataFrame(p.tables[0])

            # take first row as header
            new_header = df.iloc[0] #grab the first row for the header
            df = df[1:] #take the data less the header row
            df.columns = new_header #set the header row as the df header

            # add president as first column
            df['president'] = prez

            logger.info(f'data load for {prez} successful')

        except Exception as e:
            logger.error(f'data load for {prez} failed: {e}')

    except Exception as e:
        logger.error(f'website read for {generic_link_start}{prez}{generic_link_end} failed: {e}')

    return df
    
def retrieve_and_write_csv_to_bucket():
    approval_data = None
    for president in presidents:
        if approval_data is None:
            approval_data = retrieve_table_from_prez(president)
        else:
            approval_data = pd.concat([approval_data, retrieve_table_from_prez(president)], ignore_index=True)
        
    # some dates appear to have been entered incorrectly - pull only correct dates
    dates_cleaned = [date for date in approval_data['Start Date'] if re.match('(\d{1,2}\/\d{1,2}\/\d{4})', str(date))]
    max_date = max(dates_cleaned, key=lambda d: datetime.strptime(d, '%m/%d/%Y'))
    blob_name = f'presidential_approvals/approval ratings loaded through {max_date}.csv' 
    blob = bucket.blob(blob_name)

    try:
        blob.upload_from_string(data=approval_data.to_csv(index=False), content_type='application/csv')
        logger.info(f'Data successfully stored in GCS: {blob.public_url}')

    except Exception as e:
        logger.error(f'Storage in GCS failed: {e}')


if __name__ == '__main__':
    retrieve_and_write_csv_to_bucket()
