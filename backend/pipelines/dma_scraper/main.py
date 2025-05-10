from bronze.fetch_company_data import DMAScraper
import os
from google.cloud import storage
from  datetime import datetime
# inside backend/pipelines/dma_scraper/fetch_company_data.py
import os, sys
import time
import nest_asyncio
import asyncio

ROOT = os.path.abspath(os.path.join(__file__, "..", "..", ".."))
sys.path.insert(0, ROOT)

from common.storage_interface import LocalStorage, GCSStorage
from silver.fetch_company_detail import DMACompanyDetailScraper
PREFIX_BRONZE_SAVE_PATH = os.environ.get("BRONZE_OUTPUT_DIR", "bronze/dma")
PREFIX_SILVER_SAVE_PATH = os.environ.get("SILVER_OUTPUT_DIR", "silver/dma")
nest_asyncio.apply()

# Initialize GCS client and bucket
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
if ENVIRONMENT.lower() in ("production", "container"):
    storage_backend = GCSStorage(os.environ.get("GCS_BUCKET", "landbrugsdata-raw-data"))
else:
    storage_backend = LocalStorage(os.environ.get("BRONZE_OUTPUT_DIR", "."))

scraper = DMAScraper()

def save_data(data, timestamp, page, PATH):
    timestamp_dir = os.path.join(PATH, timestamp)
    blob_name = f"{timestamp_dir}/page_{page}.json"
    storage_backend.save_json(data, blob_name)
    print(f"Saved {blob_name} to storage")

def save_parquet(data, timestamp, page, PATH):
    timestamp_dir = os.path.join(PATH, timestamp)
    blob_name = f"{timestamp_dir}/page_{page}.parquet"
    storage_backend.save_parquet(data, blob_name)
    print(f"Saved {blob_name} to storage")

# Fetch all pages
all_results = []
page = 1
total_pages = None
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
while total_pages is None or page <= total_pages:
    print(f"Fetching page {page}...")
    data = scraper.fetch_data(page)

    if total_pages is None:
        total_pages = data['pagination']['antalSider']
        print(f"Total pages: {total_pages}")

    page_results = scraper.extract_info(data)
    save_data(page_results, timestamp, page, PREFIX_BRONZE_SAVE_PATH)
    time.sleep(1)  # Add a delay to avoid overwhelming the server
    
    detail_scraper = DMACompanyDetailScraper(page_results)

    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(detail_scraper.process_miljoeaktoer_for_company_file_path())
    save_data(data, timestamp, page, PREFIX_SILVER_SAVE_PATH)
    save_parquet(data, timestamp, page, PREFIX_SILVER_SAVE_PATH)
    
    page += 1