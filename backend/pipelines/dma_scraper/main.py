from fetch_company_data import DMAScraper
import os
from google.cloud import storage
from  datetime import datetime
# inside backend/pipelines/dma_scraper/fetch_company_data.py
import os, sys
import time
ROOT = os.path.abspath(os.path.join(__file__, "..", "..", ".."))
sys.path.insert(0, ROOT)

from common.storage_interface import LocalStorage, GCSStorage
PREFIX_SAVE_PATH = os.environ.get("BRONZE_OUTPUT_DIR", "bronze/dma")
# Initialize GCS client and bucket
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
if ENVIRONMENT.lower() in ("production", "container"):
    storage_backend = GCSStorage(os.environ.get("GCS_BUCKET_NAME", "landbrugsdata-raw-data"))
else:
    storage_backend = LocalStorage(os.environ.get("BRONZE_OUTPUT_DIR", "."))

scraper = DMAScraper()


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

    # Save page results via storage interface
    timestamp_dir = os.path.join(PREFIX_SAVE_PATH, timestamp)
    blob_name = f"{timestamp_dir}/page_{page}.json"
    storage_backend.save_json(page_results, blob_name)
    print(f"Saved {blob_name} to storage")
    page += 1
    time.sleep(1)  # Add a delay to avoid overwhelming the server