import os
import json
import logging
from google.cloud import storage
import asyncio
import aiohttp
from importlib.machinery import SourceFileLoader

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import nest_asyncio
import traceback
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

nest_asyncio.apply()

async def fetch(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()
class DMACompanyDetailScraper:
    
    def __init__(self, data):
        self.data = data

    async def scrape_details(self, session, url):
        try:
            html = await fetch(session, url)
            soup = BeautifulSoup(html, 'html.parser')
            
            data = {
                'title': soup.find('div', class_='dma-content-header').find('span').text.strip()
            }
            
            for section in ['Grunddata', 'Adresse', 'Aktiviteter/anlæg og miljøkategorier', 'Myndighed', 'IED-oplysninger (Direktivet om industrielle emissioner)']:
                section_div = soup.find('div', string=section)
                if section_div:
                    section_body = section_div.find_next('div', class_='card-body')
                    for dt, dd in zip(section_body.find_all('dt'), section_body.find_all('dd')):
                        key = dt.text.strip(':')
                        value = dd.text.strip()
                        data[key] = value
            
                return data
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    async def scrape_table(self, session, url, table_id):
        try:
            html = await fetch(session, url)
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table', id=table_id)
        
            if not table:
                return []
        
            headers = [th.text.strip() for th in table.find_all('th')]
            rows = []
        
            for row in table.find_all('tr')[1:]:  # Skip header row
                cols = row.find_all('td')
                if len(cols) == len(headers):
                    row_data = {}
                    for i, col in enumerate(cols):
                        row_data[headers[i]] = col.text.strip()
                        if col.find('a'):
                            row_data[f"{headers[i]}_url"] = 'https://dma.mst.dk' + col.find('a')['href']
                    rows.append(row_data)
        
            return rows
        except Exception as e:
            logger.error(f"Error scraping table from {url}: {str(e)}")
            logger.error(traceback.format_exc())
        return []

    async def process_miljoeaktoer(self, session, url):
        logger.info(f"Processing {url}")
        try:
            data = await self.scrape_details(session, url)
            data['miljoeaktoerUrl'] = url
            
            # Scrape Tilsyn, Håndhævelser, and Afgørelser
            for section, table_id in [('Tilsyn', 'tilsyn-tabel'), 
                                    ('Håndhævelser', 'haandhaevelse-tabel'), 
                                    ('Afgørelser', 'afgoerelser-tabel')]:
                data[section] = await self.scrape_table(session, url, table_id)
            
            return data
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            return None
        
    async def process_miljoeaktoer_for_company_file_path(self, max_concurrent=20):
        urls = [item['miljoeaktoerUrl'] for item in self.data]
        sem = asyncio.Semaphore(max_concurrent)
        async with aiohttp.ClientSession() as session:
            async def bounded(url):
                async with sem:
                    return await self.process_miljoeaktoer(session, url)
            tasks = [bounded(url) for url in urls]
            company_details_data = await asyncio.gather(*tasks)
        return company_details_data

# Initialize GCS client
storage_client = storage.Client()

# Bucket and prefix env vars
GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME", "landbrugsdata-raw-data")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "dma_scraper/details")

def dma_scraper_trigger(event, context):
    """
    Cloud Function triggered by GCS finalize event on page JSON uploads.
    Fetches the uploaded JSON, scrapes details for each URL, and writes a new JSON.
    """
    bucket_name = event.get("bucket")
    file_name = event.get("name")
    logger.info(f"Triggered by file: gs://{bucket_name}/{file_name}")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    page_data = json.loads(content)

    scraper = DMACompanyDetailScraper(page_data)

    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(scraper.process_miljoeaktoer_for_company_file_path())
    out_name = os.path.basename(file_name).replace('.json', '_details.json')
    out_path = f"{OUTPUT_PREFIX}/{out_name}"
    out_blob = bucket.blob(out_path)
    out_blob.upload_from_string(json.dumps(data), content_type="application/json")
    logger.info(f"Uploaded detailed results to gs://{bucket_name}/{out_path}")


