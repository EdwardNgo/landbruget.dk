#!/bin/bash

# Startup script for SFTP to GCS transfer VM with JSON to Parquet processing
# This script runs when the VM starts, performs the transfer with privacy transformations

set -e

# Enhanced logging function
log_with_timestamp() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a /var/log/sftp-transfer.log
    sync
}

# Error handling function
handle_error() {
    local exit_code=$?
    local line_number=$1
    log_with_timestamp "ERROR: Script failed at line $line_number with exit code $exit_code"
    log_with_timestamp "ERROR: Command that failed: ${BASH_COMMAND}"
    sync
    exit $exit_code
}

# Set error trap
trap 'handle_error ${LINENO}' ERR

# Log everything with timestamps
exec > >(while IFS= read -r line; do log_with_timestamp "$line"; done) 2>&1

log_with_timestamp "Starting enhanced SFTP to GCS transfer with processing"

# System resource check
check_resources() {
    log_with_timestamp "=== SYSTEM RESOURCE CHECK ==="
    log_with_timestamp "Available disk space:"
    df -h / | tee -a /var/log/sftp-transfer.log
    log_with_timestamp "Available memory:"
    free -h | tee -a /var/log/sftp-transfer.log
    log_with_timestamp "CPU info:"
    nproc | tee -a /var/log/sftp-transfer.log
    log_with_timestamp "================================"
    sync
}

check_resources

# Ensure we have enough disk space (need ~25GB for processing)
available_space=$(df / | awk 'NR==2{print $4}')
required_space=25000000  # 25GB in KB
if [ "$available_space" -lt "$required_space" ]; then
    log_with_timestamp "ERROR: Insufficient disk space. Available: ${available_space}KB, Required: ${required_space}KB"
    exit 1
fi

log_with_timestamp "✅ Sufficient disk space available"

# Install required packages
log_with_timestamp "Installing required packages..."
apt-get update
apt-get install -y python3-pip python3-venv git dnsutils iputils-ping unzip

log_with_timestamp "✅ System packages installed"
check_resources

# Create virtual environment
log_with_timestamp "Creating Python virtual environment..."
python3 -m venv /opt/transfer-env
source /opt/transfer-env/bin/activate

# Install required Python packages (added ijson, pyarrow, uuid for processing)
log_with_timestamp "Installing Python packages (ijson, pyarrow, geopandas, etc.)..."
pip install google-cloud-storage google-cloud-secret-manager paramiko ijson pyarrow uuid geopandas shapely pyproj

log_with_timestamp "✅ Python packages installed"
check_resources

# Create the enhanced transfer script with processing
cat > /opt/transfer_script.py << 'EOF'
#!/usr/bin/env python3

import os
import tempfile
import logging
import paramiko
import json
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import zipfile
import geopandas as gpd
from shapely.geometry import shape
from pyproj import Transformer, CRS
from datetime import datetime
from pathlib import Path
from google.cloud import storage, secretmanager
import subprocess
import sys
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def flush_logs():
    """Ensure stdout and stderr are flushed."""
    sys.stdout.flush()
    sys.stderr.flush()

class PropertyDataProcessor:
    """Handles privacy transformations and Parquet conversion."""

    def __init__(self):
        self.cpr_to_uuid_mapping = {}  # Cache for consistent UUID mapping
        # Default assumption: Danish data is typically in EPSG:25832 (UTM Zone 32N)
        # We'll convert to EPSG:4326 as required by README
        self.source_crs = 'EPSG:25832'  # Danish UTM Zone 32N (common for Danish official data)
        self.target_crs = 'EPSG:4326'   # WGS84 (required by README for Silver layer)
        self.crs_transformer = None
        self.crs_detection_done = False

    def detect_and_setup_crs_transformer(self, sample_geometry):
        """Detect CRS from sample geometry and setup transformer if needed."""
        if self.crs_detection_done:
            return

        # Try to detect CRS from coordinate ranges
        if isinstance(sample_geometry, dict) and 'coordinates' in sample_geometry:
            coords = sample_geometry['coordinates']

            # Flatten coordinates to check ranges
            flat_coords = []
            def flatten_coords(obj):
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, (list, tuple)):
                            flatten_coords(item)
                        elif isinstance(item, (int, float)):
                            flat_coords.append(item)

            flatten_coords(coords)

            if len(flat_coords) >= 2:
                # Check x,y ranges to guess CRS
                x_coords = flat_coords[::2]  # Every even index (x coordinates)
                y_coords = flat_coords[1::2]  # Every odd index (y coordinates)

                if x_coords and y_coords:
                    min_x, max_x = min(x_coords), max(x_coords)
                    min_y, max_y = min(y_coords), max(y_coords)

                    # Check if coordinates look like they're already in WGS84
                    if (-180 <= min_x <= 180) and (-90 <= min_y <= 90) and (-180 <= max_x <= 180) and (-90 <= max_y <= 90):
                        logger.info("Detected coordinates appear to be in WGS84 (EPSG:4326) - no transformation needed")
                        self.source_crs = 'EPSG:4326'
                    # Check if coordinates look like Danish UTM (typical ranges)
                    elif (400000 <= min_x <= 900000) and (6000000 <= min_y <= 6500000):
                        logger.info("Detected coordinates appear to be in Danish UTM Zone 32N (EPSG:25832)")
                        self.source_crs = 'EPSG:25832'
                    else:
                        logger.warning(f"Uncertain CRS detection. X range: {min_x:.0f}-{max_x:.0f}, Y range: {min_y:.0f}-{max_y:.0f}")
                        logger.warning(f"Assuming Danish UTM Zone 32N (EPSG:25832)")
                        self.source_crs = 'EPSG:25832'

        # Setup transformer if conversion is needed
        if self.source_crs != self.target_crs:
            logger.info(f"Setting up CRS transformation: {self.source_crs} → {self.target_crs}")
            self.crs_transformer = Transformer.from_crs(self.source_crs, self.target_crs, always_xy=True)
        else:
            logger.info("No CRS transformation needed - data already in EPSG:4326")
            self.crs_transformer = None

        self.crs_detection_done = True

    def transform_geometry_to_4326(self, geometry):
        """Transform geometry to EPSG:4326 if needed."""
        if not geometry:
            return geometry

        # Detect CRS on first geometry
        if not self.crs_detection_done:
            self.detect_and_setup_crs_transformer(geometry)

        # If no transformation needed, return as-is
        if not self.crs_transformer:
            return geometry

        try:
            # Convert to shapely geometry if it's a dict
            if isinstance(geometry, dict):
                geom = shape(geometry)
            else:
                geom = geometry

            # Transform coordinates based on geometry type
            if geom.geom_type == 'Point':
                x, y = self.crs_transformer.transform(geom.x, geom.y)
                return {'type': 'Point', 'coordinates': [x, y]}

            elif geom.geom_type == 'Polygon':
                def transform_coord_list(coords):
                    return [list(self.crs_transformer.transform(x, y)) for x, y in coords]

                exterior = transform_coord_list(geom.exterior.coords)
                holes = [transform_coord_list(hole.coords) for hole in geom.interiors]

                if holes:
                    return {'type': 'Polygon', 'coordinates': [exterior] + holes}
                else:
                    return {'type': 'Polygon', 'coordinates': [exterior]}

            elif geom.geom_type == 'MultiPolygon':
                transformed_coords = []
                for polygon in geom.geoms:
                    exterior = [list(self.crs_transformer.transform(x, y)) for x, y in polygon.exterior.coords]
                    holes = [[list(self.crs_transformer.transform(x, y)) for x, y in hole.coords] for hole in polygon.interiors]
                    if holes:
                        transformed_coords.append([exterior] + holes)
                    else:
                        transformed_coords.append([exterior])
                return {'type': 'MultiPolygon', 'coordinates': transformed_coords}

            else:
                logger.warning(f"Unsupported geometry type for transformation: {geom.geom_type}")
                return geometry

        except Exception as e:
            logger.warning(f"Failed to transform geometry: {e}")
            return geometry  # Return original if transformation fails

    def generate_uuid_for_cpr(self, cpr_id):
        """Generate consistent UUID for CPR numbers."""
        if not cpr_id:
            return None
        if cpr_id not in self.cpr_to_uuid_mapping:
            self.cpr_to_uuid_mapping[cpr_id] = str(uuid.uuid4())
        return self.cpr_to_uuid_mapping[cpr_id]

    def has_foreign_address(self, person_data):
        """Check if person has foreign address information."""
        if not person_data:
            return False
        udrejse_indrejse = person_data.get('UdrejseIndrejse', {})
        return bool(udrejse_indrejse.get('Udenlandsadresse') or
                   udrejse_indrejse.get('cprLandUdrejse') or
                   udrejse_indrejse.get('cprLandekodeUdrejse'))

    def transform_person_data(self, properties):
        """Apply privacy transformations to person ownership data."""
        if not properties or 'ejendePerson' not in properties:
            return properties

        person_data = properties['ejendePerson'].get('Person', {})
        if not person_data:
            return properties

        # Create transformed copy
        transformed = properties.copy()
        transformed_person = person_data.copy()

        # 1. Remove gender field
        if 'koen' in transformed_person:
            del transformed_person['koen']

        # 2. Replace CPR ID with UUID
        if 'id' in transformed_person:
            original_id = transformed_person['id']
            transformed_person['id'] = self.generate_uuid_for_cpr(original_id)

        # 3. Remove ALL personal address information (including municipality)
        if 'CprAdresse' in transformed_person:
            del transformed_person['CprAdresse']

        # Remove detailed address information
        if 'Adresseoplysninger' in transformed_person:
            del transformed_person['Adresseoplysninger']

        # 4. Add abroad flag
        transformed_person['lives_abroad'] = self.has_foreign_address(person_data)

        # 5. Keep privacy protection notice (Beskyttelser) as is
        # No changes needed - this stays

        # 6. Remove foreign address details but keep the flag
        if 'UdrejseIndrejse' in transformed_person:
            del transformed_person['UdrejseIndrejse']

        # 7. Remove birth date (potential CPR info)
        if 'foedselsdato' in transformed_person:
            del transformed_person['foedselsdato']
        if 'foedselsdatoUsikkerhedsmarkering' in transformed_person:
            del transformed_person['foedselsdatoUsikkerhedsmarkering']

        # Update the transformed properties
        transformed['ejendePerson']['Person'] = transformed_person

        return transformed

    def clean_empty_structures(self, obj):
        """Recursively remove empty dictionaries/structures that cause Parquet serialization issues."""
        if isinstance(obj, dict):
            cleaned = {}
            for key, value in obj.items():
                cleaned_value = self.clean_empty_structures(value)
                # Only keep non-empty values
                if cleaned_value or cleaned_value == 0 or cleaned_value == False or cleaned_value == "":
                    # Keep primitive values and non-empty containers
                    if not isinstance(cleaned_value, (dict, list)) or cleaned_value:
                        cleaned[key] = cleaned_value
            return cleaned
        elif isinstance(obj, list):
            return [self.clean_empty_structures(item) for item in obj if item is not None]
        else:
            return obj

    def normalize_schema(self, tables):
        """Normalize all table schemas to have the same fields before concatenation."""
        if not tables:
            return tables

        logger.info("Normalizing schemas across batch tables...")
        flush_logs()

        # Collect all unique field names and types from all schemas
        all_schemas = [table.schema for table in tables]
        unified_fields = {}

        for schema in all_schemas:
            for field in schema:
                field_name = field.name
                field_type = field.type
                if field_name in unified_fields:
                    # Keep the more complex type if there's a conflict
                    if str(field_type) != str(unified_fields[field_name]):
                        logger.info(f"Schema conflict for {field_name}: {unified_fields[field_name]} vs {field_type}")
                        # Keep the existing type for now
                else:
                    unified_fields[field_name] = field_type

        # Create unified schema
        unified_schema = pa.schema([pa.field(name, field_type) for name, field_type in unified_fields.items()])
        logger.info(f"Created unified schema with {len(unified_fields)} fields")
        flush_logs()

        # Normalize each table to match unified schema
        normalized_tables = []
        for i, table in enumerate(tables):
            logger.info(f"Normalizing table {i+1}/{len(tables)}")
            flush_logs()

            # Create columns dict from existing table
            columns_dict = {field.name: table.column(field.name) for field in table.schema}

            # Add missing columns with null values
            for field_name, field_type in unified_fields.items():
                if field_name not in columns_dict:
                    # Create null array of appropriate length
                    null_array = pa.nulls(len(table), field_type)
                    columns_dict[field_name] = null_array
                    logger.info(f"Added missing field '{field_name}' with nulls")

            # Create new table with unified schema column order
            ordered_columns = [columns_dict[field.name] for field in unified_schema]
            normalized_table = pa.table(ordered_columns, schema=unified_schema)
            normalized_tables.append(normalized_table)

            # Clear memory
            del columns_dict, table

        logger.info(f"Schema normalization complete for {len(normalized_tables)} tables")
        flush_logs()
        return normalized_tables

    def process_json_to_parquet(self, json_file_path, output_parquet_path):
        """Stream process JSON file and convert to Parquet with transformations using batch processing."""
        logger.info(f"Processing {json_file_path} to {output_parquet_path}")
        flush_logs()

        batch_records = []
        feature_count = 0
        batch_size = 500000  # Write every 500K features to avoid memory exhaustion
        batch_files = []
        output_dir = Path(output_parquet_path).parent

        try:
            with open(json_file_path, 'rb') as f:
                # Stream parse the GeoJSON features
                features = ijson.items(f, 'features.item')

                for feature in features:
                    # Clean empty structures that cause Parquet issues
                    cleaned_feature = self.clean_empty_structures(feature)

                    # Transform the feature
                    transformed_record = self.transform_feature(cleaned_feature)
                    batch_records.append(transformed_record)
                    feature_count += 1

                    # Log progress every 50K features
                    if feature_count % 50000 == 0:
                        logger.info(f"Processed {feature_count:,} features...")
                        flush_logs()

                    # Write batch when it reaches the batch size
                    if len(batch_records) >= batch_size:
                        batch_number = len(batch_files) + 1
                        batch_file = output_dir / f"batch_{batch_number:03d}.parquet"

                        logger.info(f"Writing batch {batch_number} ({len(batch_records):,} records) to {batch_file}")
                        flush_logs()

                        # Convert to PyArrow table and save
                        table = pa.Table.from_pylist(batch_records)
                        pq.write_table(table, batch_file)
                        batch_files.append(batch_file)

                        logger.info(f"Batch {batch_number} written, memory cleared")
                        flush_logs()

                        # Clear the batch to free memory
                        batch_records = []
                        del table

                # Write final batch if any records remain
                if batch_records:
                    batch_number = len(batch_files) + 1
                    batch_file = output_dir / f"batch_{batch_number:03d}.parquet"

                    logger.info(f"Writing final batch {batch_number} ({len(batch_records):,} records) to {batch_file}")
                    flush_logs()

                    table = pa.Table.from_pylist(batch_records)
                    pq.write_table(table, batch_file)
                    batch_files.append(batch_file)

                    logger.info(f"Final batch {batch_number} written")
                    flush_logs()

                    del batch_records, table

            logger.info(f"JSON parsing complete. Total features: {feature_count:,}")
            logger.info(f"Merging {len(batch_files)} batch files into {output_parquet_path}")
            flush_logs()

            # Read all batch files
            all_tables = []
            for batch_file in batch_files:
                table = pq.read_table(batch_file)
                all_tables.append(table)

            # Normalize schemas before concatenation
            normalized_tables = self.normalize_schema(all_tables)

            # Concatenate all tables
            final_table = pa.concat_tables(normalized_tables)

            # Write the final merged file
            logger.info(f"Writing final merged file with {len(final_table)} records")
            flush_logs()

            pq.write_table(final_table, output_parquet_path)

            # Clean up batch files
            logger.info("Cleaning up batch files...")
            for batch_file in batch_files:
                batch_file.unlink()

            logger.info(f"Successfully created {output_parquet_path} with {len(final_table):,} records")
            flush_logs()

        except Exception as e:
            logger.error(f"Error processing JSON file: {e}")
            # Clean up any partial batch files
            for batch_file in batch_files:
                if batch_file.exists():
                    batch_file.unlink()
            raise

    def transform_feature(self, feature):
        """Apply privacy transformations and CRS transformation to a GeoJSON feature."""
        if not feature:
            return feature

        # Create a copy to avoid modifying the original
        transformed_feature = feature.copy()

        # Transform properties (privacy transformations)
        if 'properties' in feature:
            transformed_feature['properties'] = self.transform_person_data(feature['properties'])

        # Transform geometry to EPSG:4326 if needed (as required by README)
        if 'geometry' in feature:
            transformed_feature['geometry'] = self.transform_geometry_to_4326(feature['geometry'])

        return transformed_feature

class SFTPToGCSTransferWithProcessing:
    def __init__(self):
        self.project_id = "landbrugsdata-1"
        self.bucket_name = "landbrugsdata-raw-data"
        self.storage_client = storage.Client()
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.processor = PropertyDataProcessor()
        logger.info("SFTPToGCSTransferWithProcessing initialized.")
        flush_logs()

    def get_secret(self, secret_name):
        logger.debug(f"Attempting to get secret: {secret_name}")
        flush_logs()
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        try:
            response = self.secret_client.access_secret_version(name=name)
            secret_value = response.payload.data.decode('UTF-8').strip()
            logger.debug(f"Successfully retrieved secret: {secret_name}")
            flush_logs()
            return secret_value
        except Exception as e:
            logger.error(f"Failed to retrieve secret: {secret_name}. Error: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

    def get_sftp_client(self):
        logger.info("Attempting to create SFTP client...")
        flush_logs()
        try:
            host = self.get_secret("datafordeler-sftp-host")
            username = self.get_secret("datafordeler-sftp-username")
            private_key_data = self.get_secret("ssh-brugerdatafordeleren")

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            import socket
            import time

            def resolve_hostname_with_retry(hostname, max_attempts=5, delay=10):
                logger.info(f"Starting DNS resolution for {hostname}.")
                flush_logs()
                for attempt in range(1, max_attempts + 1):
                    try:
                        logger.debug(f"DNS RESOLUTION ATTEMPT {attempt}/{max_attempts} for {hostname}")
                        flush_logs()

                        logger.debug(f"Attempting Python DNS resolution for {hostname}")
                        flush_logs()
                        try:
                            logger.debug("Trying socket.getaddrinfo...")
                            flush_logs()
                            result = socket.getaddrinfo(hostname, None, socket.AF_INET)
                            ip = result[0][4][0]
                            logger.info(f"DNS resolution successful: {hostname} -> {ip}")
                            flush_logs()
                            return ip
                        except Exception as e:
                            logger.warning(f"getaddrinfo failed for {hostname}: {e}")
                            flush_logs()

                    except socket.gaierror as e:
                        logger.warning(f"DNS resolution attempt {attempt} for {hostname} failed with gaierror: {e}")

                        if attempt < max_attempts:
                            logger.info(f"Waiting {delay} seconds before DNS retry for {hostname} (attempt {attempt+1}/{max_attempts})...")
                            flush_logs()
                            time.sleep(delay)
                        else:
                            logger.error(f"All DNS resolution attempts failed for {hostname}")
                            flush_logs()
                            raise
                    except Exception as e:
                        logger.error(f"Unexpected error in DNS resolution attempt {attempt}: {e}")
                        logger.error(traceback.format_exc())
                        flush_logs()
                        if attempt < max_attempts:
                            logger.info(f"Waiting {delay} seconds before retry...")
                            flush_logs()
                            time.sleep(delay)
                        else:
                            logger.error(f"All DNS resolution attempts failed for {hostname}")
                            flush_logs()
                            raise

            host_ip = resolve_hostname_with_retry(host)

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.key') as key_file:
                key_file.write(private_key_data)
                if not private_key_data.endswith('\n'):
                    key_file.write('\n')
                key_file.flush()
                key_file_path = key_file.name
                logger.debug(f"Private key written to temporary file: {key_file_path}")
                flush_logs()

                try:
                    private_key = paramiko.RSAKey.from_private_key_file(key_file_path)
                    logger.info(f"Attempting SSH connection to {host_ip} (user: {username})...")
                    flush_logs()
                    ssh.connect(
                        hostname=host_ip, port=22, username=username, pkey=private_key,
                        timeout=60, banner_timeout=30, auth_timeout=30,
                        allow_agent=False, look_for_keys=False
                    )
                    logger.info("SSH connection established successfully.")
                    flush_logs()

                    sftp = ssh.open_sftp()
                    logger.info("SFTP session opened successfully.")
                    flush_logs()
                    return sftp
                except Exception as e:
                    logger.error(f"SSH/SFTP connection failed: {e}")
                    logger.error(traceback.format_exc())
                    flush_logs()
                    raise
                finally:
                    logger.info(f"Deleting temporary key file: {key_file_path}")
                    flush_logs()
                    os.unlink(key_file_path)

        except Exception as e:
            logger.error(f"Failed to connect to SFTP: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

    def find_latest_file(self, sftp):
        logger.info("Finding latest .zip file on SFTP server...")
        flush_logs()
        latest_file = None
        latest_time = None

        try:
            for entry in sftp.listdir_attr('.'):
                logger.debug(f"SFTP entry: {entry.filename}, mtime: {entry.st_mtime}")
                flush_logs()
                if not entry.longname.startswith('d') and entry.filename.endswith('.zip'):
                    if latest_time is None or entry.st_mtime > latest_time:
                        latest_file = entry.filename
                        latest_time = entry.st_mtime
                        logger.debug(f"New latest candidate: {latest_file} (mtime: {latest_time})")
                        flush_logs()

            if latest_file is None:
                logger.error("No .zip files found on SFTP server.")
                flush_logs()
                raise FileNotFoundError("No .zip files found on SFTP server")

            logger.info(f"Found latest file: {latest_file}")
            flush_logs()
            return latest_file
        except Exception as e:
            logger.error(f"Error finding latest file: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

    def transfer_and_process_file(self):
        logger.info("Starting file transfer and processing...")
        flush_logs()
        sftp = None
        try:
            sftp = self.get_sftp_client()
            latest_file = self.find_latest_file(sftp)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            with tempfile.TemporaryDirectory() as tmpdir:
                # Download ZIP
                local_zip_path = Path(tmpdir) / latest_file
                logger.info(f"Downloading {latest_file}...")
                flush_logs()
                sftp.get(latest_file, str(local_zip_path))
                logger.info(f"Download complete. File size: {local_zip_path.stat().st_size / (1024**3):.1f} GB")
                flush_logs()

                # Extract JSON from ZIP
                json_file_path = None
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    for file_info in zip_ref.filelist:
                        if file_info.filename.endswith('.json') and not file_info.filename.endswith('_Metadata.json'):
                            json_file_path = Path(tmpdir) / file_info.filename
                            logger.info(f"Extracting {file_info.filename}...")
                            flush_logs()
                            zip_ref.extract(file_info.filename, tmpdir)
                            break

                if not json_file_path:
                    raise FileNotFoundError("No JSON file found in ZIP")

                logger.info(f"Extracted JSON file: {json_file_path} ({json_file_path.stat().st_size / (1024**3):.1f} GB)")
                flush_logs()

                # Process JSON to Parquet
                parquet_file_path = Path(tmpdir) / f"property_owners_{timestamp}.parquet"
                logger.info("Starting JSON to Parquet processing with privacy transformations...")
                flush_logs()

                self.processor.process_json_to_parquet(str(json_file_path), str(parquet_file_path))

                # Clean up JSON file immediately to save disk space (14GB SSD is limited)
                logger.info(f"Removing JSON file to save disk space: {json_file_path}")
                flush_logs()
                json_file_path.unlink()

                logger.info(f"Processing complete. Parquet file size: {parquet_file_path.stat().st_size / (1024**2):.1f} MB")
                flush_logs()

                # Upload Parquet to GCS
                gcs_filename = f"silver/property_owners/{timestamp}_property_owners_processed.parquet"
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob(gcs_filename)

                logger.info(f"Uploading to GCS: gs://{self.bucket_name}/{gcs_filename}")
                flush_logs()
                blob.upload_from_filename(str(parquet_file_path))
                logger.info("GCS upload complete.")
                flush_logs()

                # Also upload original ZIP for backup
                original_gcs_filename = f"bronze/property_owners/{timestamp}_{latest_file}"
                original_blob = bucket.blob(original_gcs_filename)
                logger.info(f"Uploading original ZIP to: gs://{self.bucket_name}/{original_gcs_filename}")
                flush_logs()
                original_blob.upload_from_filename(str(local_zip_path))
                logger.info("Original ZIP backup upload complete.")
                flush_logs()

            logger.info("Transfer and processing completed successfully.")
            flush_logs()

        except Exception as e:
            logger.error(f"Transfer and processing failed: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise
        finally:
            if sftp:
                try:
                    sftp.close()
                    logger.info("SFTP connection closed.")
                    flush_logs()
                except Exception as e:
                    logger.warning(f"Error closing SFTP: {e}")
                    flush_logs()

def main_transfer_and_shutdown():
    logger.info("=== Starting main_transfer_and_shutdown ===")
    flush_logs()

    try:
        transfer_instance = SFTPToGCSTransferWithProcessing()
        transfer_instance.transfer_and_process_file()
        logger.info("Transfer and processing successful.")
        flush_logs()
        success = True
    except Exception as e:
        logger.error(f"Transfer and processing failed: {e}")
        logger.error(traceback.format_exc())
        flush_logs()
        success = False

    logger.info(f"Process success status: {success}")
    flush_logs()

    return success

if __name__ == "__main__":
    logger.info("=== Starting Property Owners SFTP Transfer with Processing ===")
    flush_logs()

    success = main_transfer_and_shutdown()

    logger.info("Python script finished.")
    flush_logs()

EOF

chmod +x /opt/transfer_script.py

# Get current VM metadata for self-deletion
VM_NAME=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
ZONE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
PROJECT_ID=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)

# Function to delete the VM
delete_vm() {
  log_with_timestamp "Attempting to delete VM: $VM_NAME in zone: $ZONE project: $PROJECT_ID"
  sync
  gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --quiet
  log_with_timestamp "gcloud delete command executed for $VM_NAME."
  sync
}

# Function to validate success
validate_success() {
    log_with_timestamp "=== VALIDATING PROCESSING SUCCESS ==="

    # Check if silver directory was created
    if gsutil ls gs://landbrugsdata-raw-data/silver/property_owners/ >/dev/null 2>&1; then
        log_with_timestamp "✅ Silver directory exists"

        # Check if parquet file was created in last hour
        recent_files=$(gsutil ls -l gs://landbrugsdata-raw-data/silver/property_owners/*.parquet 2>/dev/null | grep "$(date +%Y-%m-%d)" | wc -l)
        if [ "$recent_files" -gt 0 ]; then
            log_with_timestamp "✅ Recent Parquet file found in silver directory"
            return 0
        else
            log_with_timestamp "❌ No recent Parquet files found in silver directory"
            return 1
        fi
    else
        log_with_timestamp "❌ Silver property_owners directory does not exist"
        return 1
    fi
}

# Run the enhanced transfer script
log_with_timestamp "Executing enhanced transfer script with processing..."
sync

if python3 /opt/transfer_script.py; then
    log_with_timestamp "✅ Python script completed successfully"

    # Validate actual success
    if validate_success; then
        log_with_timestamp "✅ SUCCESS: Processing validated - files uploaded successfully"
        log_with_timestamp "VM will self-delete in 2 minutes to allow log inspection."
        sleep 120
        delete_vm
    else
        log_with_timestamp "❌ FAILURE: Processing validation failed - no output files found"
        log_with_timestamp "VM will NOT self-delete to allow debugging"
        exit 1
    fi
else
    log_with_timestamp "❌ Python script failed with exit code $?"
    log_with_timestamp "VM will NOT self-delete to allow debugging"
    exit 1
fi

log_with_timestamp "SFTP to GCS transfer with processing finished at $(date)."
sync
log_with_timestamp "Script finished successfully."
exit 0
