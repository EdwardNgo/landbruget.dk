import os
import json
from google.cloud import storage

class StorageInterface:
    """Interface for saving JSON data to different storage backends."""
    def save_json(self, data, dst_path):
        raise NotImplementedError("save_json must be implemented by subclasses")

class LocalStorage(StorageInterface):
    """Save JSON files to the local filesystem."""
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def save_json(self, data, dst_path):
        full_path = os.path.join(self.base_dir, dst_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

class GCSStorage(StorageInterface):
    """Save JSON files to a Google Cloud Storage bucket."""
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def save_json(self, data, dst_path):
        blob = self.bucket.blob(dst_path)
        blob.upload_from_string(json.dumps(data), content_type="application/json")
