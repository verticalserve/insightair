# airflow_utils/storage/gcp.py

from google.cloud import storage
from datetime import datetime
from .base import StorageClient
from utils.logging import get_logger

log = get_logger(__name__)

class GCPStorageClient(StorageClient):
    """
    GCP Cloud Storage implementation of StorageClient.
    """

    def __init__(self, gcp_config: dict = None):
        """
        gcp_config: optional dict to pass to storage.Client (e.g. project, credentials)
        """
        self.client = storage.Client(**(gcp_config or {}))

    def get_object_contents(self, bucket: str, key: str) -> str:
        bucket_obj = self.client.bucket(bucket)
        blob = bucket_obj.blob(key)
        data = blob.download_as_text(encoding="utf-8")
        log.info(f"Read object gs://{bucket}/{key}, size={len(data)} bytes")
        return data

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        bucket_obj = self.client.bucket(bucket)
        blobs = bucket_obj.list_blobs(prefix=prefix)
        keys = [b.name for b in blobs]
        log.info(f"Listed {len(keys)} objects under gs://{bucket}/{prefix}")
        return keys

    def copy_folder(
        self,
        src_bucket: str,
        src_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        force_recopy: bool = True
    ) -> None:
        src = self.client.bucket(src_bucket)
        dest = self.client.bucket(dest_bucket)
        for blob in src.list_blobs(prefix=src_prefix):
            rel_path = blob.name[len(src_prefix):].lstrip("/")
            dest_name = f"{dest_prefix.rstrip('/')}/{rel_path}"
            dest_blob = dest.blob(dest_name)
            if not force_recopy and dest_blob.exists():
                log.debug(f"Skipping existing gs://{dest_bucket}/{dest_name}")
                continue
            src.copy_blob(blob, dest, dest_name)
            log.info(f"Copied gs://{src_bucket}/{blob.name} → gs://{dest_bucket}/{dest_name}")

    def list_partition_folders(
        self,
        bucket: str,
        prefix: str,
        from_slice_prefix: str,
        to_slice_prefix: str
    ) -> list[str]:
        bucket_obj = self.client.bucket(bucket)
        iterator = bucket_obj.list_blobs(prefix=prefix.rstrip("/") + "/", delimiter="/")
        # prefixes attribute holds subfolder prefixes under delimiter
        prefixes = list(iterator.prefixes)
        filtered = [
            p for p in prefixes
            if from_slice_prefix <= p.split("/")[-2] <= to_slice_prefix
        ]
        log.info(f"Found {len(filtered)} partition folders in gs://{bucket}/{prefix}")
        return filtered

    def write_object(
        self,
        bucket: str,
        key: str,
        data_str: str,
        force_recopy: bool = True
    ) -> None:
        bucket_obj = self.client.bucket(bucket)
        blob = bucket_obj.blob(key)
        if not force_recopy and blob.exists():
            log.debug(f"Skipping write, exists gs://{bucket}/{key}")
            return
        blob.upload_from_string(data_str, content_type="text/plain")
        log.info(f"Wrote object gs://{bucket}/{key}, size={len(data_str)} bytes")

    def get_object_modify_date(self, bucket: str, key: str) -> datetime:
        bucket_obj = self.client.bucket(bucket)
        blob = bucket_obj.get_blob(key)
        mod_date = blob.updated
        log.info(f"Object gs://{bucket}/{key} last modified at {mod_date.isoformat()}")
        return mod_date
