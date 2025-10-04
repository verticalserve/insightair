# airflow_utils/storage/azure.py

from azure.storage.blob import BlobServiceClient
from datetime import datetime
from .base import StorageClient
from utils.logging import get_logger

log = get_logger(__name__)

class AzureStorageClient(StorageClient):
    """
    Azure Blob Storage implementation of StorageClient.
    """

    def __init__(self, azure_config: dict):
        """
        azure_config: dict that must contain either:
          - connection_string
          - or account_url & credential
        """
        if "connection_string" in azure_config:
            self.client = BlobServiceClient.from_connection_string(
                azure_config["connection_string"]
            )
        else:
            self.client = BlobServiceClient(
                account_url=azure_config["account_url"],
                credential=azure_config["credential"]
            )

    def get_object_contents(self, bucket: str, key: str) -> str:
        container = self.client.get_container_client(bucket)
        blob = container.get_blob_client(key)
        data = blob.download_blob().content_as_text()
        log.info(f"Read blob azure://{bucket}/{key}, size={len(data)} bytes")
        return data

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        container = self.client.get_container_client(bucket)
        blobs = container.list_blobs(name_starts_with=prefix)
        keys = [b.name for b in blobs]
        log.info(f"Listed {len(keys)} blobs under azure://{bucket}/{prefix}")
        return keys

    def copy_folder(
        self,
        src_bucket: str,
        src_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        force_recopy: bool = True
    ) -> None:
        src_ctr = self.client.get_container_client(src_bucket)
        dest_ctr = self.client.get_container_client(dest_bucket)
        for blob_props in src_ctr.list_blobs(name_starts_with=src_prefix):
            rel = blob_props.name[len(src_prefix):].lstrip("/")
            dest_name = f"{dest_prefix.rstrip('/')}/{rel}"
            dest_blob = dest_ctr.get_blob_client(dest_name)
            if not force_recopy:
                try:
                    props = dest_blob.get_blob_properties()
                    log.debug(f"Skipping existing azure://{dest_bucket}/{dest_name}")
                    continue
                except Exception:
                    pass
            src_blob_url = src_ctr.get_blob_client(blob_props.name).url
            dest_blob.start_copy_from_url(src_blob_url)
            log.info(f"Copied azure://{src_bucket}/{blob_props.name} → azure://{dest_bucket}/{dest_name}")

    def list_partition_folders(
        self,
        bucket: str,
        prefix: str,
        from_slice_prefix: str,
        to_slice_prefix: str
    ) -> list[str]:
        container = self.client.get_container_client(bucket)
        blobs = container.list_blobs(name_starts_with=prefix.rstrip("/") + "/")
        # extract unique immediate subfolders
        folders = set()
        base_len = len(prefix.rstrip("/")) + 1
        for b in blobs:
            remainder = b.name[base_len:]
            parts = remainder.split("/", 1)
            if len(parts) > 1:
                folders.add(parts[0] + "/")
        filtered = [
            f for f in folders
            if from_slice_prefix <= f.rstrip("/") <= to_slice_prefix
        ]
        log.info(f"Found {len(filtered)} partition folders in azure://{bucket}/{prefix}")
        return sorted(filtered)

    def write_object(
        self,
        bucket: str,
        key: str,
        data_str: str,
        force_recopy: bool = True
    ) -> None:
        container = self.client.get_container_client(bucket)
        blob = container.get_blob_client(key)
        if not force_recopy:
            try:
                blob.get_blob_properties()
                log.debug(f"Skipping write, exists azure://{bucket}/{key}")
                return
            except Exception:
                pass
        blob.upload_blob(data_str, overwrite=True)
        log.info(f"Wrote object azure://{bucket}/{key}, size={len(data_str)} bytes")

    def get_object_modify_date(self, bucket: str, key: str) -> datetime:
        blob = self.client.get_container_client(bucket).get_blob_client(key)
        props = blob.get_blob_properties()
        mod_date = props.last_modified
        log.info(f"Blob azure://{bucket}/{key} last modified at {mod_date.isoformat()}")
        return mod_date
