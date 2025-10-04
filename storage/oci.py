import oci
from datetime import datetime
from email.utils import parsedate_to_datetime
from .base import StorageClient
from utils.logging import get_logger

log = get_logger(__name__)

class OCIStorageClient(StorageClient):
    """
    OCI Object Storage implementation of StorageClient.
    """

    def __init__(self, oci_config: dict = None):
        # Load OCI config: default, file path, or dict
        if oci_config is None:
            cfg = oci.config.from_file()
        elif isinstance(oci_config, str):
            cfg = oci.config.from_file(oci_config)
        elif "config_file" in oci_config:
            cfg = oci.config.from_file(
                oci_config["config_file"],
                oci_config.get("profile", "DEFAULT")
            )
        else:
            cfg = oci_config

        self.client = oci.object_storage.ObjectStorageClient(cfg)
        self.namespace = self.client.get_namespace().data

    def get_object_contents(self, bucket: str, key: str) -> str:
        resp = self.client.get_object(self.namespace, bucket, key)
        try:
            data_bytes = resp.data.read()
        except AttributeError:
            data_bytes = resp.data.raw.read()
        text = data_bytes.decode("utf-8")
        log.info(f"Read object oci://{bucket}/{key}, size={len(text)} bytes")
        return text

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        keys = []
        resp = self.client.list_objects(
            namespace_name=self.namespace,
            bucket_name=bucket,
            prefix=prefix
        )
        keys.extend([obj.name for obj in resp.data.objects])
        next_page = resp.headers.get("opc-next-page")
        while next_page:
            resp = self.client.list_objects(
                namespace_name=self.namespace,
                bucket_name=bucket,
                prefix=prefix,
                page=next_page
            )
            keys.extend([obj.name for obj in resp.data.objects])
            next_page = resp.headers.get("opc-next-page")
        log.info(f"Listed {len(keys)} objects under oci://{bucket}/{prefix}")
        return keys

    def copy_folder(
        self,
        src_bucket: str,
        src_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        force_recopy: bool = True
    ) -> None:
        from oci.object_storage.models import CopyObjectDetails
        keys = self.list_objects(src_bucket, src_prefix)
        for key in keys:
            rel = key[len(src_prefix):].lstrip("/")
            dest_key = f"{dest_prefix.rstrip('/')}/{rel}"
            if not force_recopy:
                try:
                    self.client.head_object(
                        namespace_name=self.namespace,
                        bucket_name=dest_bucket,
                        object_name=dest_key
                    )
                    log.debug(f"Skipping existing oci://{dest_bucket}/{dest_key}")
                    continue
                except oci.exceptions.ServiceError as e:
                    if e.status != 404:
                        raise
            details = CopyObjectDetails(
                destination_namespace=self.namespace,
                destination_bucket=dest_bucket,
                destination_object=dest_key
            )
            self.client.copy_object(
                namespace_name=self.namespace,
                bucket_name=src_bucket,
                object_name=key,
                copy_object_details=details
            )
            log.info(f"Copied oci://{src_bucket}/{key} → oci://{dest_bucket}/{dest_key}")

    def list_partition_folders(
        self,
        bucket: str,
        prefix: str,
        from_slice_prefix: str,
        to_slice_prefix: str
    ) -> list[str]:
        resp = self.client.list_objects(
            namespace_name=self.namespace,
            bucket_name=bucket,
            prefix=prefix.rstrip("/") + "/",
            delimiter="/"
        )
        prefixes = resp.data.prefixes or []
        filtered = [
            p for p in prefixes
            if from_slice_prefix <= p.split("/")[-2] <= to_slice_prefix
        ]
        log.info(f"Found {len(filtered)} partition folders in oci://{bucket}/{prefix}")
        return filtered

    def write_object(
        self,
        bucket: str,
        key: str,
        data_str: str,
        force_recopy: bool = True
    ) -> None:
        if not force_recopy:
            try:
                self.client.head_object(
                    namespace_name=self.namespace,
                    bucket_name=bucket,
                    object_name=key
                )
                log.debug(f"Skipping write, exists oci://{bucket}/{key}")
                return
            except oci.exceptions.ServiceError as e:
                if e.status != 404:
                    raise
        self.client.put_object(
            namespace_name=self.namespace,
            bucket_name=bucket,
            object_name=key,
            put_object_body=data_str.encode("utf-8")
        )
        log.info(f"Wrote object oci://{bucket}/{key}, size={len(data_str)} bytes")

    def get_object_modify_date(self, bucket: str, key: str) -> datetime:
        resp = self.client.head_object(
            namespace_name=self.namespace,
            bucket_name=bucket,
            object_name=key
        )
        last_mod = resp.headers.get("last-modified")
        if last_mod:
            mod_date = parsedate_to_datetime(last_mod)
        else:
            mod_date = resp.data.time_created
        log.info(f"Object oci://{bucket}/{key} last modified at {mod_date.isoformat()}")
        return mod_date
