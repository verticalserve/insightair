import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from .base import StorageClient
from utils.logging import get_logger

log = get_logger(__name__)

class S3Client(StorageClient):
    def __init__(self, aws_conn_kwargs: dict = None):
        """
        aws_conn_kwargs: dict passed to boto3.client('s3', **aws_conn_kwargs)
        """
        self.s3 = boto3.client('s3', **(aws_conn_kwargs or {}))

    def get_object_contents(self, bucket: str, key: str) -> str:
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        data = resp['Body'].read().decode('utf-8')
        log.info(f"Read object s3://{bucket}/{key}, size={len(data)} bytes")
        return data

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        keys = []
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                keys.append(obj['Key'])
        log.info(f"Listed {len(keys)} objects under s3://{bucket}/{prefix}")
        return keys

    def copy_folder(
        self,
        src_bucket: str,
        src_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        force_recopy: bool = True
    ) -> None:
        keys = self.list_objects(src_bucket, src_prefix)
        for key in keys:
            rel_path = key[len(src_prefix):].lstrip('/')
            dest_key = f"{dest_prefix.rstrip('/')}/{rel_path}"
            if not force_recopy:
                try:
                    self.s3.head_object(Bucket=dest_bucket, Key=dest_key)
                    log.debug(f"Skipping existing s3://{dest_bucket}/{dest_key}")
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] != '404':
                        raise
            copy_source = {'Bucket': src_bucket, 'Key': key}
            self.s3.copy_object(
                Bucket=dest_bucket,
                CopySource=copy_source,
                Key=dest_key
            )
            log.info(f"Copied s3://{src_bucket}/{key} → s3://{dest_bucket}/{dest_key}")

    def list_partition_folders(
        self,
        bucket: str,
        prefix: str,
        from_slice_prefix: str,
        to_slice_prefix: str
    ) -> list[str]:
        # Use delimiter to get common prefixes (subfolders)
        resp = self.s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix.rstrip('/') + '/',
            Delimiter='/'
        )
        prefixes = [cp['Prefix'] for cp in resp.get('CommonPrefixes', [])]
        # Filter by lexicographic range
        filtered = [
            p for p in prefixes
            if from_slice_prefix <= p.split('/')[-2] <= to_slice_prefix
        ]
        log.info(f"Found {len(filtered)} partition folders in s3://{bucket}/{prefix}")
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
                self.s3.head_object(Bucket=bucket, Key=key)
                log.debug(f"Skipping write, object exists s3://{bucket}/{key}")
                return
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise
        self.s3.put_object(Bucket=bucket, Key=key, Body=data_str.encode('utf-8'))
        log.info(f"Wrote object s3://{bucket}/{key}, size={len(data_str)} bytes")

    def get_object_modify_date(self, bucket: str, key: str) -> datetime:
        resp = self.s3.head_object(Bucket=bucket, Key=key)
        mod_date = resp['LastModified']
        log.info(f"Object s3://{bucket}/{key} last modified at {mod_date.isoformat()}")
        return mod_date
