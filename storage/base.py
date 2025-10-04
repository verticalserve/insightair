# airflow_utils/storage/base.py

```python
from abc import ABC, abstractmethod
from typing import List
from datetime import datetime

class StorageClient(ABC):
    """
    Abstract base class for object storage operations.
    """

    @abstractmethod
    def get_object_contents(self, bucket: str, key: str) -> str:
        """
        Read and return the contents of an object.
        """
        pass

    @abstractmethod
    def list_objects(self, bucket: str, prefix: str) -> List[str]:
        """
        List all object keys under the given prefix.
        """
        pass

    @abstractmethod
    def copy_folder(
        self,
        src_bucket: str,
        src_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        force_recopy: bool = True
    ) -> None:
        """
        Copy all objects from src_prefix to dest_prefix,
        optionally skipping existing objects.
        """
        pass

    @abstractmethod
    def list_partition_folders(
        self,
        bucket: str,
        prefix: str,
        from_slice_prefix: str,
        to_slice_prefix: str
    ) -> List[str]:
        """
        List subfolder names under prefix filtered between
        from_slice_prefix and to_slice_prefix.
        """
        pass

    @abstractmethod
    def write_object(
        self,
        bucket: str,
        key: str,
        data_str: str,
        force_recopy: bool = True
    ) -> None:
        """
        Upload a string as an object, optionally skipping if exists.
        """
        pass

    @abstractmethod
    def get_object_modify_date(self, bucket: str, key: str) -> datetime:
        """
        Retrieve the last modified timestamp of the object.
        """
        pass
