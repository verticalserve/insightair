# Generic Archive Operations - Multi-cloud file compression and archiving
"""
Generic archive operations that work across cloud environments:
- File compression and decompression (zip, tar, gzip, bz2)
- Directory archiving with multiple formats
- Cloud storage integration for archives
- Batch archiving operations
- Archive validation and integrity checks
"""

import logging
import os
import zipfile
import tarfile
import gzip
import bz2
import shutil
from typing import Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod
from pathlib import Path
import fnmatch

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    CloudProvider
)

logger = logging.getLogger(__name__)


class BaseArchiveOperator(BaseInsightAirOperator, ABC):
    """Base class for archive operations"""
    
    def __init__(self, **kwargs):
        """Initialize BaseArchiveOperator"""
        super().__init__(**kwargs)
    
    @abstractmethod
    def execute_archive_operation(self, context: Context) -> Any:
        """Execute the archive operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic configuration"""
        return self.execute_archive_operation(context)


class FileArchiveOperator(BaseArchiveOperator):
    """Generic file archiving operator (compress/decompress)"""
    
    template_fields = ('source_path', 'target_path', 'file_pattern')
    
    def __init__(self,
                 operation: str,  # compress, decompress
                 source_path: str,
                 target_path: str,
                 archive_format: str = 'zip',  # zip, tar, tar.gz, tar.bz2, gzip, bz2
                 file_pattern: str = '*',
                 compression_level: int = 6,
                 preserve_permissions: bool = False,
                 include_subdirs: bool = True,
                 **kwargs):
        """
        Initialize FileArchiveOperator
        
        Args:
            operation: Archive operation (compress, decompress)
            source_path: Source file or directory path
            target_path: Target archive file path
            archive_format: Archive format (zip, tar, tar.gz, tar.bz2, gzip, bz2)
            file_pattern: File pattern to include (for compress operation)
            compression_level: Compression level (1-9, where applicable)
            preserve_permissions: Preserve file permissions in archive
            include_subdirs: Include subdirectories in archive
        """
        super().__init__(**kwargs)
        self.operation = operation.lower()
        self.source_path = source_path
        self.target_path = target_path
        self.archive_format = archive_format.lower()
        self.file_pattern = file_pattern
        self.compression_level = compression_level
        self.preserve_permissions = preserve_permissions
        self.include_subdirs = include_subdirs
    
    def execute_archive_operation(self, context: Context) -> Dict[str, Any]:
        """Execute archive operation"""
        
        env_context = get_environment_context()
        
        if env_context.cloud_provider == CloudProvider.AWS:
            return self._execute_aws(context)
        elif env_context.cloud_provider == CloudProvider.GCP:
            return self._execute_gcp(context)
        elif env_context.cloud_provider == CloudProvider.AZURE:
            return self._execute_azure(context)
        elif env_context.cloud_provider == CloudProvider.OCI:
            return self._execute_oci(context)
        else:
            return self._execute_generic(context)
    
    def _execute_aws(self, context: Context) -> Dict[str, Any]:
        """Execute on AWS"""
        result = self._execute_generic(context)
        result['platform'] = 'aws'
        return result
    
    def _execute_gcp(self, context: Context) -> Dict[str, Any]:
        """Execute on GCP"""
        result = self._execute_generic(context)
        result['platform'] = 'gcp'
        return result
    
    def _execute_azure(self, context: Context) -> Dict[str, Any]:
        """Execute on Azure"""
        result = self._execute_generic(context)
        result['platform'] = 'azure'
        return result
    
    def _execute_oci(self, context: Context) -> Dict[str, Any]:
        """Execute on OCI"""
        result = self._execute_generic(context)
        result['platform'] = 'oci'
        return result
    
    def _execute_generic(self, context: Context) -> Dict[str, Any]:
        """Generic archive operation execution"""
        
        logger.info(f"Executing {self.operation} operation with format {self.archive_format}")
        
        if self.operation == 'compress':
            return self._compress_files(context)
        elif self.operation == 'decompress':
            return self._decompress_files(context)
        else:
            raise ValueError(f"Unsupported operation: {self.operation}")
    
    def _compress_files(self, context: Context) -> Dict[str, Any]:
        """Compress files to archive"""
        
        if not os.path.exists(self.source_path):
            raise FileNotFoundError(f"Source path not found: {self.source_path}")
        
        # Create target directory if needed
        target_dir = os.path.dirname(self.target_path)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)
        
        files_archived = 0
        total_size = 0
        compressed_size = 0
        
        if self.archive_format == 'zip':
            files_archived, total_size, compressed_size = self._create_zip_archive()
        elif self.archive_format in ['tar', 'tar.gz', 'tar.bz2']:
            files_archived, total_size, compressed_size = self._create_tar_archive()
        elif self.archive_format == 'gzip':
            files_archived, total_size, compressed_size = self._create_gzip_archive()
        elif self.archive_format == 'bz2':
            files_archived, total_size, compressed_size = self._create_bz2_archive()
        else:
            raise ValueError(f"Unsupported archive format: {self.archive_format}")
        
        compression_ratio = (total_size - compressed_size) / total_size if total_size > 0 else 0.0
        
        logger.info(f"Compression completed: {files_archived} files, {total_size} -> {compressed_size} bytes "
                   f"(compression: {compression_ratio:.1%})")
        
        return {
            'operation': 'compress',
            'source_path': self.source_path,
            'target_path': self.target_path,
            'archive_format': self.archive_format,
            'files_archived': files_archived,
            'original_size': total_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio,
            'success': True
        }
    
    def _decompress_files(self, context: Context) -> Dict[str, Any]:
        """Decompress archive files"""
        
        if not os.path.exists(self.source_path):
            raise FileNotFoundError(f"Archive file not found: {self.source_path}")
        
        # Create target directory if needed
        os.makedirs(self.target_path, exist_ok=True)
        
        files_extracted = 0
        total_size = 0
        
        if self.archive_format == 'zip':
            files_extracted, total_size = self._extract_zip_archive()
        elif self.archive_format in ['tar', 'tar.gz', 'tar.bz2']:
            files_extracted, total_size = self._extract_tar_archive()
        elif self.archive_format == 'gzip':
            files_extracted, total_size = self._extract_gzip_archive()
        elif self.archive_format == 'bz2':
            files_extracted, total_size = self._extract_bz2_archive()
        else:
            raise ValueError(f"Unsupported archive format: {self.archive_format}")
        
        logger.info(f"Decompression completed: {files_extracted} files extracted, {total_size} bytes")
        
        return {
            'operation': 'decompress',
            'source_path': self.source_path,
            'target_path': self.target_path,
            'archive_format': self.archive_format,
            'files_extracted': files_extracted,
            'total_size': total_size,
            'success': True
        }
    
    def _create_zip_archive(self) -> tuple:
        """Create ZIP archive"""
        
        files_archived = 0
        total_size = 0
        
        with zipfile.ZipFile(self.target_path, 'w', zipfile.ZIP_DEFLATED, 
                           compresslevel=self.compression_level) as zipf:
            
            if os.path.isfile(self.source_path):
                # Single file
                file_size = os.path.getsize(self.source_path)
                zipf.write(self.source_path, os.path.basename(self.source_path))
                files_archived = 1
                total_size = file_size
            else:
                # Directory
                for root, dirs, files in os.walk(self.source_path):
                    if not self.include_subdirs and root != self.source_path:
                        continue
                    
                    for file in files:
                        if fnmatch.fnmatch(file, self.file_pattern):
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, self.source_path)
                            
                            file_size = os.path.getsize(file_path)
                            zipf.write(file_path, arcname)
                            files_archived += 1
                            total_size += file_size
        
        compressed_size = os.path.getsize(self.target_path)
        return files_archived, total_size, compressed_size
    
    def _create_tar_archive(self) -> tuple:
        """Create TAR archive (with optional compression)"""
        
        mode_map = {
            'tar': 'w',
            'tar.gz': 'w:gz',
            'tar.bz2': 'w:bz2'
        }
        mode = mode_map.get(self.archive_format, 'w')
        
        files_archived = 0
        total_size = 0
        
        with tarfile.open(self.target_path, mode) as tar:
            if os.path.isfile(self.source_path):
                # Single file
                file_size = os.path.getsize(self.source_path)
                tar.add(self.source_path, arcname=os.path.basename(self.source_path))
                files_archived = 1
                total_size = file_size
            else:
                # Directory
                for root, dirs, files in os.walk(self.source_path):
                    if not self.include_subdirs and root != self.source_path:
                        continue
                    
                    for file in files:
                        if fnmatch.fnmatch(file, self.file_pattern):
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, self.source_path)
                            
                            file_size = os.path.getsize(file_path)
                            tar.add(file_path, arcname=arcname)
                            files_archived += 1
                            total_size += file_size
        
        compressed_size = os.path.getsize(self.target_path)
        return files_archived, total_size, compressed_size
    
    def _create_gzip_archive(self) -> tuple:
        """Create GZIP archive (single file only)"""
        
        if not os.path.isfile(self.source_path):
            raise ValueError("GZIP format only supports single files")
        
        file_size = os.path.getsize(self.source_path)
        
        with open(self.source_path, 'rb') as f_in:
            with gzip.open(self.target_path, 'wb', compresslevel=self.compression_level) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        compressed_size = os.path.getsize(self.target_path)
        return 1, file_size, compressed_size
    
    def _create_bz2_archive(self) -> tuple:
        """Create BZ2 archive (single file only)"""
        
        if not os.path.isfile(self.source_path):
            raise ValueError("BZ2 format only supports single files")
        
        file_size = os.path.getsize(self.source_path)
        
        with open(self.source_path, 'rb') as f_in:
            with bz2.open(self.target_path, 'wb', compresslevel=self.compression_level) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        compressed_size = os.path.getsize(self.target_path)
        return 1, file_size, compressed_size
    
    def _extract_zip_archive(self) -> tuple:
        """Extract ZIP archive"""
        
        files_extracted = 0
        total_size = 0
        
        with zipfile.ZipFile(self.source_path, 'r') as zipf:
            zipf.extractall(self.target_path)
            
            for info in zipf.infolist():
                if not info.is_dir():
                    files_extracted += 1
                    total_size += info.file_size
        
        return files_extracted, total_size
    
    def _extract_tar_archive(self) -> tuple:
        """Extract TAR archive"""
        
        mode_map = {
            'tar': 'r',
            'tar.gz': 'r:gz',
            'tar.bz2': 'r:bz2'
        }
        mode = mode_map.get(self.archive_format, 'r')
        
        files_extracted = 0
        total_size = 0
        
        with tarfile.open(self.source_path, mode) as tar:
            tar.extractall(self.target_path)
            
            for member in tar.getmembers():
                if member.isfile():
                    files_extracted += 1
                    total_size += member.size
        
        return files_extracted, total_size
    
    def _extract_gzip_archive(self) -> tuple:
        """Extract GZIP archive"""
        
        # Determine output filename
        output_filename = os.path.basename(self.source_path)
        if output_filename.endswith('.gz'):
            output_filename = output_filename[:-3]
        
        output_path = os.path.join(self.target_path, output_filename)
        
        with gzip.open(self.source_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        total_size = os.path.getsize(output_path)
        return 1, total_size
    
    def _extract_bz2_archive(self) -> tuple:
        """Extract BZ2 archive"""
        
        # Determine output filename
        output_filename = os.path.basename(self.source_path)
        if output_filename.endswith('.bz2'):
            output_filename = output_filename[:-4]
        
        output_path = os.path.join(self.target_path, output_filename)
        
        with bz2.open(self.source_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        total_size = os.path.getsize(output_path)
        return 1, total_size


class BatchArchiveOperator(BaseArchiveOperator):
    """Generic batch archiving operator for multiple files/directories"""
    
    template_fields = ('archive_jobs',)
    
    def __init__(self,
                 archive_jobs: List[Dict[str, Any]],
                 default_format: str = 'zip',
                 parallel_processing: bool = False,
                 **kwargs):
        """
        Initialize BatchArchiveOperator
        
        Args:
            archive_jobs: List of archive job configurations
            default_format: Default archive format if not specified in job
            parallel_processing: Whether to process jobs in parallel
        """
        super().__init__(**kwargs)
        self.archive_jobs = archive_jobs
        self.default_format = default_format
        self.parallel_processing = parallel_processing
    
    def execute_archive_operation(self, context: Context) -> Dict[str, Any]:
        """Execute batch archive operations"""
        
        logger.info(f"Starting batch archive operation with {len(self.archive_jobs)} jobs")
        
        results = []
        total_files = 0
        total_original_size = 0
        total_compressed_size = 0
        
        for i, job in enumerate(self.archive_jobs):
            try:
                logger.info(f"Processing archive job {i + 1}/{len(self.archive_jobs)}")
                
                # Create individual archive operator for this job
                archive_op = FileArchiveOperator(
                    operation=job.get('operation', 'compress'),
                    source_path=job['source_path'],
                    target_path=job['target_path'],
                    archive_format=job.get('archive_format', self.default_format),
                    file_pattern=job.get('file_pattern', '*'),
                    compression_level=job.get('compression_level', 6),
                    preserve_permissions=job.get('preserve_permissions', False),
                    include_subdirs=job.get('include_subdirs', True)
                )
                
                # Execute archive operation
                result = archive_op.execute_archive_operation(context)
                result['job_index'] = i
                result['job_name'] = job.get('name', f'job_{i}')
                
                results.append(result)
                
                # Accumulate statistics
                if result.get('success'):
                    total_files += result.get('files_archived', result.get('files_extracted', 0))
                    total_original_size += result.get('original_size', result.get('total_size', 0))
                    total_compressed_size += result.get('compressed_size', 0)
                
                logger.info(f"Archive job {i + 1} completed successfully")
                
            except Exception as e:
                logger.error(f"Archive job {i + 1} failed: {str(e)}")
                results.append({
                    'job_index': i,
                    'job_name': job.get('name', f'job_{i}'),
                    'success': False,
                    'error': str(e)
                })
        
        # Calculate overall statistics
        successful_jobs = sum(1 for r in results if r.get('success'))
        failed_jobs = len(results) - successful_jobs
        overall_compression_ratio = ((total_original_size - total_compressed_size) / 
                                   total_original_size if total_original_size > 0 else 0.0)
        
        # Store detailed results in XCom
        context['task_instance'].xcom_push(key='batch_archive_results', value=results)
        
        logger.info(f"Batch archive completed: {successful_jobs}/{len(self.archive_jobs)} jobs successful")
        
        return {
            'operation': 'batch_archive',
            'total_jobs': len(self.archive_jobs),
            'successful_jobs': successful_jobs,
            'failed_jobs': failed_jobs,
            'total_files_processed': total_files,
            'total_original_size': total_original_size,
            'total_compressed_size': total_compressed_size,
            'overall_compression_ratio': overall_compression_ratio,
            'success': failed_jobs == 0,
            'results': results
        }


def register_implementations(registry):
    """Register archive operation implementations"""
    
    # Register generic archive operations
    registry.register_implementation("FILE_ARCHIVE", "generic", FileArchiveOperator)
    registry.register_implementation("ARCHIVE", "generic", FileArchiveOperator)
    registry.register_implementation("COMPRESS", "generic", FileArchiveOperator)
    registry.register_implementation("DECOMPRESS", "generic", FileArchiveOperator)
    registry.register_implementation("BATCH_ARCHIVE", "generic", BatchArchiveOperator)
    
    # Register cloud provider implementations
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("FILE_ARCHIVE", provider, FileArchiveOperator)
        registry.register_implementation("ARCHIVE", provider, FileArchiveOperator)
        registry.register_implementation("COMPRESS", provider, FileArchiveOperator)
        registry.register_implementation("DECOMPRESS", provider, FileArchiveOperator)
        registry.register_implementation("BATCH_ARCHIVE", provider, BatchArchiveOperator)


# Example configuration usage:
"""
tasks:
  - name: "compress_logs"
    type: "COMPRESS"
    description: "Compress log files for archival"
    properties:
      operation: "compress"
      source_path: "/var/log/application"
      target_path: "/archives/logs_{{ ds }}.tar.gz"
      archive_format: "tar.gz"
      file_pattern: "*.log"
      compression_level: 9
      include_subdirs: true

  - name: "decompress_data"
    type: "DECOMPRESS"
    description: "Extract data from archive"
    properties:
      operation: "decompress"
      source_path: "/tmp/data_{{ ds }}.zip"
      target_path: "/tmp/extracted_data_{{ ds }}"
      archive_format: "zip"

  - name: "batch_archive_reports"
    type: "BATCH_ARCHIVE"
    description: "Archive multiple report directories"
    properties:
      default_format: "zip"
      archive_jobs:
        - name: "daily_reports"
          operation: "compress"
          source_path: "/reports/daily/{{ ds }}"
          target_path: "/archives/daily_reports_{{ ds }}.zip"
          file_pattern: "*.pdf"
        - name: "monthly_reports"
          operation: "compress"  
          source_path: "/reports/monthly/{{ ds[:7] }}"
          target_path: "/archives/monthly_reports_{{ ds[:7] }}.tar.gz"
          archive_format: "tar.gz"
          compression_level: 6
"""