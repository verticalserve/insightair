# Generic Salesforce Operations - Multi-cloud Salesforce integration
"""
Generic Salesforce operations that work across cloud environments:
- Data extraction from Salesforce objects
- Data loading to Salesforce
- Bulk API operations
- Marketing Cloud integration
- Automatic cloud-specific configuration
"""

import logging
from typing import Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod
import pandas as pd

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    CloudProvider
)

logger = logging.getLogger(__name__)


class BaseSalesforceOperator(BaseInsightAirOperator, ABC):
    """Base class for Salesforce operations"""
    
    def __init__(self,
                 salesforce_connection_id: Optional[str] = None,
                 sandbox: bool = False,
                 api_version: str = "52.0",
                 **kwargs):
        """
        Initialize BaseSalesforceOperator
        
        Args:
            salesforce_connection_id: Salesforce connection ID
            sandbox: Whether to use sandbox environment
            api_version: Salesforce API version
        """
        super().__init__(**kwargs)
        self.salesforce_connection_id = salesforce_connection_id or "salesforce_default"
        self.sandbox = sandbox
        self.api_version = api_version
        self._sf_client = None
    
    def get_salesforce_client(self):
        """Get Salesforce client based on environment"""
        
        if self._sf_client:
            return self._sf_client
        
        try:
            from simple_salesforce import Salesforce
            from airflow.hooks.base import BaseHook
            
            # Get connection details
            connection = BaseHook.get_connection(self.salesforce_connection_id)
            
            # Extract additional connection parameters
            extra = connection.extra_dejson
            
            # Create Salesforce client
            self._sf_client = Salesforce(
                username=connection.login,
                password=connection.password,
                security_token=extra.get('security_token'),
                domain=extra.get('domain', 'test' if self.sandbox else 'login'),
                version=self.api_version
            )
            
            return self._sf_client
            
        except ImportError:
            raise ImportError("simple-salesforce package required. Install with: pip install simple-salesforce")
    
    @abstractmethod
    def execute_salesforce_operation(self, context: Context) -> Any:
        """Execute the Salesforce operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic configuration"""
        return self.execute_salesforce_operation(context)


class SalesforceExtractOperator(BaseSalesforceOperator):
    """Generic Salesforce data extraction operator"""
    
    template_fields = ('soql_query', 'object_name', 'output_file')
    
    def __init__(self,
                 soql_query: Optional[str] = None,
                 object_name: Optional[str] = None,
                 fields: Optional[List[str]] = None,
                 where_clause: Optional[str] = None,
                 output_file: Optional[str] = None,
                 output_format: str = "csv",  # csv, json, parquet
                 batch_size: int = 10000,
                 include_deleted: bool = False,
                 **kwargs):
        """
        Initialize SalesforceExtractOperator
        
        Args:
            soql_query: Custom SOQL query
            object_name: Salesforce object name (if not using custom query)
            fields: Fields to extract (if using object_name)
            where_clause: WHERE clause for query
            output_file: Output file path
            output_format: Output format (csv, json, parquet)
            batch_size: Batch size for bulk operations
            include_deleted: Whether to include deleted records
        """
        super().__init__(**kwargs)
        self.soql_query = soql_query
        self.object_name = object_name
        self.fields = fields or []
        self.where_clause = where_clause
        self.output_file = output_file
        self.output_format = output_format.lower()
        self.batch_size = batch_size
        self.include_deleted = include_deleted
    
    def execute_salesforce_operation(self, context: Context) -> Dict[str, Any]:
        """Extract data from Salesforce"""
        
        env_context = get_environment_context()
        
        if env_context.cloud_provider == CloudProvider.AWS:
            return self._extract_aws(context)
        elif env_context.cloud_provider == CloudProvider.GCP:
            return self._extract_gcp(context)
        elif env_context.cloud_provider == CloudProvider.AZURE:
            return self._extract_azure(context)
        elif env_context.cloud_provider == CloudProvider.OCI:
            return self._extract_oci(context)
        else:
            return self._extract_generic(context)
    
    def _extract_aws(self, context: Context) -> Dict[str, Any]:
        """Extract on AWS"""
        result = self._extract_generic(context)
        result['platform'] = 'aws'
        
        # AWS-specific optimizations could be added here
        # e.g., direct S3 upload, Lambda processing, etc.
        
        return result
    
    def _extract_gcp(self, context: Context) -> Dict[str, Any]:
        """Extract on GCP"""
        result = self._extract_generic(context)
        result['platform'] = 'gcp'
        
        # GCP-specific optimizations could be added here
        # e.g., BigQuery integration, Cloud Functions, etc.
        
        return result
    
    def _extract_azure(self, context: Context) -> Dict[str, Any]:
        """Extract on Azure"""
        result = self._extract_generic(context)
        result['platform'] = 'azure'
        
        # Azure-specific optimizations could be added here
        # e.g., Azure Data Factory integration, etc.
        
        return result
    
    def _extract_oci(self, context: Context) -> Dict[str, Any]:
        """Extract on OCI"""
        result = self._extract_generic(context)
        result['platform'] = 'oci'
        return result
    
    def _extract_generic(self, context: Context) -> Dict[str, Any]:
        """Generic Salesforce extraction"""
        
        sf = self.get_salesforce_client()
        
        # Build query
        if self.soql_query:
            query = self.soql_query
        else:
            query = self._build_query()
        
        logger.info(f"Executing Salesforce query: {query}")
        
        # Execute query
        if self.include_deleted:
            result = sf.query_all_iter(query, include_deleted=True)
        else:
            result = sf.query_all_iter(query)
        
        # Process results
        records = []
        for record in result:
            # Remove Salesforce metadata
            clean_record = {k: v for k, v in record.items() if not k.startswith('attributes')}
            records.append(clean_record)
        
        logger.info(f"Extracted {len(records)} records from Salesforce")
        
        # Save to file if specified
        if self.output_file:
            self._save_records_to_file(records)
        
        # Store in XCom
        context['task_instance'].xcom_push(key='salesforce_records', value=records[:1000])  # Limit XCom size
        context['task_instance'].xcom_push(key='record_count', value=len(records))
        
        return {
            'object_name': self.object_name,
            'query': query,
            'records_extracted': len(records),
            'output_file': self.output_file,
            'success': True
        }
    
    def _build_query(self) -> str:
        """Build SOQL query from parameters"""
        
        if not self.object_name:
            raise ValueError("Either soql_query or object_name must be specified")
        
        # Build SELECT clause
        if self.fields:
            select_clause = ", ".join(self.fields)
        else:
            # Get object description to retrieve all fields
            sf = self.get_salesforce_client()
            obj_desc = getattr(sf, self.object_name).describe()
            select_clause = ", ".join([field['name'] for field in obj_desc['fields']])
        
        # Build query
        query = f"SELECT {select_clause} FROM {self.object_name}"
        
        if self.where_clause:
            query += f" WHERE {self.where_clause}"
        
        return query
    
    def _save_records_to_file(self, records: List[Dict[str, Any]]):
        """Save records to output file"""
        
        if not records:
            logger.warning("No records to save")
            return
        
        from pathlib import Path
        
        # Create output directory
        Path(self.output_file).parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Save based on format
        if self.output_format == 'csv':
            df.to_csv(self.output_file, index=False)
        elif self.output_format == 'json':
            df.to_json(self.output_file, orient='records', indent=2)
        elif self.output_format == 'parquet':
            df.to_parquet(self.output_file, index=False)
        else:
            raise ValueError(f"Unsupported output format: {self.output_format}")
        
        logger.info(f"Saved {len(records)} records to {self.output_file}")


class SalesforceLoadOperator(BaseSalesforceOperator):
    """Generic Salesforce data loading operator"""
    
    template_fields = ('input_file', 'object_name')
    
    def __init__(self,
                 object_name: str,
                 input_file: Optional[str] = None,
                 input_data: Optional[List[Dict[str, Any]]] = None,
                 operation: str = "insert",  # insert, update, upsert, delete
                 external_id_field: Optional[str] = None,  # For upsert operations
                 batch_size: int = 200,
                 use_bulk_api: bool = True,
                 **kwargs):
        """
        Initialize SalesforceLoadOperator
        
        Args:
            object_name: Salesforce object name
            input_file: Input file path
            input_data: Input data as list of dictionaries
            operation: Operation type (insert, update, upsert, delete)
            external_id_field: External ID field for upsert operations
            batch_size: Batch size for operations
            use_bulk_api: Whether to use Bulk API for large datasets
        """
        super().__init__(**kwargs)
        self.object_name = object_name
        self.input_file = input_file
        self.input_data = input_data
        self.operation = operation.lower()
        self.external_id_field = external_id_field
        self.batch_size = batch_size
        self.use_bulk_api = use_bulk_api
    
    def execute_salesforce_operation(self, context: Context) -> Dict[str, Any]:
        """Load data to Salesforce"""
        
        env_context = get_environment_context()
        
        if env_context.cloud_provider == CloudProvider.AWS:
            return self._load_aws(context)
        elif env_context.cloud_provider == CloudProvider.GCP:
            return self._load_gcp(context)
        elif env_context.cloud_provider == CloudProvider.AZURE:
            return self._load_azure(context)
        elif env_context.cloud_provider == CloudProvider.OCI:
            return self._load_oci(context)
        else:
            return self._load_generic(context)
    
    def _load_aws(self, context: Context) -> Dict[str, Any]:
        """Load on AWS"""
        result = self._load_generic(context)
        result['platform'] = 'aws'
        return result
    
    def _load_gcp(self, context: Context) -> Dict[str, Any]:
        """Load on GCP"""
        result = self._load_generic(context)
        result['platform'] = 'gcp'
        return result
    
    def _load_azure(self, context: Context) -> Dict[str, Any]:
        """Load on Azure"""
        result = self._load_generic(context)
        result['platform'] = 'azure'
        return result
    
    def _load_oci(self, context: Context) -> Dict[str, Any]:
        """Load on OCI"""
        result = self._load_generic(context)
        result['platform'] = 'oci'
        return result
    
    def _load_generic(self, context: Context) -> Dict[str, Any]:
        """Generic Salesforce loading"""
        
        sf = self.get_salesforce_client()
        
        # Load data
        if self.input_data:
            data = self.input_data
        elif self.input_file:
            data = self._load_data_from_file()
        else:
            raise ValueError("Either input_file or input_data must be provided")
        
        if not data:
            logger.warning("No data to load")
            return {
                'object_name': self.object_name,
                'operation': self.operation,
                'records_processed': 0,
                'success': True,
                'message': 'No data to load'
            }
        
        logger.info(f"Loading {len(data)} records to {self.object_name} using {self.operation}")
        
        # Execute operation
        if self.use_bulk_api and len(data) > self.batch_size:
            results = self._execute_bulk_operation(sf, data)
        else:
            results = self._execute_standard_operation(sf, data)
        
        # Process results
        success_count = sum(1 for r in results if r.get('success', False))
        error_count = len(results) - success_count
        
        # Store results in XCom
        context['task_instance'].xcom_push(key='load_results', value=results[:100])  # Limit XCom size
        context['task_instance'].xcom_push(key='success_count', value=success_count)
        context['task_instance'].xcom_push(key='error_count', value=error_count)
        
        return {
            'object_name': self.object_name,
            'operation': self.operation,
            'records_processed': len(data),
            'success_count': success_count,
            'error_count': error_count,
            'use_bulk_api': self.use_bulk_api and len(data) > self.batch_size,
            'success': error_count == 0
        }
    
    def _load_data_from_file(self) -> List[Dict[str, Any]]:
        """Load data from input file"""
        from pathlib import Path
        
        file_path = Path(self.input_file)
        
        if not file_path.exists():
            raise ValueError(f"Input file not found: {self.input_file}")
        
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(self.input_file)
            return df.to_dict('records')
        elif file_path.suffix.lower() == '.json':
            import json
            with open(self.input_file, 'r') as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    def _execute_standard_operation(self, sf, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute operation using standard REST API"""
        
        results = []
        
        # Get object reference
        obj = getattr(sf, self.object_name)
        
        # Process in batches
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            
            try:
                if self.operation == 'insert':
                    batch_results = [obj.create(record) for record in batch]
                elif self.operation == 'update':
                    batch_results = [obj.update(record['Id'], record) for record in batch]
                elif self.operation == 'upsert':
                    if not self.external_id_field:
                        raise ValueError("external_id_field required for upsert operation")
                    batch_results = [obj.upsert(self.external_id_field, record) for record in batch]
                elif self.operation == 'delete':
                    batch_results = [obj.delete(record['Id']) for record in batch]
                else:
                    raise ValueError(f"Unsupported operation: {self.operation}")
                
                results.extend(batch_results)
                
            except Exception as e:
                logger.error(f"Batch operation failed: {str(e)}")
                # Add error result for each record in batch
                for record in batch:
                    results.append({'success': False, 'error': str(e), 'record': record})
        
        return results
    
    def _execute_bulk_operation(self, sf, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute operation using Bulk API"""
        
        try:
            from salesforce_bulk import SalesforceBulk
            
            bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.base_url)
            
            # Create job
            job_id = bulk.create_query_job(self.object_name, contentType='CSV')
            
            # Convert data to CSV
            df = pd.DataFrame(data)
            csv_data = df.to_csv(index=False)
            
            # Add batch
            batch_id = bulk.query(job=job_id, soql=csv_data)
            
            # Close job
            bulk.close_job(job_id)
            
            # Wait for completion and get results
            bulk.wait_for_batch(job=job_id, batch=batch_id)
            results = bulk.get_batch_results(job_id=job_id, batch_id=batch_id)
            
            return results
            
        except ImportError:
            logger.warning("salesforce-bulk package not available, falling back to standard API")
            return self._execute_standard_operation(sf, data)
        except Exception as e:
            logger.error(f"Bulk API operation failed: {str(e)}")
            return [{'success': False, 'error': str(e)} for _ in data]


class SalesforceQueryOperator(BaseSalesforceOperator):
    """Generic Salesforce SOQL query operator"""
    
    template_fields = ('soql_query',)
    
    def __init__(self,
                 soql_query: str,
                 include_deleted: bool = False,
                 **kwargs):
        """
        Initialize SalesforceQueryOperator
        
        Args:
            soql_query: SOQL query to execute
            include_deleted: Whether to include deleted records
        """
        super().__init__(**kwargs)
        self.soql_query = soql_query
        self.include_deleted = include_deleted
    
    def execute_salesforce_operation(self, context: Context) -> Dict[str, Any]:
        """Execute SOQL query"""
        
        sf = self.get_salesforce_client()
        
        logger.info(f"Executing SOQL query: {self.soql_query}")
        
        # Execute query
        if self.include_deleted:
            result = sf.query_all(self.soql_query, include_deleted=True)
        else:
            result = sf.query_all(self.soql_query)
        
        records = result['records']
        
        # Clean records (remove Salesforce metadata)
        clean_records = []
        for record in records:
            clean_record = {k: v for k, v in record.items() if not k.startswith('attributes')}
            clean_records.append(clean_record)
        
        logger.info(f"Query returned {len(clean_records)} records")
        
        # Store in XCom
        context['task_instance'].xcom_push(key='query_results', value=clean_records[:1000])  # Limit XCom size
        context['task_instance'].xcom_push(key='record_count', value=len(clean_records))
        
        return {
            'query': self.soql_query,
            'record_count': len(clean_records),
            'total_size': result['totalSize'],
            'done': result['done'],
            'success': True
        }


def register_implementations(registry):
    """Register Salesforce operation implementations"""
    
    # Register generic Salesforce operations
    registry.register_implementation("SALESFORCE_EXTRACT", "generic", SalesforceExtractOperator)
    registry.register_implementation("SALESFORCE_LOAD", "generic", SalesforceLoadOperator)
    registry.register_implementation("SALESFORCE_QUERY", "generic", SalesforceQueryOperator)
    
    # Register cloud provider implementations
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("SALESFORCE_EXTRACT", provider, SalesforceExtractOperator)
        registry.register_implementation("SALESFORCE_LOAD", provider, SalesforceLoadOperator)
        registry.register_implementation("SALESFORCE_QUERY", provider, SalesforceQueryOperator)


# Example configuration usage:
"""
tasks:
  - name: "extract_opportunities"
    type: "SALESFORCE_EXTRACT"
    description: "Extract opportunity data from Salesforce"
    properties:
      object_name: "Opportunity"
      fields: ["Id", "Name", "Amount", "CloseDate", "StageName", "AccountId"]
      where_clause: "CloseDate >= {{ ds }} AND CloseDate < {{ next_ds }}"
      output_file: "/tmp/opportunities_{{ ds }}.csv"
      output_format: "csv"
      # salesforce_connection_id: auto-resolved

  - name: "load_contacts"
    type: "SALESFORCE_LOAD"
    description: "Load new contacts to Salesforce"
    properties:
      object_name: "Contact"
      input_file: "/tmp/new_contacts_{{ ds }}.csv"
      operation: "insert"
      batch_size: 200
      use_bulk_api: true

  - name: "query_account_summary"
    type: "SALESFORCE_QUERY"
    description: "Get account summary data"
    properties:
      soql_query: |
        SELECT Id, Name, Type, Industry, 
               (SELECT COUNT() FROM Opportunities WHERE CloseDate >= THIS_MONTH)
        FROM Account 
        WHERE Type = 'Customer'
      include_deleted: false

  - name: "upsert_lead_data"
    type: "SALESFORCE_LOAD"
    description: "Upsert lead data using external ID"
    properties:
      object_name: "Lead"
      input_file: "/tmp/lead_updates_{{ ds }}.json"
      operation: "upsert"
      external_id_field: "External_Id__c"
      use_bulk_api: false
"""