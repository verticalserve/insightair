# Generic Database Operations - Multi-database abstraction
"""
Generic database operations that work across database types and cloud providers:
- SQL query execution
- Data extraction and loading
- Table operations (create, drop, truncate)
- Bulk data operations
- Automatic database type detection and hook selection
"""

import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from abc import ABC, abstractmethod
import pandas as pd

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    DatabaseType, CloudProvider
)

logger = logging.getLogger(__name__)


class BaseDatabaseOperator(BaseInsightAirOperator, ABC):
    """Base class for database operations"""
    
    def __init__(self,
                 sql: Optional[str] = None,
                 database_connection_id: Optional[str] = None,
                 database_type: Optional[str] = None,
                 **kwargs):
        """
        Initialize BaseDatabaseOperator
        
        Args:
            sql: SQL query to execute
            database_connection_id: Database connection ID (auto-resolved if not provided)
            database_type: Database type override
        """
        super().__init__(**kwargs)
        self.sql = sql
        self.database_connection_id = database_connection_id
        self.database_type = database_type
        self._resolved_hook = None
    
    def get_database_hook(self):
        """Get appropriate database hook based on connection and environment"""
        
        if self._resolved_hook:
            return self._resolved_hook
        
        # Resolve connection if not provided
        if not self.database_connection_id:
            resolver = get_connection_resolver()
            database_type_enum = DatabaseType(self.database_type) if self.database_type else None
            self.database_connection_id = resolver.resolve_database_connection(
                database_type=database_type_enum
            )
        
        # Get connection to determine database type
        connection = BaseHook.get_connection(self.database_connection_id)
        conn_type = connection.conn_type.lower()
        
        # Map connection type to database hook
        if conn_type in ['postgres', 'postgresql']:
            self._resolved_hook = self._get_postgres_hook()
        elif conn_type in ['mysql']:
            self._resolved_hook = self._get_mysql_hook()
        elif conn_type in ['oracle']:
            self._resolved_hook = self._get_oracle_hook()
        elif conn_type in ['mssql', 'sqlserver']:
            self._resolved_hook = self._get_sqlserver_hook()
        elif conn_type in ['sqlite']:
            self._resolved_hook = self._get_sqlite_hook()
        elif conn_type in ['snowflake']:
            self._resolved_hook = self._get_snowflake_hook()
        elif conn_type in ['bigquery']:
            self._resolved_hook = self._get_bigquery_hook()
        elif conn_type in ['redshift']:
            self._resolved_hook = self._get_redshift_hook()
        else:
            # Generic database hook
            from airflow.hooks.dbapi import DbApiHook
            self._resolved_hook = DbApiHook(conn_id=self.database_connection_id)
        
        return self._resolved_hook
    
    def _get_postgres_hook(self):
        """Get PostgreSQL hook"""
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            return PostgresHook(postgres_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("PostgreSQL provider not available. Install with: pip install apache-airflow-providers-postgres")
    
    def _get_mysql_hook(self):
        """Get MySQL hook"""
        try:
            from airflow.providers.mysql.hooks.mysql import MySqlHook
            return MySqlHook(mysql_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("MySQL provider not available. Install with: pip install apache-airflow-providers-mysql")
    
    def _get_oracle_hook(self):
        """Get Oracle hook"""
        try:
            from airflow.providers.oracle.hooks.oracle import OracleHook
            return OracleHook(oracle_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("Oracle provider not available. Install with: pip install apache-airflow-providers-oracle")
    
    def _get_sqlserver_hook(self):
        """Get SQL Server hook"""
        try:
            from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
            return MsSqlHook(mssql_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("SQL Server provider not available. Install with: pip install apache-airflow-providers-microsoft-mssql")
    
    def _get_sqlite_hook(self):
        """Get SQLite hook"""
        try:
            from airflow.providers.sqlite.hooks.sqlite import SqliteHook
            return SqliteHook(sqlite_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("SQLite provider not available. Install with: pip install apache-airflow-providers-sqlite")
    
    def _get_snowflake_hook(self):
        """Get Snowflake hook"""
        try:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
            return SnowflakeHook(snowflake_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("Snowflake provider not available. Install with: pip install apache-airflow-providers-snowflake")
    
    def _get_bigquery_hook(self):
        """Get BigQuery hook"""
        try:
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
            return BigQueryHook(gcp_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("BigQuery provider not available. Install with: pip install apache-airflow-providers-google")
    
    def _get_redshift_hook(self):
        """Get Redshift hook"""
        try:
            from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
            return RedshiftSQLHook(redshift_conn_id=self.database_connection_id)
        except ImportError:
            raise ImportError("Redshift provider not available. Install with: pip install apache-airflow-providers-amazon")
    
    @abstractmethod
    def execute_database_operation(self, context: Context) -> Any:
        """Execute the database operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic hook resolution"""
        return self.execute_database_operation(context)


class DatabaseQueryOperator(BaseDatabaseOperator):
    """Generic database query operator"""
    
    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)
    
    def __init__(self,
                 sql: str,
                 parameters: Optional[Dict[str, Any]] = None,
                 fetch_result: bool = False,
                 return_last: bool = True,
                 split_statements: bool = False,
                 **kwargs):
        """
        Initialize DatabaseQueryOperator
        
        Args:
            sql: SQL query to execute
            parameters: Query parameters for parameterized queries
            fetch_result: Whether to fetch and return query results
            return_last: Whether to return only the last statement result
            split_statements: Whether to split SQL into multiple statements
        """
        super().__init__(sql=sql, **kwargs)
        self.parameters = parameters or {}
        self.fetch_result = fetch_result
        self.return_last = return_last
        self.split_statements = split_statements
    
    def execute_database_operation(self, context: Context) -> Any:
        """Execute SQL query"""
        
        hook = self.get_database_hook()
        
        logger.info(f"Executing SQL query on {hook.__class__.__name__}")
        
        try:
            if self.fetch_result:
                # Execute query and fetch results
                if hasattr(hook, 'get_pandas_df'):
                    # Use pandas method if available
                    result = hook.get_pandas_df(
                        sql=self.sql,
                        parameters=self.parameters
                    )
                    logger.info(f"Query returned {len(result)} rows")
                    return result.to_dict('records')
                else:
                    # Use standard record fetching
                    result = hook.get_records(
                        sql=self.sql,
                        parameters=self.parameters
                    )
                    logger.info(f"Query returned {len(result)} rows")
                    return result
            else:
                # Execute without fetching results
                if self.split_statements:
                    statements = [stmt.strip() for stmt in self.sql.split(';') if stmt.strip()]
                    results = []
                    for stmt in statements:
                        result = hook.run(
                            sql=stmt,
                            parameters=self.parameters,
                            autocommit=True
                        )
                        results.append(result)
                    return results[-1] if self.return_last else results
                else:
                    result = hook.run(
                        sql=self.sql,
                        parameters=self.parameters,
                        autocommit=True
                    )
                    logger.info("Query executed successfully")
                    return result
                    
        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
            raise


class DatabaseTableOperator(BaseDatabaseOperator):
    """Generic database table operations operator"""
    
    template_fields = ('table_name', 'schema', 'sql')
    
    def __init__(self,
                 operation: str,  # 'create', 'drop', 'truncate', 'exists'
                 table_name: str,
                 schema: Optional[str] = None,
                 sql: Optional[str] = None,  # For create table operations
                 if_exists: str = 'raise',  # 'raise', 'ignore', 'replace'
                 **kwargs):
        """
        Initialize DatabaseTableOperator
        
        Args:
            operation: Table operation to perform
            table_name: Name of the table
            schema: Database schema name
            sql: SQL for table creation
            if_exists: What to do if table exists ('raise', 'ignore', 'replace')
        """
        super().__init__(sql=sql, **kwargs)
        self.operation = operation.lower()
        self.table_name = table_name
        self.schema = schema
        self.if_exists = if_exists
    
    def execute_database_operation(self, context: Context) -> Dict[str, Any]:
        """Execute table operation"""
        
        hook = self.get_database_hook()
        full_table_name = f"{self.schema}.{self.table_name}" if self.schema else self.table_name
        
        logger.info(f"Executing table operation '{self.operation}' on {full_table_name}")
        
        if self.operation == 'create':
            return self._create_table(hook, full_table_name)
        elif self.operation == 'drop':
            return self._drop_table(hook, full_table_name)
        elif self.operation == 'truncate':
            return self._truncate_table(hook, full_table_name)
        elif self.operation == 'exists':
            return self._check_table_exists(hook, full_table_name)
        else:
            raise ValueError(f"Unsupported table operation: {self.operation}")
    
    def _create_table(self, hook, table_name: str) -> Dict[str, Any]:
        """Create table"""
        
        # Check if table exists
        table_exists = self._table_exists(hook, table_name)
        
        if table_exists:
            if self.if_exists == 'raise':
                raise ValueError(f"Table {table_name} already exists")
            elif self.if_exists == 'ignore':
                logger.info(f"Table {table_name} already exists, ignoring")
                return {'operation': 'create', 'table_name': table_name, 'action': 'ignored', 'success': True}
            elif self.if_exists == 'replace':
                # Drop existing table first
                drop_sql = f"DROP TABLE {table_name}"
                hook.run(drop_sql, autocommit=True)
                logger.info(f"Dropped existing table {table_name}")
        
        # Create table
        if not self.sql:
            raise ValueError("SQL statement required for table creation")
        
        hook.run(self.sql, autocommit=True)
        logger.info(f"Created table {table_name}")
        
        return {
            'operation': 'create',
            'table_name': table_name,
            'action': 'created',
            'success': True
        }
    
    def _drop_table(self, hook, table_name: str) -> Dict[str, Any]:
        """Drop table"""
        
        table_exists = self._table_exists(hook, table_name)
        
        if not table_exists and self.if_exists == 'ignore':
            logger.info(f"Table {table_name} does not exist, ignoring")
            return {'operation': 'drop', 'table_name': table_name, 'action': 'ignored', 'success': True}
        
        if not table_exists:
            raise ValueError(f"Table {table_name} does not exist")
        
        drop_sql = f"DROP TABLE {table_name}"
        hook.run(drop_sql, autocommit=True)
        logger.info(f"Dropped table {table_name}")
        
        return {
            'operation': 'drop',
            'table_name': table_name,
            'action': 'dropped',
            'success': True
        }
    
    def _truncate_table(self, hook, table_name: str) -> Dict[str, Any]:
        """Truncate table"""
        
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        hook.run(truncate_sql, autocommit=True)
        logger.info(f"Truncated table {table_name}")
        
        return {
            'operation': 'truncate',
            'table_name': table_name,
            'action': 'truncated',
            'success': True
        }
    
    def _check_table_exists(self, hook, table_name: str) -> Dict[str, Any]:
        """Check if table exists"""
        
        exists = self._table_exists(hook, table_name)
        logger.info(f"Table {table_name} exists: {exists}")
        
        return {
            'operation': 'exists',
            'table_name': table_name,
            'exists': exists,
            'success': True
        }
    
    def _table_exists(self, hook, table_name: str) -> bool:
        """Check if table exists"""
        try:
            if hasattr(hook, 'table_exists'):
                # Some hooks have a table_exists method
                return hook.table_exists(table_name)
            else:
                # Generic check using information_schema
                schema_name, table_name_only = table_name.split('.') if '.' in table_name else (None, table_name)
                
                if schema_name:
                    check_sql = """
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s AND table_schema = %s
                    """
                    result = hook.get_records(check_sql, parameters=[table_name_only, schema_name])
                else:
                    check_sql = """
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s
                    """
                    result = hook.get_records(check_sql, parameters=[table_name_only])
                
                return len(result) > 0
        except Exception as e:
            logger.debug(f"Error checking table existence: {e}")
            return False


class DatabaseBulkLoadOperator(BaseDatabaseOperator):
    """Generic bulk data loading operator"""
    
    template_fields = ('table_name', 'source_file', 'schema')
    
    def __init__(self,
                 table_name: str,
                 source_file: Optional[str] = None,
                 source_data: Optional[List[Dict[str, Any]]] = None,
                 schema: Optional[str] = None,
                 load_mode: str = 'append',  # 'append', 'overwrite', 'replace'
                 batch_size: int = 1000,
                 create_table: bool = False,
                 **kwargs):
        """
        Initialize DatabaseBulkLoadOperator
        
        Args:
            table_name: Target table name
            source_file: Source file path (CSV, JSON, etc.)
            source_data: Source data as list of dictionaries
            schema: Database schema name
            load_mode: How to load data ('append', 'overwrite', 'replace')
            batch_size: Batch size for bulk operations
            create_table: Whether to create table if it doesn't exist
        """
        super().__init__(**kwargs)
        self.table_name = table_name
        self.source_file = source_file
        self.source_data = source_data
        self.schema = schema
        self.load_mode = load_mode
        self.batch_size = batch_size
        self.create_table = create_table
    
    def execute_database_operation(self, context: Context) -> Dict[str, Any]:
        """Execute bulk load operation"""
        
        hook = self.get_database_hook()
        full_table_name = f"{self.schema}.{self.table_name}" if self.schema else self.table_name
        
        # Prepare data
        if self.source_file:
            data = self._load_data_from_file()
        elif self.source_data:
            data = self.source_data
        else:
            raise ValueError("Either source_file or source_data must be provided")
        
        if not data:
            logger.warning("No data to load")
            return {
                'table_name': full_table_name,
                'records_loaded': 0,
                'success': True,
                'message': 'No data to load'
            }
        
        logger.info(f"Loading {len(data)} records to {full_table_name}")
        
        # Handle load modes
        if self.load_mode == 'overwrite':
            # Truncate table first
            try:
                hook.run(f"TRUNCATE TABLE {full_table_name}", autocommit=True)
                logger.info(f"Truncated table {full_table_name}")
            except Exception as e:
                logger.warning(f"Could not truncate table: {e}")
        
        # Load data
        records_loaded = self._bulk_insert_data(hook, full_table_name, data)
        
        return {
            'table_name': full_table_name,
            'records_loaded': records_loaded,
            'success': True,
            'load_mode': self.load_mode
        }
    
    def _load_data_from_file(self) -> List[Dict[str, Any]]:
        """Load data from file"""
        from pathlib import Path
        
        file_path = Path(self.source_file)
        
        if not file_path.exists():
            raise ValueError(f"Source file not found: {self.source_file}")
        
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(self.source_file)
            return df.to_dict('records')
        elif file_path.suffix.lower() == '.json':
            import json
            with open(self.source_file, 'r') as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(self.source_file)
            return df.to_dict('records')
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    def _bulk_insert_data(self, hook, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Bulk insert data into table"""
        
        if not data:
            return 0
        
        # Get column names from first record
        columns = list(data[0].keys())
        
        # Prepare bulk insert SQL
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Convert data to tuples
        rows = [tuple(record.get(col) for col in columns) for record in data]
        
        # Execute in batches
        records_loaded = 0
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i + self.batch_size]
            
            try:
                if hasattr(hook, 'insert_rows'):
                    # Use insert_rows method if available
                    hook.insert_rows(
                        table=table_name,
                        rows=batch,
                        target_fields=columns,
                        commit_every=self.batch_size
                    )
                else:
                    # Use executemany for bulk insert
                    with hook.get_conn() as conn:
                        with conn.cursor() as cursor:
                            cursor.executemany(insert_sql, batch)
                        conn.commit()
                
                records_loaded += len(batch)
                logger.info(f"Loaded batch of {len(batch)} records ({records_loaded}/{len(rows)} total)")
                
            except Exception as e:
                logger.error(f"Failed to load batch starting at row {i}: {e}")
                raise
        
        return records_loaded


def register_implementations(registry):
    """Register database operation implementations"""
    
    # Register generic database operations
    registry.register_implementation("DATABASE_QUERY", "generic", DatabaseQueryOperator)
    registry.register_implementation("DATABASE_TABLE", "generic", DatabaseTableOperator)
    registry.register_implementation("DATABASE_BULK_LOAD", "generic", DatabaseBulkLoadOperator)
    
    # Register cloud provider implementations (same operators work for all)
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("DATABASE_QUERY", provider, DatabaseQueryOperator)
        registry.register_implementation("DATABASE_TABLE", provider, DatabaseTableOperator)
        registry.register_implementation("DATABASE_BULK_LOAD", provider, DatabaseBulkLoadOperator)
        
        # Register database-specific implementations
        for db_type in ['postgresql', 'mysql', 'oracle', 'sqlserver', 'sqlite', 'snowflake', 'bigquery', 'redshift']:
            registry.register_implementation("DATABASE_QUERY", f"{provider}_{db_type}", DatabaseQueryOperator)
            registry.register_implementation("DATABASE_TABLE", f"{provider}_{db_type}", DatabaseTableOperator)
            registry.register_implementation("DATABASE_BULK_LOAD", f"{provider}_{db_type}", DatabaseBulkLoadOperator)


# Example configuration usage:
"""
tasks:
  - name: "create_staging_table"
    type: "DATABASE_TABLE"
    description: "Create staging table for data processing"
    properties:
      operation: "create"
      table_name: "staging_data"
      schema: "{{ var.value.staging_schema }}"
      if_exists: "replace"
      sql: |
        CREATE TABLE staging_data (
          id SERIAL PRIMARY KEY,
          name VARCHAR(100),
          email VARCHAR(100),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      # database_connection_id: auto-resolved based on environment
      # database_type: auto-detected from connection

  - name: "load_user_data"
    type: "DATABASE_BULK_LOAD"
    description: "Bulk load user data from CSV file"
    properties:
      table_name: "users"
      schema: "public"
      source_file: "/tmp/users_{{ ds }}.csv"
      load_mode: "append"
      batch_size: 1000
      create_table: false

  - name: "run_data_analysis"
    type: "DATABASE_QUERY"
    description: "Run data analysis query"
    properties:
      sql: |
        SELECT 
          DATE_TRUNC('day', created_at) as date,
          COUNT(*) as user_count,
          COUNT(DISTINCT email) as unique_emails
        FROM users 
        WHERE created_at >= '{{ ds }}'
        GROUP BY DATE_TRUNC('day', created_at)
        ORDER BY date
      fetch_result: true
      parameters:
        min_date: "{{ ds }}"

  - name: "cleanup_staging"
    type: "DATABASE_TABLE"
    description: "Clean up staging table"
    properties:
      operation: "drop"
      table_name: "staging_data"
      schema: "{{ var.value.staging_schema }}"
      if_exists: "ignore"
"""