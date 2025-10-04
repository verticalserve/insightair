"""
AWS Redshift Database Operations
Provides database operations for Amazon Redshift data warehouse
"""

import logging
from typing import Dict, List, Any, Optional, Union
import psycopg2
import psycopg2.extras
from .base import BaseDatabaseOperations

logger = logging.getLogger(__name__)


class RedshiftOperations(BaseDatabaseOperations):
    """
    AWS Redshift database operations class
    Handles connections and operations specific to Amazon Redshift
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize Redshift connection parameters
        
        Args:
            connection_params: Dictionary containing Redshift connection parameters
                - host: Redshift cluster endpoint
                - port: Port number (default: 5439)
                - database: Database name
                - user: Username
                - password: Password
                - sslmode: SSL mode (default: require)
        """
        super().__init__(connection_params)
        self.host = connection_params.get('host')
        self.port = connection_params.get('port', 5439)
        self.database = connection_params.get('database')
        self.user = connection_params.get('user')
        self.password = connection_params.get('password')
        self.sslmode = connection_params.get('sslmode', 'require')
        
    def connect(self) -> bool:
        """
        Establish connection to Redshift cluster
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                sslmode=self.sslmode
            )
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logger.info(f"Connected to Redshift cluster: {self.host}:{self.port}/{self.database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            return False
            
    def disconnect(self):
        """
        Close Redshift connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from Redshift")
        
    def execute_query(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries representing query results
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            results = self.cursor.fetchall()
            self.log_operation("SELECT", f"Query executed, {len(results)} rows returned")
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
            
    def execute_command(self, command: str, params: Optional[List[Any]] = None) -> int:
        """
        Execute a non-SELECT command (INSERT, UPDATE, DELETE, etc.)
        
        Args:
            command: SQL command string
            params: Optional command parameters
            
        Returns:
            Number of affected rows
        """
        try:
            if params:
                self.cursor.execute(command, params)
            else:
                self.cursor.execute(command)
            
            self.connection.commit()
            affected_rows = self.cursor.rowcount
            self.log_operation("COMMAND", f"Command executed, {affected_rows} rows affected")
            return affected_rows
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            self.connection.rollback()
            raise
            
    def call_sproc(self, sproc: str, params: Optional[List[Any]] = None) -> Union[List[Dict[str, Any]], str]:
        """
        Invokes a stored procedure (or function) in Redshift with optional positional parameters
        Uses SQL: CALL <sproc>(<params>)
        
        Args:
            sproc: Name of the stored procedure
            params: Optional list of parameters for the stored procedure
            
        Returns:
            Result set as list of dictionaries or success message if no results
        """
        try:
            # Format parameters for the CALL statement
            param_str = self.format_params(params) if params else ""
            call_sql = f"CALL {sproc}({param_str})"
            
            logger.info(f"Executing stored procedure: {call_sql}")
            self.cursor.execute(call_sql)
            
            # Try to fetch results (some procedures return data, others don't)
            try:
                results = self.cursor.fetchall()
                if results:
                    result_list = [dict(row) for row in results]
                    self.log_operation("CALL_SPROC", f"Stored procedure {sproc} executed, {len(result_list)} rows returned")
                    return result_list
                else:
                    self.log_operation("CALL_SPROC", f"Stored procedure {sproc} executed successfully, no results returned")
                    return f"Stored procedure {sproc} executed successfully"
            except psycopg2.ProgrammingError:
                # No results to fetch (procedure didn't return data)
                self.connection.commit()
                self.log_operation("CALL_SPROC", f"Stored procedure {sproc} executed successfully")
                return f"Stored procedure {sproc} executed successfully"
                
        except Exception as e:
            logger.error(f"Stored procedure execution failed: {e}")
            self.connection.rollback()
            raise
            
    def add_partition(self, params: Dict[str, Any]) -> str:
        """
        Constructs and executes an ALTER TABLE ... ADD IF NOT EXISTS PARTITION ... LOCATION ... SQL command
        to add partitions to an external schema table in Redshift, based on S3 folder structure
        
        Args:
            params: Dictionary containing partition parameters:
                - schema: Schema name
                - table: Table name
                - partition_sql: Partition definition SQL (e.g., "year=2023, month=01")
                - bucket: S3 bucket name
                - folder: Folder path in S3
                - file: File path in S3
                - partition_str: Partition string for S3 path
                
        Returns:
            Success message
        """
        try:
            # Parse and validate parameters
            parsed_params = self.parse_partition_params(params)
            
            # Build S3 location
            location = self.build_partition_location(
                parsed_params['bucket'],
                parsed_params['folder'],
                parsed_params['file'],
                parsed_params['partition_str']
            )
            
            # Construct ALTER TABLE SQL
            alter_sql = f"""
            ALTER TABLE {parsed_params['schema']}.{parsed_params['table']} 
            ADD IF NOT EXISTS PARTITION ({parsed_params['partition_sql']}) 
            LOCATION '{location}'
            """
            
            logger.info(f"Adding partition to table {parsed_params['schema']}.{parsed_params['table']}")
            logger.info(f"Partition SQL: {parsed_params['partition_sql']}")
            logger.info(f"Location: {location}")
            
            # Execute the ALTER TABLE command
            affected_rows = self.execute_command(alter_sql)
            
            success_msg = f"Partition added successfully to {parsed_params['schema']}.{parsed_params['table']}"
            self.log_operation("ADD_PARTITION", success_msg)
            return success_msg
            
        except Exception as e:
            logger.error(f"Add partition operation failed: {e}")
            raise
            
    def create_external_schema(self, schema_name: str, iam_role: str, database_name: str) -> str:
        """
        Create external schema for Redshift Spectrum
        
        Args:
            schema_name: Name of the external schema
            iam_role: IAM role ARN for accessing S3
            database_name: Glue catalog database name
            
        Returns:
            Success message
        """
        try:
            create_schema_sql = f"""
            CREATE EXTERNAL SCHEMA IF NOT EXISTS {schema_name}
            FROM DATA CATALOG
            DATABASE '{database_name}'
            IAM_ROLE '{iam_role}'
            """
            
            self.execute_command(create_schema_sql)
            success_msg = f"External schema {schema_name} created successfully"
            self.log_operation("CREATE_EXTERNAL_SCHEMA", success_msg)
            return success_msg
            
        except Exception as e:
            logger.error(f"Create external schema failed: {e}")
            raise
            
    def get_table_info(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """
        Get information about a table including columns and data types
        
        Args:
            schema: Schema name
            table: Table name
            
        Returns:
            List of column information dictionaries
        """
        try:
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            
            results = self.execute_query(query, [schema, table])
            self.log_operation("GET_TABLE_INFO", f"Retrieved info for {schema}.{table}")
            return results
            
        except Exception as e:
            logger.error(f"Get table info failed: {e}")
            raise
            
    def check_partition_exists(self, schema: str, table: str, partition_sql: str) -> bool:
        """
        Check if a partition already exists for an external table
        
        Args:
            schema: Schema name
            table: Table name
            partition_sql: Partition definition SQL
            
        Returns:
            True if partition exists, False otherwise
        """
        try:
            # Query system views to check for existing partitions
            query = """
            SELECT COUNT(*) as partition_count
            FROM SVV_EXTERNAL_PARTITIONS
            WHERE schemaname = %s 
            AND tablename = %s
            AND values = %s
            """
            
            results = self.execute_query(query, [schema, table, partition_sql])
            exists = results[0]['partition_count'] > 0 if results else False
            
            self.log_operation("CHECK_PARTITION", f"Partition exists check for {schema}.{table}: {exists}")
            return exists
            
        except Exception as e:
            logger.warning(f"Check partition exists failed: {e}")
            return False
            
    def get_cluster_info(self) -> Dict[str, Any]:
        """
        Get Redshift cluster information
        
        Returns:
            Dictionary with cluster information
        """
        try:
            query = """
            SELECT 
                version() as version,
                current_database() as database,
                current_schema() as schema,
                current_user as user
            """
            
            results = self.execute_query(query)
            cluster_info = results[0] if results else {}
            self.log_operation("GET_CLUSTER_INFO", "Retrieved cluster information")
            return cluster_info
            
        except Exception as e:
            logger.error(f"Get cluster info failed: {e}")
            raise