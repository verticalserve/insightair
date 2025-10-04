"""
OCI Autonomous Data Warehouse (ADW) Database Operations
Provides database operations for Oracle Cloud Infrastructure Autonomous Data Warehouse
"""

import logging
from typing import Dict, List, Any, Optional, Union
import cx_Oracle
from .base import BaseDatabaseOperations

logger = logging.getLogger(__name__)


class OCIADWOperations(BaseDatabaseOperations):
    """
    OCI Autonomous Data Warehouse database operations class
    Handles connections and operations specific to Oracle ADW
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize OCI ADW connection parameters
        
        Args:
            connection_params: Dictionary containing ADW connection parameters
                - user: Database username
                - password: Database password
                - dsn: Data source name (connection string)
                - wallet_location: Path to wallet files (for secure connections)
                - wallet_password: Wallet password
                - service_name: Service name for the ADW instance
        """
        super().__init__(connection_params)
        self.user = connection_params.get('user')
        self.password = connection_params.get('password')
        self.dsn = connection_params.get('dsn')
        self.wallet_location = connection_params.get('wallet_location')
        self.wallet_password = connection_params.get('wallet_password')
        self.service_name = connection_params.get('service_name')
        
        # Initialize Oracle client if wallet is provided
        if self.wallet_location:
            try:
                cx_Oracle.init_oracle_client(config_dir=self.wallet_location)
                logger.info(f"Oracle client initialized with wallet: {self.wallet_location}")
            except Exception as e:
                logger.warning(f"Failed to initialize Oracle client with wallet: {e}")
        
    def connect(self) -> bool:
        """
        Establish connection to OCI ADW
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.dsn:
                # Use DSN connection string
                self.connection = cx_Oracle.connect(
                    user=self.user,
                    password=self.password,
                    dsn=self.dsn
                )
            elif self.service_name and self.wallet_location:
                # Use service name with wallet
                dsn = cx_Oracle.makedsn(
                    host=None,
                    port=None,
                    service_name=self.service_name
                )
                self.connection = cx_Oracle.connect(
                    user=self.user,
                    password=self.password,
                    dsn=dsn
                )
            else:
                raise ValueError("Either DSN or service_name with wallet_location must be provided")
                
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to OCI ADW: {self.service_name or self.dsn}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to OCI ADW: {e}")
            return False
            
    def disconnect(self):
        """
        Close OCI ADW connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from OCI ADW")
        
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
            
            # Get column names
            columns = [desc[0] for desc in self.cursor.description]
            
            # Fetch all results
            rows = self.cursor.fetchall()
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    # Handle Oracle-specific data types
                    if isinstance(value, cx_Oracle.LOB):
                        row_dict[columns[i]] = value.read()
                    else:
                        row_dict[columns[i]] = value
                results.append(row_dict)
                
            self.log_operation("SELECT", f"Query executed, {len(results)} rows returned")
            return results
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
        Invokes a stored procedure (or function) in Oracle ADW with optional positional parameters
        Uses SQL: CALL <sproc>(<params>) or BEGIN <sproc>(<params>); END;
        
        Args:
            sproc: Name of the stored procedure
            params: Optional list of parameters for the stored procedure
            
        Returns:
            Result set as list of dictionaries or success message if no results
        """
        try:
            # Format parameters for Oracle procedure call
            if params:
                # Create bind variables for parameters
                bind_vars = {}
                param_placeholders = []
                
                for i, param in enumerate(params):
                    var_name = f"param_{i}"
                    bind_vars[var_name] = param
                    param_placeholders.append(f":{var_name}")
                
                param_str = ", ".join(param_placeholders)
            else:
                param_str = ""
                bind_vars = {}
            
            # Use PL/SQL block for procedure call
            call_sql = f"BEGIN {sproc}({param_str}); END;"
            
            logger.info(f"Executing stored procedure: {call_sql}")
            
            if bind_vars:
                self.cursor.execute(call_sql, bind_vars)
            else:
                self.cursor.execute(call_sql)
            
            self.connection.commit()
            
            # Check if procedure returned results via cursor
            try:
                if self.cursor.description:
                    columns = [desc[0] for desc in self.cursor.description]
                    rows = self.cursor.fetchall()
                    
                    results = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            if isinstance(value, cx_Oracle.LOB):
                                row_dict[columns[i]] = value.read()
                            else:
                                row_dict[columns[i]] = value
                        results.append(row_dict)
                        
                    self.log_operation("CALL_SPROC", f"Stored procedure {sproc} executed, {len(results)} rows returned")
                    return results
                else:
                    self.log_operation("CALL_SPROC", f"Stored procedure {sproc} executed successfully")
                    return f"Stored procedure {sproc} executed successfully"
            except:
                self.log_operation("CALL_SPROC", f"Stored procedure {sproc} executed successfully")
                return f"Stored procedure {sproc} executed successfully"
                
        except Exception as e:
            logger.error(f"Stored procedure execution failed: {e}")
            self.connection.rollback()
            raise
            
    def add_partition(self, params: Dict[str, Any]) -> str:
        """
        Constructs and executes an ALTER TABLE ... ADD PARTITION ... command
        to add partitions to an external table in Oracle ADW
        
        Args:
            params: Dictionary containing partition parameters:
                - schema: Schema name
                - table: Table name
                - partition_name: Partition name
                - partition_sql: Partition definition SQL (e.g., "VALUES LESS THAN ('2023-02-01')")
                - bucket: Object Storage bucket name (optional)
                - folder: Folder path in Object Storage (optional)
                - file: File path in Object Storage (optional)
                - partition_str: Partition string for Object Storage path (optional)
                
        Returns:
            Success message
        """
        try:
            # Parse and validate parameters
            schema = params.get('schema')
            table = params.get('table')
            partition_name = params.get('partition_name')
            partition_sql = params.get('partition_sql')
            
            if not all([schema, table, partition_name, partition_sql]):
                raise ValueError("Missing required parameters: schema, table, partition_name, partition_sql")
            
            # Build ALTER TABLE SQL for Oracle
            if 'bucket' in params and 'folder' in params:
                # External table with Object Storage location
                location = self.build_oci_storage_location(
                    params.get('bucket'),
                    params.get('folder'),
                    params.get('file', ''),
                    params.get('partition_str', '')
                )
                
                alter_sql = f"""
                ALTER TABLE {schema}.{table} 
                ADD PARTITION {partition_name} {partition_sql}
                LOCATION ('{location}')
                """
            else:
                # Regular partitioned table
                alter_sql = f"""
                ALTER TABLE {schema}.{table} 
                ADD PARTITION {partition_name} {partition_sql}
                """
            
            logger.info(f"Adding partition to table {schema}.{table}")
            logger.info(f"Partition name: {partition_name}")
            logger.info(f"Partition SQL: {partition_sql}")
            
            # Execute the ALTER TABLE command
            affected_rows = self.execute_command(alter_sql)
            
            success_msg = f"Partition {partition_name} added successfully to {schema}.{table}"
            self.log_operation("ADD_PARTITION", success_msg)
            return success_msg
            
        except Exception as e:
            logger.error(f"Add partition operation failed: {e}")
            raise
            
    def build_oci_storage_location(self, bucket: str, folder: str, file: str, partition_str: str) -> str:
        """
        Build OCI Object Storage location string for partition
        
        Args:
            bucket: Object Storage bucket name
            folder: Folder path
            file: File path
            partition_str: Partition string
            
        Returns:
            Complete Object Storage location URL
        """
        # Ensure proper path formatting
        parts = [folder.strip('/'), file.strip('/'), partition_str.strip('/')]
        path = '/'.join(part for part in parts if part)
        
        # OCI Object Storage URL format
        return f"https://objectstorage.{self.connection_params.get('region', 'us-phoenix-1')}.oraclecloud.com/n/{self.connection_params.get('namespace')}/b/{bucket}/o/{path}"
        
    def create_external_table(self, schema: str, table: str, columns: List[Dict[str, str]], 
                             location: str, file_format: str = 'CSV') -> str:
        """
        Create external table for accessing data in OCI Object Storage
        
        Args:
            schema: Schema name
            table: Table name
            columns: List of column definitions [{'name': 'col1', 'type': 'VARCHAR2(100)'}]
            location: Object Storage location
            file_format: File format (CSV, JSON, PARQUET)
            
        Returns:
            Success message
        """
        try:
            # Build column definitions
            column_defs = []
            for col in columns:
                column_defs.append(f"{col['name']} {col['type']}")
            columns_sql = ",\n    ".join(column_defs)
            
            # Create external table SQL
            create_sql = f"""
            CREATE TABLE {schema}.{table} (
                {columns_sql}
            )
            ORGANIZATION EXTERNAL (
                TYPE ORACLE_LOADER
                DEFAULT DIRECTORY DATA_PUMP_DIR
                ACCESS PARAMETERS (
                    RECORDS DELIMITED BY NEWLINE
                    FIELDS TERMINATED BY ','
                    MISSING FIELD VALUES ARE NULL
                )
                LOCATION ('{location}')
            )
            PARALLEL
            REJECT LIMIT UNLIMITED
            """
            
            self.execute_command(create_sql)
            success_msg = f"External table {schema}.{table} created successfully"
            self.log_operation("CREATE_EXTERNAL_TABLE", success_msg)
            return success_msg
            
        except Exception as e:
            logger.error(f"Create external table failed: {e}")
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
                COLUMN_NAME,
                DATA_TYPE,
                DATA_LENGTH,
                DATA_PRECISION,
                DATA_SCALE,
                NULLABLE,
                DATA_DEFAULT
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = UPPER(:schema) AND TABLE_NAME = UPPER(:table)
            ORDER BY COLUMN_ID
            """
            
            results = self.execute_query(query, [schema.upper(), table.upper()])
            self.log_operation("GET_TABLE_INFO", f"Retrieved info for {schema}.{table}")
            return results
            
        except Exception as e:
            logger.error(f"Get table info failed: {e}")
            raise
            
    def check_partition_exists(self, schema: str, table: str, partition_name: str) -> bool:
        """
        Check if a partition already exists for a table
        
        Args:
            schema: Schema name
            table: Table name
            partition_name: Partition name
            
        Returns:
            True if partition exists, False otherwise
        """
        try:
            query = """
            SELECT COUNT(*) as partition_count
            FROM ALL_TAB_PARTITIONS
            WHERE TABLE_OWNER = UPPER(:schema) 
            AND TABLE_NAME = UPPER(:table)
            AND PARTITION_NAME = UPPER(:partition_name)
            """
            
            results = self.execute_query(query, [schema.upper(), table.upper(), partition_name.upper()])
            exists = results[0]['PARTITION_COUNT'] > 0 if results else False
            
            self.log_operation("CHECK_PARTITION", f"Partition exists check for {schema}.{table}.{partition_name}: {exists}")
            return exists
            
        except Exception as e:
            logger.warning(f"Check partition exists failed: {e}")
            return False
            
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get Oracle ADW database information
        
        Returns:
            Dictionary with database information
        """
        try:
            query = """
            SELECT 
                banner as version,
                SYS_CONTEXT('USERENV', 'DB_NAME') as database_name,
                SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') as current_schema,
                USER as current_user,
                SYS_CONTEXT('USERENV', 'SERVICE_NAME') as service_name
            FROM v$version 
            WHERE banner LIKE 'Oracle%'
            """
            
            results = self.execute_query(query)
            db_info = results[0] if results else {}
            self.log_operation("GET_DATABASE_INFO", "Retrieved database information")
            return db_info
            
        except Exception as e:
            logger.error(f"Get database info failed: {e}")
            raise
            
    def execute_plsql_block(self, plsql_block: str, bind_vars: Optional[Dict[str, Any]] = None) -> str:
        """
        Execute a PL/SQL block
        
        Args:
            plsql_block: PL/SQL block to execute
            bind_vars: Optional bind variables dictionary
            
        Returns:
            Success message
        """
        try:
            if bind_vars:
                self.cursor.execute(plsql_block, bind_vars)
            else:
                self.cursor.execute(plsql_block)
                
            self.connection.commit()
            success_msg = "PL/SQL block executed successfully"
            self.log_operation("EXECUTE_PLSQL", success_msg)
            return success_msg
            
        except Exception as e:
            logger.error(f"PL/SQL block execution failed: {e}")
            self.connection.rollback()
            raise