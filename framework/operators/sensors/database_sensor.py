# Database Sensor Operators - Monitor database conditions
"""
Database sensor operators for monitoring database conditions including:
- Table existence and row count monitoring
- Query result monitoring
- Database connection health checks
- Data freshness monitoring
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook

from ..base_operator import BaseInsightAirOperator

logger = logging.getLogger(__name__)


class DatabaseSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that monitors database conditions using SQL queries
    """
    
    template_fields = ('sql', 'conn_id')
    
    def __init__(self,
                 sql: str,
                 conn_id: str,
                 expected_result: Optional[Any] = None,
                 comparison_operator: str = 'equals',
                 min_row_count: Optional[int] = None,
                 max_row_count: Optional[int] = None,
                 hook_params: Optional[Dict[str, Any]] = None,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize DatabaseSensor
        
        Args:
            sql: SQL query to execute
            conn_id: Airflow connection ID for database
            expected_result: Expected query result for comparison
            comparison_operator: How to compare result ('equals', 'not_equals', 'greater_than', etc.)
            min_row_count: Minimum number of rows expected
            max_row_count: Maximum number of rows expected
            hook_params: Additional parameters for database hook
            poke_interval: Time between checks in seconds
            timeout: Timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.expected_result = expected_result
        self.comparison_operator = comparison_operator
        self.min_row_count = min_row_count
        self.max_row_count = max_row_count
        self.hook_params = hook_params or {}
    
    def poke(self, context: Context) -> bool:
        """
        Check if database condition is met
        
        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            # Get database hook based on connection type
            hook = self._get_database_hook()
            
            # Execute query
            result = hook.get_records(self.sql)
            
            # Check conditions
            if self._check_row_count_conditions(result, context):
                if self._check_result_conditions(result, context):
                    logger.info(f"Database condition satisfied for query: {self.sql}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"DatabaseSensor poke failed: {str(e)}")
            return False
    
    def _get_database_hook(self):
        """Get appropriate database hook based on connection type"""
        connection = BaseHook.get_connection(self.conn_id)
        conn_type = connection.conn_type.lower()
        
        if conn_type in ['postgres', 'postgresql']:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            return PostgresHook(postgres_conn_id=self.conn_id, **self.hook_params)
        elif conn_type in ['mysql']:
            from airflow.providers.mysql.hooks.mysql import MySqlHook
            return MySqlHook(mysql_conn_id=self.conn_id, **self.hook_params)
        elif conn_type in ['sqlite']:
            from airflow.providers.sqlite.hooks.sqlite import SqliteHook
            return SqliteHook(sqlite_conn_id=self.conn_id, **self.hook_params)
        elif conn_type in ['mssql', 'sqlserver']:
            from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
            return MsSqlHook(mssql_conn_id=self.conn_id, **self.hook_params)
        elif conn_type in ['oracle']:
            from airflow.providers.oracle.hooks.oracle import OracleHook
            return OracleHook(oracle_conn_id=self.conn_id, **self.hook_params)
        else:
            # Generic SQL hook
            from airflow.hooks.dbapi import DbApiHook
            return DbApiHook(conn_id=self.conn_id, **self.hook_params)
    
    def _check_row_count_conditions(self, result: List, context: Context) -> bool:
        """Check row count conditions"""
        row_count = len(result)
        
        if self.min_row_count is not None and row_count < self.min_row_count:
            logger.debug(f"Row count {row_count} < minimum {self.min_row_count}")
            return False
        
        if self.max_row_count is not None and row_count > self.max_row_count:
            logger.debug(f"Row count {row_count} > maximum {self.max_row_count}")
            return False
        
        # Store row count in XCom
        context['task_instance'].xcom_push(key='row_count', value=row_count)
        
        return True
    
    def _check_result_conditions(self, result: List, context: Context) -> bool:
        """Check result value conditions"""
        if self.expected_result is None:
            return True
        
        # For single row, single column results
        if len(result) == 1 and len(result[0]) == 1:
            actual_value = result[0][0]
        else:
            actual_value = result
        
        # Store actual result in XCom
        context['task_instance'].xcom_push(key='query_result', value=actual_value)
        
        # Compare based on operator
        if self.comparison_operator == 'equals':
            return actual_value == self.expected_result
        elif self.comparison_operator == 'not_equals':
            return actual_value != self.expected_result
        elif self.comparison_operator == 'greater_than':
            return actual_value > self.expected_result
        elif self.comparison_operator == 'less_than':
            return actual_value < self.expected_result
        elif self.comparison_operator == 'greater_equal':
            return actual_value >= self.expected_result
        elif self.comparison_operator == 'less_equal':
            return actual_value <= self.expected_result
        elif self.comparison_operator == 'contains':
            return self.expected_result in str(actual_value)
        elif self.comparison_operator == 'not_null':
            return actual_value is not None
        else:
            logger.warning(f"Unknown comparison operator: {self.comparison_operator}")
            return False


class TableSensor(BaseInsightAirOperator, BaseSensorOperator):
    """
    Sensor that monitors table-specific conditions
    """
    
    template_fields = ('table_name', 'schema', 'conn_id')
    
    def __init__(self,
                 table_name: str,
                 conn_id: str,
                 schema: Optional[str] = None,
                 check_existence: bool = True,
                 min_rows: Optional[int] = None,
                 max_age_hours: Optional[int] = None,
                 timestamp_column: Optional[str] = None,
                 where_clause: Optional[str] = None,
                 poke_interval: int = 60,
                 timeout: int = 60 * 60 * 24,
                 **kwargs):
        """
        Initialize TableSensor
        
        Args:
            table_name: Name of table to monitor
            conn_id: Airflow connection ID for database
            schema: Database schema name
            check_existence: Whether to check if table exists
            min_rows: Minimum number of rows expected in table
            max_age_hours: Maximum age of newest data in hours
            timestamp_column: Column to use for age checking
            where_clause: Optional WHERE clause for row counting
            poke_interval: Time between checks in seconds
            timeout: Timeout in seconds
        """
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.table_name = table_name
        self.conn_id = conn_id
        self.schema = schema
        self.check_existence = check_existence
        self.min_rows = min_rows
        self.max_age_hours = max_age_hours
        self.timestamp_column = timestamp_column
        self.where_clause = where_clause
    
    def poke(self, context: Context) -> bool:
        """
        Check if table condition is met
        
        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            hook = self._get_database_hook()
            
            # Check table existence
            if self.check_existence and not self._table_exists(hook):
                logger.debug(f"Table does not exist: {self._get_full_table_name()}")
                return False
            
            # Check row count
            if self.min_rows is not None and not self._check_row_count(hook, context):
                return False
            
            # Check data freshness
            if self.max_age_hours is not None and not self._check_data_freshness(hook, context):
                return False
            
            logger.info(f"Table condition satisfied: {self._get_full_table_name()}")
            return True
            
        except Exception as e:
            logger.error(f"TableSensor poke failed: {str(e)}")
            return False
    
    def _get_database_hook(self):
        """Get appropriate database hook"""
        connection = BaseHook.get_connection(self.conn_id)
        conn_type = connection.conn_type.lower()
        
        if conn_type in ['postgres', 'postgresql']:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            return PostgresHook(postgres_conn_id=self.conn_id)
        elif conn_type in ['mysql']:
            from airflow.providers.mysql.hooks.mysql import MySqlHook
            return MySqlHook(mysql_conn_id=self.conn_id)
        else:
            from airflow.hooks.dbapi import DbApiHook
            return DbApiHook(conn_id=self.conn_id)
    
    def _get_full_table_name(self) -> str:
        """Get full table name with schema if provided"""
        if self.schema:
            return f"{self.schema}.{self.table_name}"
        return self.table_name
    
    def _table_exists(self, hook) -> bool:
        """Check if table exists"""
        try:
            # Try to query the table metadata
            if hasattr(hook, 'table_exists'):
                return hook.table_exists(self.table_name, self.schema)
            else:
                # Generic check using information_schema
                sql = f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{self.table_name}'
                """
                if self.schema:
                    sql += f" AND table_schema = '{self.schema}'"
                
                result = hook.get_records(sql)
                return len(result) > 0
        except Exception as e:
            logger.debug(f"Error checking table existence: {e}")
            return False
    
    def _check_row_count(self, hook, context: Context) -> bool:
        """Check minimum row count"""
        try:
            where_part = f" WHERE {self.where_clause}" if self.where_clause else ""
            sql = f"SELECT COUNT(*) FROM {self._get_full_table_name()}{where_part}"
            
            result = hook.get_records(sql)
            row_count = result[0][0] if result else 0
            
            # Store row count in XCom
            context['task_instance'].xcom_push(key='table_row_count', value=row_count)
            
            if row_count < self.min_rows:
                logger.debug(f"Row count {row_count} < minimum {self.min_rows}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking row count: {e}")
            return False
    
    def _check_data_freshness(self, hook, context: Context) -> bool:
        """Check data freshness based on timestamp column"""
        if not self.timestamp_column:
            logger.warning("timestamp_column not specified for freshness check")
            return True
        
        try:
            sql = f"""
            SELECT MAX({self.timestamp_column}) 
            FROM {self._get_full_table_name()}
            """
            
            result = hook.get_records(sql)
            if not result or not result[0][0]:
                logger.debug("No timestamp data found in table")
                return False
            
            latest_timestamp = result[0][0]
            if isinstance(latest_timestamp, str):
                latest_timestamp = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
            
            # Calculate age
            age = datetime.now() - latest_timestamp.replace(tzinfo=None)
            max_age = timedelta(hours=self.max_age_hours)
            
            # Store freshness info in XCom
            context['task_instance'].xcom_push(
                key='data_freshness',
                value={
                    'latest_timestamp': latest_timestamp.isoformat(),
                    'age_hours': age.total_seconds() / 3600,
                    'is_fresh': age <= max_age
                }
            )
            
            if age > max_age:
                logger.debug(f"Data age {age} > maximum {max_age}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {e}")
            return False


# Example configuration usage:
"""
tasks:
  - name: "wait_for_user_data"
    type: "DATABASE_SENSOR"
    description: "Wait for new user records"
    properties:
      conn_id: "postgres_default"
      sql: "SELECT COUNT(*) FROM users WHERE created_at >= CURRENT_DATE"
      min_row_count: 1
      poke_interval: 300
      timeout: 3600

  - name: "check_table_exists"
    type: "TABLE_SENSOR"
    description: "Ensure daily_reports table exists with data"
    properties:
      conn_id: "mysql_default"
      table_name: "daily_reports"
      schema: "analytics"
      check_existence: true
      min_rows: 100
      max_age_hours: 25
      timestamp_column: "report_date"
      poke_interval: 120

  - name: "validate_data_quality"
    type: "DATABASE_SENSOR"
    description: "Check data quality metrics"
    properties:
      conn_id: "postgres_default"
      sql: |
        SELECT 
          COUNT(*) as total_rows,
          COUNT(CASE WHEN email IS NULL THEN 1 END) as null_emails
        FROM customer_data 
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      expected_result: [100, 0]
      comparison_operator: "equals"
"""