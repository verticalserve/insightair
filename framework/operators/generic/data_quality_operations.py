# Generic Data Quality Operations - Multi-cloud data validation and quality checks
"""
Generic data quality operations that work across cloud environments:
- Data validation rules and checks
- Schema validation and profiling
- Data completeness and accuracy checks
- Automated data quality reporting
- Integration with cloud data quality services
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Union, Callable
from abc import ABC, abstractmethod
import json
import re
from datetime import datetime, date
from decimal import Decimal

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

from ..base_operator import BaseInsightAirOperator
from ...core.generic_task_system import (
    get_environment_context, get_connection_resolver, get_task_type_registry,
    CloudProvider, DatabaseType
)

logger = logging.getLogger(__name__)


class BaseDataQualityOperator(BaseInsightAirOperator, ABC):
    """Base class for data quality operations"""
    
    def __init__(self,
                 database_connection_id: Optional[str] = None,
                 **kwargs):
        """
        Initialize BaseDataQualityOperator
        
        Args:
            database_connection_id: Database connection ID (auto-resolved if not provided)
        """
        super().__init__(**kwargs)
        self.database_connection_id = database_connection_id
    
    @abstractmethod
    def execute_data_quality_operation(self, context: Context) -> Any:
        """Execute the data quality operation"""
        pass
    
    def execute(self, context: Context) -> Any:
        """Execute with automatic configuration"""
        return self.execute_data_quality_operation(context)


class DataValidationOperator(BaseDataQualityOperator):
    """Generic data validation operator with configurable rules"""
    
    template_fields = ('data_source', 'validation_rules', 'sql_query')
    
    def __init__(self,
                 data_source: str,  # sql_query, file_path, table_name
                 source_type: str = 'sql',  # sql, csv, json, parquet, table
                 validation_rules: List[Dict[str, Any]] = None,
                 sql_query: Optional[str] = None,
                 table_name: Optional[str] = None,
                 file_path: Optional[str] = None,
                 schema: Optional[str] = None,
                 fail_on_error: bool = True,
                 quality_threshold: float = 0.95,  # Minimum pass rate for quality checks
                 **kwargs):
        """
        Initialize DataValidationOperator
        
        Args:
            data_source: Data source identifier (query, file path, table name)
            source_type: Type of data source (sql, csv, json, parquet, table)
            validation_rules: List of validation rule dictionaries
            sql_query: SQL query to fetch data
            table_name: Table name for direct table validation
            file_path: File path for file-based validation
            schema: Database schema name
            fail_on_error: Whether to fail the task on validation errors
            quality_threshold: Minimum pass rate (0.0-1.0) for validation to pass
        """
        super().__init__(**kwargs)
        self.data_source = data_source
        self.source_type = source_type.lower()
        self.validation_rules = validation_rules or []
        self.sql_query = sql_query
        self.table_name = table_name
        self.file_path = file_path
        self.schema = schema
        self.fail_on_error = fail_on_error
        self.quality_threshold = quality_threshold
    
    def execute_data_quality_operation(self, context: Context) -> Dict[str, Any]:
        """Execute data validation operation"""
        
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
        """Execute on AWS (may use AWS Glue Data Quality)"""
        result = self._execute_generic(context)
        result['platform'] = 'aws'
        
        # AWS-specific enhancements could be added here
        # e.g., AWS Glue Data Quality, AWS Deequ integration
        
        return result
    
    def _execute_gcp(self, context: Context) -> Dict[str, Any]:
        """Execute on GCP (may use Cloud Data Quality)"""
        result = self._execute_generic(context)
        result['platform'] = 'gcp'
        return result
    
    def _execute_azure(self, context: Context) -> Dict[str, Any]:
        """Execute on Azure (may use Azure Data Factory Data Quality)"""
        result = self._execute_generic(context)
        result['platform'] = 'azure'
        return result
    
    def _execute_oci(self, context: Context) -> Dict[str, Any]:
        """Execute on OCI"""
        result = self._execute_generic(context)
        result['platform'] = 'oci'
        return result
    
    def _execute_generic(self, context: Context) -> Dict[str, Any]:
        """Generic data validation execution"""
        
        logger.info(f"Starting data validation for {self.source_type} source: {self.data_source}")
        
        # Load data based on source type
        data = self._load_data()
        
        if data is None or len(data) == 0:
            logger.warning("No data found for validation")
            return {
                'source': self.data_source,
                'source_type': self.source_type,
                'total_records': 0,
                'rules_executed': 0,
                'rules_passed': 0,
                'rules_failed': 0,
                'pass_rate': 0.0,
                'validation_passed': False,
                'success': True,
                'message': 'No data to validate'
            }
        
        logger.info(f"Loaded {len(data)} records for validation")
        
        # Execute validation rules
        validation_results = self._execute_validation_rules(data)
        
        # Calculate overall results
        total_rules = len(validation_results)
        passed_rules = sum(1 for result in validation_results if result['passed'])
        failed_rules = total_rules - passed_rules
        pass_rate = passed_rules / total_rules if total_rules > 0 else 0.0
        
        validation_passed = pass_rate >= self.quality_threshold
        
        # Store detailed results in XCom
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        context['task_instance'].xcom_push(key='quality_metrics', value={
            'pass_rate': pass_rate,
            'total_records': len(data),
            'rules_passed': passed_rules,
            'rules_failed': failed_rules
        })
        
        # Log results
        logger.info(f"Validation completed: {passed_rules}/{total_rules} rules passed ({pass_rate:.2%})")
        
        if not validation_passed:
            error_msg = f"Data quality validation failed: {pass_rate:.2%} < {self.quality_threshold:.2%} threshold"
            logger.error(error_msg)
            
            if self.fail_on_error:
                raise Exception(error_msg)
        
        return {
            'source': self.data_source,
            'source_type': self.source_type,
            'total_records': len(data),
            'rules_executed': total_rules,
            'rules_passed': passed_rules,
            'rules_failed': failed_rules,
            'pass_rate': pass_rate,
            'quality_threshold': self.quality_threshold,
            'validation_passed': validation_passed,
            'success': True,
            'validation_results': validation_results
        }
    
    def _load_data(self) -> Optional[pd.DataFrame]:
        """Load data based on source type"""
        
        if self.source_type == 'sql':
            return self._load_data_from_sql()
        elif self.source_type == 'table':
            return self._load_data_from_table()
        elif self.source_type == 'csv':
            return self._load_data_from_csv()
        elif self.source_type == 'json':
            return self._load_data_from_json()
        elif self.source_type == 'parquet':
            return self._load_data_from_parquet()
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")
    
    def _load_data_from_sql(self) -> Optional[pd.DataFrame]:
        """Load data from SQL query"""
        try:
            from ...operators.generic.database_operations import BaseDatabaseOperator
            
            # Create a temporary database operator to get the hook
            db_operator = BaseDatabaseOperator(database_connection_id=self.database_connection_id)
            hook = db_operator.get_database_hook()
            
            # Execute query and get data
            if hasattr(hook, 'get_pandas_df'):
                return hook.get_pandas_df(sql=self.sql_query or self.data_source)
            else:
                records = hook.get_records(sql=self.sql_query or self.data_source)
                if records:
                    # Convert to DataFrame (this is a simplified approach)
                    return pd.DataFrame(records)
                return None
                
        except Exception as e:
            logger.error(f"Failed to load data from SQL: {e}")
            raise
    
    def _load_data_from_table(self) -> Optional[pd.DataFrame]:
        """Load data from table"""
        table_name = self.table_name or self.data_source
        schema_prefix = f"{self.schema}." if self.schema else ""
        query = f"SELECT * FROM {schema_prefix}{table_name}"
        
        # Temporarily set sql_query and use SQL loading
        original_query = self.sql_query
        self.sql_query = query
        try:
            return self._load_data_from_sql()
        finally:
            self.sql_query = original_query
    
    def _load_data_from_csv(self) -> Optional[pd.DataFrame]:
        """Load data from CSV file"""
        file_path = self.file_path or self.data_source
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"Failed to load CSV file {file_path}: {e}")
            raise
    
    def _load_data_from_json(self) -> Optional[pd.DataFrame]:
        """Load data from JSON file"""
        file_path = self.file_path or self.data_source
        try:
            return pd.read_json(file_path)
        except Exception as e:
            logger.error(f"Failed to load JSON file {file_path}: {e}")
            raise
    
    def _load_data_from_parquet(self) -> Optional[pd.DataFrame]:
        """Load data from Parquet file"""
        file_path = self.file_path or self.data_source
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            logger.error(f"Failed to load Parquet file {file_path}: {e}")
            raise
    
    def _execute_validation_rules(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Execute all validation rules on data"""
        
        results = []
        
        for rule in self.validation_rules:
            try:
                result = self._execute_single_validation_rule(data, rule)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to execute validation rule {rule.get('name', 'unnamed')}: {e}")
                results.append({
                    'rule_name': rule.get('name', 'unnamed'),
                    'rule_type': rule.get('type', 'unknown'),
                    'passed': False,
                    'error': str(e),
                    'details': {}
                })
        
        return results
    
    def _execute_single_validation_rule(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single validation rule"""
        
        rule_name = rule.get('name', 'unnamed')
        rule_type = rule.get('type', '').lower()
        
        logger.debug(f"Executing validation rule: {rule_name} ({rule_type})")
        
        if rule_type == 'not_null':
            return self._validate_not_null(data, rule)
        elif rule_type == 'unique':
            return self._validate_unique(data, rule)
        elif rule_type == 'range':
            return self._validate_range(data, rule)
        elif rule_type == 'pattern':
            return self._validate_pattern(data, rule)
        elif rule_type == 'custom':
            return self._validate_custom(data, rule)
        elif rule_type == 'completeness':
            return self._validate_completeness(data, rule)
        elif rule_type == 'consistency':
            return self._validate_consistency(data, rule)
        elif rule_type == 'freshness':
            return self._validate_freshness(data, rule)
        else:
            raise ValueError(f"Unsupported validation rule type: {rule_type}")
    
    def _validate_not_null(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that columns are not null"""
        columns = rule.get('columns', [])
        if isinstance(columns, str):
            columns = [columns]
        
        total_violations = 0
        column_results = {}
        
        for column in columns:
            if column in data.columns:
                null_count = data[column].isnull().sum()
                column_results[column] = {
                    'null_count': int(null_count),
                    'total_count': len(data),
                    'null_percentage': float(null_count / len(data)) if len(data) > 0 else 0.0
                }
                total_violations += null_count
            else:
                logger.warning(f"Column '{column}' not found in data")
        
        passed = total_violations == 0
        
        return {
            'rule_name': rule.get('name', 'not_null'),
            'rule_type': 'not_null',
            'passed': passed,
            'total_violations': int(total_violations),
            'details': column_results
        }
    
    def _validate_unique(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that columns have unique values"""
        columns = rule.get('columns', [])
        if isinstance(columns, str):
            columns = [columns]
        
        duplicates = data.duplicated(subset=columns).sum()
        passed = duplicates == 0
        
        return {
            'rule_name': rule.get('name', 'unique'),
            'rule_type': 'unique',
            'passed': passed,
            'duplicate_count': int(duplicates),
            'total_count': len(data),
            'details': {
                'columns': columns,
                'duplicate_percentage': float(duplicates / len(data)) if len(data) > 0 else 0.0
            }
        }
    
    def _validate_range(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that numeric columns are within specified ranges"""
        column = rule.get('column')
        min_value = rule.get('min_value')
        max_value = rule.get('max_value')
        
        if column not in data.columns:
            raise ValueError(f"Column '{column}' not found in data")
        
        violations = 0
        if min_value is not None:
            violations += (data[column] < min_value).sum()
        if max_value is not None:
            violations += (data[column] > max_value).sum()
        
        passed = violations == 0
        
        return {
            'rule_name': rule.get('name', 'range'),
            'rule_type': 'range',
            'passed': passed,
            'violations': int(violations),
            'total_count': len(data),
            'details': {
                'column': column,
                'min_value': min_value,
                'max_value': max_value,
                'violation_percentage': float(violations / len(data)) if len(data) > 0 else 0.0
            }
        }
    
    def _validate_pattern(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that string columns match specified patterns"""
        column = rule.get('column')
        pattern = rule.get('pattern')
        
        if column not in data.columns:
            raise ValueError(f"Column '{column}' not found in data")
        
        # Apply pattern matching
        matches = data[column].astype(str).str.match(pattern, na=False)
        violations = (~matches).sum()
        passed = violations == 0
        
        return {
            'rule_name': rule.get('name', 'pattern'),
            'rule_type': 'pattern',
            'passed': passed,
            'violations': int(violations),
            'total_count': len(data),
            'details': {
                'column': column,
                'pattern': pattern,
                'violation_percentage': float(violations / len(data)) if len(data) > 0 else 0.0
            }
        }
    
    def _validate_custom(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate using custom function"""
        custom_function = rule.get('function')
        
        if not custom_function:
            raise ValueError("Custom validation rule requires 'function' parameter")
        
        # This is a simplified implementation - in practice, you'd want to resolve
        # the function from a registry or import it dynamically
        try:
            if callable(custom_function):
                result = custom_function(data)
            else:
                # Assume it's a lambda string or similar
                result = eval(custom_function)(data)
            
            if isinstance(result, bool):
                passed = result
                details = {}
            elif isinstance(result, dict):
                passed = result.get('passed', False)
                details = result.get('details', {})
            else:
                passed = bool(result)
                details = {'result': result}
            
            return {
                'rule_name': rule.get('name', 'custom'),
                'rule_type': 'custom',
                'passed': passed,
                'details': details
            }
            
        except Exception as e:
            raise ValueError(f"Custom validation function failed: {e}")
    
    def _validate_completeness(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data completeness"""
        columns = rule.get('columns', [])
        threshold = rule.get('threshold', 0.95)
        
        if isinstance(columns, str):
            columns = [columns]
        
        column_results = {}
        overall_completeness = 0.0
        
        for column in columns:
            if column in data.columns:
                non_null_count = data[column].notna().sum()
                completeness = non_null_count / len(data) if len(data) > 0 else 0.0
                column_results[column] = {
                    'completeness': float(completeness),
                    'non_null_count': int(non_null_count),
                    'total_count': len(data)
                }
                overall_completeness += completeness
        
        if columns:
            overall_completeness /= len(columns)
        
        passed = overall_completeness >= threshold
        
        return {
            'rule_name': rule.get('name', 'completeness'),
            'rule_type': 'completeness',
            'passed': passed,
            'overall_completeness': float(overall_completeness),
            'threshold': float(threshold),
            'details': column_results
        }
    
    def _validate_consistency(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data consistency across columns"""
        # This is a simplified implementation
        # In practice, you'd implement specific consistency checks
        
        consistency_type = rule.get('consistency_type', 'referential')
        
        if consistency_type == 'referential':
            # Check referential consistency between columns
            parent_column = rule.get('parent_column')
            child_column = rule.get('child_column')
            
            if parent_column not in data.columns or child_column not in data.columns:
                raise ValueError(f"Columns '{parent_column}' or '{child_column}' not found")
            
            # Check if all child values exist in parent values
            parent_values = set(data[parent_column].dropna())
            child_values = set(data[child_column].dropna())
            orphaned_values = child_values - parent_values
            
            passed = len(orphaned_values) == 0
            
            return {
                'rule_name': rule.get('name', 'consistency'),
                'rule_type': 'consistency',
                'passed': passed,
                'orphaned_count': len(orphaned_values),
                'details': {
                    'consistency_type': consistency_type,
                    'parent_column': parent_column,
                    'child_column': child_column,
                    'orphaned_values': list(orphaned_values)[:10]  # Limit for logging
                }
            }
        
        return {
            'rule_name': rule.get('name', 'consistency'),
            'rule_type': 'consistency',
            'passed': True,
            'details': {'message': 'Consistency check not implemented for this type'}
        }
    
    def _validate_freshness(self, data: pd.DataFrame, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data freshness"""
        timestamp_column = rule.get('timestamp_column')
        max_age_hours = rule.get('max_age_hours', 24)
        
        if timestamp_column not in data.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' not found")
        
        # Convert to datetime if needed
        timestamps = pd.to_datetime(data[timestamp_column])
        current_time = datetime.now()
        
        # Check how old the data is
        age_hours = (current_time - timestamps.max()).total_seconds() / 3600
        passed = age_hours <= max_age_hours
        
        return {
            'rule_name': rule.get('name', 'freshness'),
            'rule_type': 'freshness',
            'passed': passed,
            'age_hours': float(age_hours),
            'max_age_hours': float(max_age_hours),
            'details': {
                'timestamp_column': timestamp_column,
                'latest_timestamp': timestamps.max().isoformat(),
                'current_time': current_time.isoformat()
            }
        }


class DataProfilingOperator(BaseDataQualityOperator):
    """Generic data profiling operator"""
    
    template_fields = ('data_source', 'sql_query')
    
    def __init__(self,
                 data_source: str,
                 source_type: str = 'sql',
                 sql_query: Optional[str] = None,
                 table_name: Optional[str] = None,
                 file_path: Optional[str] = None,
                 schema: Optional[str] = None,
                 profile_columns: Optional[List[str]] = None,
                 **kwargs):
        """
        Initialize DataProfilingOperator
        
        Args:
            data_source: Data source identifier
            source_type: Type of data source (sql, csv, json, parquet, table)
            sql_query: SQL query to fetch data
            table_name: Table name for direct table profiling
            file_path: File path for file-based profiling
            schema: Database schema name
            profile_columns: Specific columns to profile (None = all columns)
        """
        super().__init__(**kwargs)
        self.data_source = data_source
        self.source_type = source_type.lower()
        self.sql_query = sql_query
        self.table_name = table_name
        self.file_path = file_path
        self.schema = schema
        self.profile_columns = profile_columns
    
    def execute_data_quality_operation(self, context: Context) -> Dict[str, Any]:
        """Execute data profiling operation"""
        
        logger.info(f"Starting data profiling for {self.source_type} source: {self.data_source}")
        
        # Create a temporary validation operator to reuse data loading logic
        validator = DataValidationOperator(
            data_source=self.data_source,
            source_type=self.source_type,
            sql_query=self.sql_query,
            table_name=self.table_name,
            file_path=self.file_path,
            schema=self.schema,
            database_connection_id=self.database_connection_id
        )
        
        # Load data
        data = validator._load_data()
        
        if data is None or len(data) == 0:
            logger.warning("No data found for profiling")
            return {
                'source': self.data_source,
                'source_type': self.source_type,
                'total_records': 0,
                'columns_profiled': 0,
                'profile': {},
                'success': True,
                'message': 'No data to profile'
            }
        
        logger.info(f"Loaded {len(data)} records for profiling")
        
        # Generate profile
        profile = self._generate_data_profile(data)
        
        # Store profile in XCom
        context['task_instance'].xcom_push(key='data_profile', value=profile)
        
        return {
            'source': self.data_source,
            'source_type': self.source_type,
            'total_records': len(data),
            'columns_profiled': len(profile.get('columns', {})),
            'profile': profile,
            'success': True
        }
    
    def _generate_data_profile(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive data profile"""
        
        columns_to_profile = self.profile_columns or data.columns.tolist()
        
        profile = {
            'dataset_summary': {
                'total_rows': len(data),
                'total_columns': len(data.columns),
                'columns_profiled': len(columns_to_profile),
                'memory_usage': data.memory_usage(deep=True).sum(),
                'profiling_timestamp': datetime.now().isoformat()
            },
            'columns': {}
        }
        
        for column in columns_to_profile:
            if column in data.columns:
                profile['columns'][column] = self._profile_column(data[column])
        
        return profile
    
    def _profile_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile a single column"""
        
        column_profile = {
            'name': series.name,
            'dtype': str(series.dtype),
            'count': len(series),
            'null_count': int(series.isnull().sum()),
            'null_percentage': float(series.isnull().sum() / len(series)) if len(series) > 0 else 0.0,
            'unique_count': int(series.nunique()),
            'unique_percentage': float(series.nunique() / len(series)) if len(series) > 0 else 0.0
        }
        
        # Type-specific profiling
        if pd.api.types.is_numeric_dtype(series):
            column_profile.update(self._profile_numeric_column(series))
        elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
            column_profile.update(self._profile_text_column(series))
        elif pd.api.types.is_datetime64_any_dtype(series):
            column_profile.update(self._profile_datetime_column(series))
        elif pd.api.types.is_bool_dtype(series):
            column_profile.update(self._profile_boolean_column(series))
        
        return column_profile
    
    def _profile_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile numeric column"""
        numeric_series = series.dropna()
        
        if len(numeric_series) == 0:
            return {'type': 'numeric', 'stats': 'no_valid_values'}
        
        return {
            'type': 'numeric',
            'stats': {
                'min': float(numeric_series.min()),
                'max': float(numeric_series.max()),
                'mean': float(numeric_series.mean()),
                'median': float(numeric_series.median()),
                'std': float(numeric_series.std()),
                'q25': float(numeric_series.quantile(0.25)),
                'q75': float(numeric_series.quantile(0.75)),
                'zeros_count': int((numeric_series == 0).sum()),
                'negative_count': int((numeric_series < 0).sum()),
                'positive_count': int((numeric_series > 0).sum())
            }
        }
    
    def _profile_text_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile text column"""
        text_series = series.dropna().astype(str)
        
        if len(text_series) == 0:
            return {'type': 'text', 'stats': 'no_valid_values'}
        
        lengths = text_series.str.len()
        
        return {
            'type': 'text',
            'stats': {
                'min_length': int(lengths.min()),
                'max_length': int(lengths.max()),
                'avg_length': float(lengths.mean()),
                'empty_strings': int((text_series == '').sum()),
                'whitespace_only': int(text_series.str.strip().eq('').sum()),
                'most_common': text_series.value_counts().head(5).to_dict()
            }
        }
    
    def _profile_datetime_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile datetime column"""
        datetime_series = pd.to_datetime(series, errors='coerce').dropna()
        
        if len(datetime_series) == 0:
            return {'type': 'datetime', 'stats': 'no_valid_values'}
        
        return {
            'type': 'datetime',
            'stats': {
                'min_date': datetime_series.min().isoformat(),
                'max_date': datetime_series.max().isoformat(),
                'date_range_days': (datetime_series.max() - datetime_series.min()).days,
                'most_common_year': datetime_series.dt.year.mode().iloc[0] if len(datetime_series.dt.year.mode()) > 0 else None,
                'most_common_month': datetime_series.dt.month.mode().iloc[0] if len(datetime_series.dt.month.mode()) > 0 else None,
                'most_common_weekday': datetime_series.dt.dayofweek.mode().iloc[0] if len(datetime_series.dt.dayofweek.mode()) > 0 else None
            }
        }
    
    def _profile_boolean_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile boolean column"""
        bool_series = series.dropna()
        
        if len(bool_series) == 0:
            return {'type': 'boolean', 'stats': 'no_valid_values'}
        
        return {
            'type': 'boolean',
            'stats': {
                'true_count': int(bool_series.sum()),
                'false_count': int((~bool_series).sum()),
                'true_percentage': float(bool_series.sum() / len(bool_series)) if len(bool_series) > 0 else 0.0
            }
        }


def register_implementations(registry):
    """Register data quality operation implementations"""
    
    # Register generic data quality operations
    registry.register_implementation("DATA_VALIDATION", "generic", DataValidationOperator)
    registry.register_implementation("DATA_QUALITY", "generic", DataValidationOperator)
    registry.register_implementation("DQ", "generic", DataValidationOperator)
    registry.register_implementation("DATA_PROFILING", "generic", DataProfilingOperator)
    
    # Register cloud provider implementations
    for provider in ['aws', 'gcp', 'azure', 'oci']:
        registry.register_implementation("DATA_VALIDATION", provider, DataValidationOperator)
        registry.register_implementation("DATA_QUALITY", provider, DataValidationOperator)
        registry.register_implementation("DQ", provider, DataValidationOperator)
        registry.register_implementation("DATA_PROFILING", provider, DataProfilingOperator)


# Example configuration usage:
"""
tasks:
  - name: "validate_customer_data"
    type: "DATA_VALIDATION"
    description: "Validate customer data quality"
    properties:
      data_source: "customers"
      source_type: "table"
      schema: "public"
      validation_rules:
        - name: "customer_id_not_null"
          type: "not_null"
          columns: ["customer_id"]
        - name: "email_unique"
          type: "unique"
          columns: ["email"]
        - name: "age_range"
          type: "range"
          column: "age"
          min_value: 18
          max_value: 120
        - name: "email_pattern"
          type: "pattern"
          column: "email"
          pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
        - name: "data_completeness"
          type: "completeness"
          columns: ["customer_id", "name", "email"]
          threshold: 0.95
      quality_threshold: 0.90
      fail_on_error: true

  - name: "profile_sales_data"
    type: "DATA_PROFILING"
    description: "Profile sales data for insights"
    properties:
      data_source: "SELECT * FROM sales WHERE created_date >= CURRENT_DATE - INTERVAL '30 days'"
      source_type: "sql"
      profile_columns: ["amount", "product_category", "customer_id", "created_date"]

  - name: "validate_csv_upload"
    type: "DATA_VALIDATION"
    description: "Validate uploaded CSV file"
    properties:
      data_source: "/tmp/uploaded_data_{{ ds }}.csv"
      source_type: "csv"
      validation_rules:
        - name: "required_columns"
          type: "not_null"
          columns: ["id", "name", "value"]
        - name: "positive_values"
          type: "range"
          column: "value"
          min_value: 0
      quality_threshold: 1.0
      fail_on_error: true
"""