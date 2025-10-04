# Audit System - Comprehensive logging and tracking for workflow lifecycle events
"""
Audit system provides comprehensive tracking and logging for:
- Job lifecycle events (start, end, success, failure)
- Task lifecycle events (start, end, success, failure)
- Task group lifecycle events
- Custom audit actions and hooks
- Configurable audit levels and destinations
"""

import logging
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union, Callable
from enum import Enum
from dataclasses import dataclass, asdict

from airflow.utils.context import Context
from airflow.models import Variable

logger = logging.getLogger(__name__)


class AuditLevel(Enum):
    """Audit logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AuditEventType(Enum):
    """Types of audit events"""
    JOB_START = "JOB_START"
    JOB_END = "JOB_END"
    JOB_SUCCESS = "JOB_SUCCESS"
    JOB_FAILURE = "JOB_FAILURE"
    
    TASK_GROUP_START = "TASK_GROUP_START"
    TASK_GROUP_END = "TASK_GROUP_END"
    TASK_GROUP_SUCCESS = "TASK_GROUP_SUCCESS"
    TASK_GROUP_FAILURE = "TASK_GROUP_FAILURE"
    
    TASK_START = "TASK_START"
    TASK_END = "TASK_END"
    TASK_SUCCESS = "TASK_SUCCESS"
    TASK_FAILURE = "TASK_FAILURE"
    TASK_RETRY = "TASK_RETRY"
    TASK_SKIP = "TASK_SKIP"
    
    CUSTOM = "CUSTOM"


@dataclass
class AuditEvent:
    """Audit event data structure"""
    event_type: AuditEventType
    timestamp: datetime
    dag_id: str
    run_id: str
    task_id: Optional[str] = None
    task_group_id: Optional[str] = None
    level: AuditLevel = AuditLevel.INFO
    message: str = ""
    details: Optional[Dict[str, Any]] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    custom_data: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert audit event to dictionary"""
        data = asdict(self)
        # Convert enum values to strings
        data['event_type'] = self.event_type.value
        data['level'] = self.level.value
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert audit event to JSON string"""
        return json.dumps(self.to_dict(), default=str)


class AuditDestination:
    """Base class for audit destinations"""
    
    def __init__(self, name: str, enabled: bool = True):
        self.name = name
        self.enabled = enabled
    
    def write_event(self, event: AuditEvent) -> bool:
        """Write audit event to destination"""
        if not self.enabled:
            return False
        
        try:
            return self._write_event_impl(event)
        except Exception as e:
            logger.error(f"Failed to write audit event to {self.name}: {e}")
            return False
    
    def _write_event_impl(self, event: AuditEvent) -> bool:
        """Implementation-specific event writing"""
        raise NotImplementedError


class LoggerDestination(AuditDestination):
    """Log audit events to Python logger"""
    
    def __init__(self, logger_name: str = "insightair.audit", enabled: bool = True):
        super().__init__(f"logger:{logger_name}", enabled)
        self.audit_logger = logging.getLogger(logger_name)
    
    def _write_event_impl(self, event: AuditEvent) -> bool:
        log_level = getattr(logging, event.level.value)
        message = f"[{event.event_type.value}] {event.message}"
        
        if event.details:
            message += f" | Details: {json.dumps(event.details)}"
        
        self.audit_logger.log(log_level, message, extra={
            'dag_id': event.dag_id,
            'run_id': event.run_id,
            'task_id': event.task_id,
            'task_group_id': event.task_group_id,
            'event_type': event.event_type.value,
            'timestamp': event.timestamp.isoformat(),
            'duration_seconds': event.duration_seconds
        })
        
        return True


class DatabaseDestination(AuditDestination):
    """Store audit events in database"""
    
    def __init__(self, conn_id: str, table_name: str = "audit_events", enabled: bool = True):
        super().__init__(f"database:{conn_id}", enabled)
        self.conn_id = conn_id
        self.table_name = table_name
    
    def _write_event_impl(self, event: AuditEvent) -> bool:
        try:
            from airflow.hooks.base import BaseHook
            hook = BaseHook.get_connection(self.conn_id)
            
            # This would need to be implemented based on specific database type
            # For now, just log that we would write to database
            logger.debug(f"Would write audit event to database {self.conn_id}.{self.table_name}: {event.to_json()}")
            return True
            
        except Exception as e:
            logger.error(f"Database audit write failed: {e}")
            return False


class XComDestination(AuditDestination):
    """Store audit events in Airflow XCom"""
    
    def __init__(self, key_prefix: str = "audit_event", enabled: bool = True):
        super().__init__(f"xcom:{key_prefix}", enabled)
        self.key_prefix = key_prefix
        self._context = None
    
    def set_context(self, context: Context):
        """Set Airflow context for XCom operations"""
        self._context = context
    
    def _write_event_impl(self, event: AuditEvent) -> bool:
        if not self._context:
            logger.warning("XCom audit destination requires context to be set")
            return False
        
        key = f"{self.key_prefix}_{event.event_type.value.lower()}_{int(event.timestamp.timestamp())}"
        
        self._context['task_instance'].xcom_push(
            key=key,
            value=event.to_dict()
        )
        
        return True


class CustomCallableDestination(AuditDestination):
    """Execute custom callable for audit events"""
    
    def __init__(self, callable_name: str, enabled: bool = True):
        super().__init__(f"callable:{callable_name}", enabled)
        self.callable_name = callable_name
    
    def _write_event_impl(self, event: AuditEvent) -> bool:
        try:
            from .callable_registry import get_callable_registry
            registry = get_callable_registry()
            audit_callable = registry.get_callable(self.callable_name)
            
            result = audit_callable(event)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Custom callable audit failed: {e}")
            return False


class AuditSystem:
    """Central audit system for managing lifecycle events"""
    
    def __init__(self):
        self.destinations: List[AuditDestination] = []
        self.enabled = True
        self.default_level = AuditLevel.INFO
        self._event_filters: List[Callable[[AuditEvent], bool]] = []
        
        # Add default logger destination
        self.add_destination(LoggerDestination())
    
    def add_destination(self, destination: AuditDestination):
        """Add audit destination"""
        self.destinations.append(destination)
        logger.info(f"Added audit destination: {destination.name}")
    
    def remove_destination(self, destination_name: str) -> bool:
        """Remove audit destination by name"""
        for i, dest in enumerate(self.destinations):
            if dest.name == destination_name:
                del self.destinations[i]
                logger.info(f"Removed audit destination: {destination_name}")
                return True
        return False
    
    def add_event_filter(self, filter_func: Callable[[AuditEvent], bool]):
        """Add event filter function"""
        self._event_filters.append(filter_func)
    
    def configure_from_settings(self, settings: Dict[str, Any]):
        """Configure audit system from settings dictionary"""
        self.enabled = settings.get('enabled', True)
        
        if 'level' in settings:
            self.default_level = AuditLevel(settings['level'])
        
        # Configure destinations
        destinations_config = settings.get('destinations', [])
        for dest_config in destinations_config:
            dest_type = dest_config.get('type')
            
            if dest_type == 'logger':
                dest = LoggerDestination(
                    logger_name=dest_config.get('logger_name', 'insightair.audit'),
                    enabled=dest_config.get('enabled', True)
                )
            elif dest_type == 'database':
                dest = DatabaseDestination(
                    conn_id=dest_config.get('conn_id'),
                    table_name=dest_config.get('table_name', 'audit_events'),
                    enabled=dest_config.get('enabled', True)
                )
            elif dest_type == 'xcom':
                dest = XComDestination(
                    key_prefix=dest_config.get('key_prefix', 'audit_event'),
                    enabled=dest_config.get('enabled', True)
                )
            elif dest_type == 'custom':
                dest = CustomCallableDestination(
                    callable_name=dest_config.get('callable'),
                    enabled=dest_config.get('enabled', True)
                )
            else:
                logger.warning(f"Unknown audit destination type: {dest_type}")
                continue
            
            self.add_destination(dest)
    
    def log_event(self,
                  event_type: AuditEventType,
                  dag_id: str,
                  run_id: str,
                  message: str = "",
                  task_id: Optional[str] = None,
                  task_group_id: Optional[str] = None,
                  level: Optional[AuditLevel] = None,
                  details: Optional[Dict[str, Any]] = None,
                  duration_seconds: Optional[float] = None,
                  error_message: Optional[str] = None,
                  custom_data: Optional[Dict[str, Any]] = None,
                  context: Optional[Context] = None) -> bool:
        """Log an audit event"""
        
        if not self.enabled:
            return True
        
        # Create audit event
        event = AuditEvent(
            event_type=event_type,
            timestamp=datetime.now(timezone.utc),
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            task_group_id=task_group_id,
            level=level or self.default_level,
            message=message,
            details=details,
            duration_seconds=duration_seconds,
            error_message=error_message,
            custom_data=custom_data
        )
        
        # Apply event filters
        for filter_func in self._event_filters:
            if not filter_func(event):
                logger.debug(f"Audit event filtered out: {event.event_type.value}")
                return True
        
        # Set context for XCom destinations
        if context:
            for dest in self.destinations:
                if isinstance(dest, XComDestination):
                    dest.set_context(context)
        
        # Write to all destinations
        success_count = 0
        for destination in self.destinations:
            if destination.write_event(event):
                success_count += 1
        
        if success_count == 0:
            logger.error(f"Failed to write audit event to any destination: {event.event_type.value}")
            return False
        
        return True
    
    def log_job_start(self, dag_id: str, run_id: str, context: Optional[Context] = None, **kwargs):
        """Log job start event"""
        return self.log_event(
            AuditEventType.JOB_START,
            dag_id=dag_id,
            run_id=run_id,
            message=f"Job started: {dag_id}",
            context=context,
            **kwargs
        )
    
    def log_job_end(self, dag_id: str, run_id: str, duration_seconds: Optional[float] = None, 
                    context: Optional[Context] = None, **kwargs):
        """Log job end event"""
        return self.log_event(
            AuditEventType.JOB_END,
            dag_id=dag_id,
            run_id=run_id,
            message=f"Job ended: {dag_id}",
            duration_seconds=duration_seconds,
            context=context,
            **kwargs
        )
    
    def log_job_success(self, dag_id: str, run_id: str, duration_seconds: Optional[float] = None,
                       context: Optional[Context] = None, **kwargs):
        """Log job success event"""
        return self.log_event(
            AuditEventType.JOB_SUCCESS,
            dag_id=dag_id,
            run_id=run_id,
            message=f"Job completed successfully: {dag_id}",
            duration_seconds=duration_seconds,
            level=AuditLevel.INFO,
            context=context,
            **kwargs
        )
    
    def log_job_failure(self, dag_id: str, run_id: str, error_message: str,
                       duration_seconds: Optional[float] = None,
                       context: Optional[Context] = None, **kwargs):
        """Log job failure event"""
        return self.log_event(
            AuditEventType.JOB_FAILURE,
            dag_id=dag_id,
            run_id=run_id,
            message=f"Job failed: {dag_id}",
            error_message=error_message,
            duration_seconds=duration_seconds,
            level=AuditLevel.ERROR,
            context=context,
            **kwargs
        )
    
    def log_task_start(self, dag_id: str, run_id: str, task_id: str,
                      context: Optional[Context] = None, **kwargs):
        """Log task start event"""
        return self.log_event(
            AuditEventType.TASK_START,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            message=f"Task started: {task_id}",
            context=context,
            **kwargs
        )
    
    def log_task_end(self, dag_id: str, run_id: str, task_id: str,
                    duration_seconds: Optional[float] = None,
                    context: Optional[Context] = None, **kwargs):
        """Log task end event"""
        return self.log_event(
            AuditEventType.TASK_END,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            message=f"Task ended: {task_id}",
            duration_seconds=duration_seconds,
            context=context,
            **kwargs
        )
    
    def log_task_success(self, dag_id: str, run_id: str, task_id: str,
                        duration_seconds: Optional[float] = None,
                        context: Optional[Context] = None, **kwargs):
        """Log task success event"""
        return self.log_event(
            AuditEventType.TASK_SUCCESS,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            message=f"Task completed successfully: {task_id}",
            duration_seconds=duration_seconds,
            context=context,
            **kwargs
        )
    
    def log_task_failure(self, dag_id: str, run_id: str, task_id: str, error_message: str,
                        duration_seconds: Optional[float] = None,
                        context: Optional[Context] = None, **kwargs):
        """Log task failure event"""
        return self.log_event(
            AuditEventType.TASK_FAILURE,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            message=f"Task failed: {task_id}",
            error_message=error_message,
            duration_seconds=duration_seconds,
            level=AuditLevel.ERROR,
            context=context,
            **kwargs
        )
    
    def log_task_group_start(self, dag_id: str, run_id: str, task_group_id: str,
                            context: Optional[Context] = None, **kwargs):
        """Log task group start event"""
        return self.log_event(
            AuditEventType.TASK_GROUP_START,
            dag_id=dag_id,
            run_id=run_id,
            task_group_id=task_group_id,
            message=f"Task group started: {task_group_id}",
            context=context,
            **kwargs
        )
    
    def log_task_group_end(self, dag_id: str, run_id: str, task_group_id: str,
                          duration_seconds: Optional[float] = None,
                          context: Optional[Context] = None, **kwargs):
        """Log task group end event"""
        return self.log_event(
            AuditEventType.TASK_GROUP_END,
            dag_id=dag_id,
            run_id=run_id,
            task_group_id=task_group_id,
            message=f"Task group ended: {task_group_id}",
            duration_seconds=duration_seconds,
            context=context,
            **kwargs
        )


# Global audit system instance
_global_audit_system: Optional[AuditSystem] = None


def get_audit_system() -> AuditSystem:
    """Get the global audit system instance"""
    global _global_audit_system
    if _global_audit_system is None:
        _global_audit_system = AuditSystem()
        
        # Try to configure from Airflow Variables
        try:
            audit_config = Variable.get("insightair_audit_config", deserialize_json=True, default_var={})
            if audit_config:
                _global_audit_system.configure_from_settings(audit_config)
        except Exception as e:
            logger.debug(f"Could not load audit config from Variables: {e}")
    
    return _global_audit_system


def configure_audit_system(settings: Dict[str, Any]):
    """Configure the global audit system"""
    audit_system = get_audit_system()
    audit_system.configure_from_settings(settings)


# Convenience functions for common audit operations
def log_job_start(dag_id: str, run_id: str, context: Optional[Context] = None, **kwargs):
    """Convenience function to log job start"""
    return get_audit_system().log_job_start(dag_id, run_id, context, **kwargs)


def log_job_end(dag_id: str, run_id: str, duration_seconds: Optional[float] = None,
                context: Optional[Context] = None, **kwargs):
    """Convenience function to log job end"""
    return get_audit_system().log_job_end(dag_id, run_id, duration_seconds, context, **kwargs)


def log_task_start(dag_id: str, run_id: str, task_id: str, context: Optional[Context] = None, **kwargs):
    """Convenience function to log task start"""
    return get_audit_system().log_task_start(dag_id, run_id, task_id, context, **kwargs)


def log_task_end(dag_id: str, run_id: str, task_id: str, duration_seconds: Optional[float] = None,
                 context: Optional[Context] = None, **kwargs):
    """Convenience function to log task end"""
    return get_audit_system().log_task_end(dag_id, run_id, task_id, duration_seconds, context, **kwargs)