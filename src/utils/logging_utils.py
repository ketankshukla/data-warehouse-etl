"""
Logging Utilities module for the Data Warehouse ETL Framework.
Provides logging configuration and helper functions.
"""
import logging
import logging.config
import os
import sys
from datetime import datetime
from typing import Dict, Optional, Union

# Default logging configuration
DEFAULT_LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] [%(name)s:%(lineno)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] [%(name)s:%(lineno)s] [%(process)d:%(thread)d] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'etl.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'encoding': 'utf8'
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': True
        }
    }
}

def setup_logging(
    log_config: Optional[Dict] = None,
    log_file: Optional[str] = None,
    log_level: Union[str, int] = logging.INFO,
    console_level: Union[str, int] = logging.INFO
) -> None:
    """
    Set up logging configuration for the ETL framework.
    
    Args:
        log_config: Dictionary with logging configuration. If None, uses DEFAULT_LOG_CONFIG.
        log_file: Path to the log file. If None, uses 'etl.log' in the current directory.
        log_level: Logging level for file handler (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        console_level: Logging level for console handler.
    """
    config = log_config or DEFAULT_LOG_CONFIG.copy()
    
    # Set the log file path if specified
    if log_file:
        # Create directory for log file if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Set the log file path in the configuration
        config['handlers']['file']['filename'] = log_file
    
    # Set logging levels if specified
    if 'handlers' in config:
        if 'file' in config['handlers']:
            config['handlers']['file']['level'] = log_level
        if 'console' in config['handlers']:
            config['handlers']['console']['level'] = console_level
    
    # Apply the configuration
    logging.config.dictConfig(config)
    
    # Log the start of the ETL process
    root_logger = logging.getLogger()
    root_logger.info(f"ETL logging initialized at level: {logging.getLevelName(root_logger.level)}")
    root_logger.debug("Detailed logging enabled")

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Name for the logger, typically the module name.
        
    Returns:
        Logger instance.
    """
    return logging.getLogger(name)

class ETLLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter to add context information to log messages.
    """
    
    def __init__(self, logger, job_id=None, component=None, extra=None):
        """
        Initialize the logger adapter.
        
        Args:
            logger: The logger to adapt.
            job_id: Optional ID for the ETL job.
            component: Optional name of the ETL component.
            extra: Additional context information to include.
        """
        extra = extra or {}
        if job_id:
            extra['job_id'] = job_id
        if component:
            extra['component'] = component
        super().__init__(logger, extra)
    
    def process(self, msg, kwargs):
        """
        Process the log message to add context information.
        
        Args:
            msg: The log message.
            kwargs: Additional keyword arguments.
            
        Returns:
            Tuple of (modified message, modified kwargs).
        """
        context = []
        if 'job_id' in self.extra:
            context.append(f"job_id={self.extra['job_id']}")
        if 'component' in self.extra:
            context.append(f"component={self.extra['component']}")
        
        if context:
            context_str = ' '.join(context)
            msg = f"[{context_str}] {msg}"
        
        return msg, kwargs

def get_etl_logger(name: str, job_id: Optional[str] = None, component: Optional[str] = None) -> ETLLoggerAdapter:
    """
    Get a logger adapter with ETL context information.
    
    Args:
        name: Name for the logger, typically the module name.
        job_id: Optional ID for the ETL job.
        component: Optional name of the ETL component.
        
    Returns:
        ETLLoggerAdapter instance.
    """
    logger = logging.getLogger(name)
    return ETLLoggerAdapter(logger, job_id=job_id, component=component)

def generate_job_id() -> str:
    """
    Generate a unique job ID for ETL tracking.
    
    Returns:
        Unique job ID string.
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    # Add a random suffix to ensure uniqueness
    import random
    random_suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=6))
    return f"job_{timestamp}_{random_suffix}"
