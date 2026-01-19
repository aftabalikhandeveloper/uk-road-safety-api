"""
Logging utilities for ETL pipeline
"""

import sys
from pathlib import Path
from datetime import datetime
from loguru import logger

from ..config import LOG_DIR


def setup_logging(
    level: str = "INFO",
    log_dir: Path = None,
    console: bool = True,
    file: bool = True,
    job_name: str = None
):
    """
    Configure logging for ETL jobs.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory for log files
        console: Enable console output
        file: Enable file logging
        job_name: Name for log file prefix
    """
    log_dir = Path(log_dir or LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Remove default handler
    logger.remove()
    
    # Console handler
    if console:
        logger.add(
            sys.stderr,
            level=level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
                   "<level>{message}</level>"
        )
    
    # File handler
    if file:
        prefix = job_name or "etl"
        log_file = log_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d')}.log"
        
        logger.add(
            log_file,
            level="DEBUG",
            rotation="1 day",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                   "{name}:{function}:{line} - {message}"
        )
    
    logger.info(f"Logging configured: level={level}, dir={log_dir}")
    return logger


def get_logger(name: str = None):
    """Get a logger instance."""
    return logger.bind(name=name) if name else logger
