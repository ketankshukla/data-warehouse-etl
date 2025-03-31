"""
Command Line Interface for the Data Warehouse ETL Framework.
Provides functionality to run ETL jobs from the command line.
"""
import argparse
import logging
import os
import sys
import time
from typing import Dict, List, Optional

from src.pipeline import ETLPipeline
from src.utils.config_manager import ConfigManager
from src.utils.logging_utils import setup_logging, get_logger

logger = get_logger(__name__)

def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Data Warehouse ETL Framework")
    
    # Required arguments
    parser.add_argument(
        "-c", "--config", 
        required=True,
        help="Path to the ETL configuration file (YAML)"
    )
    
    # Optional arguments
    parser.add_argument(
        "-l", "--log-file",
        default=None,
        help="Path to the log file (default: etl.log in current directory)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--console-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Console logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the configuration file without running the ETL pipeline"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare the pipeline but don't execute (useful for testing)"
    )
    
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for storing output files (default: output)"
    )
    
    parser.add_argument(
        "--job-id",
        default=None,
        help="Specify a custom job ID for this ETL run"
    )
    
    return parser.parse_args()

def validate_config(config_path: str) -> bool:
    """
    Validate the ETL configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        True if the configuration is valid, False otherwise
    """
    try:
        logger.info(f"Validating configuration file: {config_path}")
        
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            return False
        
        # Load and validate configuration
        config_manager = ConfigManager(config_path)
        config = config_manager.get_config()
        
        # Validate required sections
        required_sections = ["extractors", "loaders"]
        missing_sections = [section for section in required_sections if section not in config]
        
        if missing_sections:
            logger.error(f"Missing required configuration sections: {missing_sections}")
            return False
        
        # Basic validation of each section
        errors = 0
        
        # Validate extractors
        extractors = config.get("extractors", {})
        if not extractors:
            logger.error("No extractors defined in configuration")
            errors += 1
        else:
            for name, extractor_config in extractors.items():
                if "type" not in extractor_config:
                    logger.error(f"Extractor '{name}' is missing required 'type' field")
                    errors += 1
        
        # Validate transformers (optional)
        transformers = config.get("transformers", {})
        for name, transformer_config in transformers.items():
            if "type" not in transformer_config:
                logger.error(f"Transformer '{name}' is missing required 'type' field")
                errors += 1
        
        # Validate loaders
        loaders = config.get("loaders", {})
        if not loaders:
            logger.error("No loaders defined in configuration")
            errors += 1
        else:
            for name, loader_config in loaders.items():
                if "type" not in loader_config:
                    logger.error(f"Loader '{name}' is missing required 'type' field")
                    errors += 1
        
        # Check for pipeline section (optional but recommended)
        if "pipeline" not in config:
            logger.warning("No 'pipeline' section defined in configuration")
        
        if errors > 0:
            logger.error(f"Configuration validation failed with {errors} errors")
            return False
        
        logger.info("Configuration validation successful")
        return True
        
    except Exception as e:
        logger.error(f"Error validating configuration: {str(e)}")
        return False

def main():
    """
    Main entry point for the ETL CLI.
    """
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    log_file = args.log_file or "etl.log"
    log_level = getattr(logging, args.log_level)
    console_level = getattr(logging, args.console_level)
    
    setup_logging(
        log_file=log_file,
        log_level=log_level,
        console_level=console_level
    )
    
    logger.info("Data Warehouse ETL Framework")
    logger.info(f"Configuration file: {args.config}")
    
    # Create output directory if it doesn't exist
    if args.output_dir and not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        logger.info(f"Created output directory: {args.output_dir}")
    
    # Validate configuration
    if not validate_config(args.config):
        logger.error("Configuration validation failed, exiting")
        sys.exit(1)
    
    # Exit if only validating
    if args.validate_only:
        logger.info("Configuration validation successful, exiting")
        return
    
    # Run the ETL pipeline
    try:
        start_time = time.time()
        
        # Create pipeline with additional options
        pipeline_options = {
            "output_dir": args.output_dir
        }
        
        # Add custom job ID if specified
        if args.job_id:
            pipeline_options["job_id"] = args.job_id
            logger.info(f"Using custom job ID: {args.job_id}")
        
        # Handle dry run mode
        if args.dry_run:
            logger.info("Running in dry-run mode (pipeline will be prepared but not executed)")
            pipeline = ETLPipeline(args.config)
            pipeline.setup()
            logger.info("Dry run completed successfully, pipeline is ready but was not executed")
            return
        
        # Normal execution mode
        logger.info("Starting ETL pipeline")
        pipeline = ETLPipeline(args.config, **pipeline_options)
        pipeline.setup()
        metrics = pipeline.run()
        
        # Log completion
        duration = time.time() - start_time
        logger.info(f"ETL pipeline completed in {duration:.2f} seconds with status: {metrics['status']}")
        
        # Exit with error code if there were errors
        if metrics["errors"] > 0:
            logger.warning(f"ETL pipeline completed with {metrics['errors']} errors")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error running ETL pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
