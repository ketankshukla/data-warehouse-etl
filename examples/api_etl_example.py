"""
API ETL Example Script for the Data Warehouse ETL Framework.

This script demonstrates a complete API ETL workflow, including:
1. Data extraction from a REST API
2. Transformation of nested JSON responses
3. Loading data to various destinations

Run this script directly to see the API ETL process in action.
"""
import json
import logging
import os
import sys
import pandas as pd
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = str(Path(__file__).parent.parent.absolute())
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from src.pipeline import ETLPipeline, run_pipeline_from_config
from src.extractors.api_extractor import APIExtractor
from src.transformers.flattening_transformer import FlatteningTransformer
from src.transformers.json_transformer import JSONTransformer
from src.loaders.csv_loader import CSVLoader
from src.loaders.sql_loader import SQLLoader
from src.utils.logging_utils import setup_logging

# Set up logging
setup_logging(log_level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_config():
    """
    Create a sample API ETL configuration.
    
    This configuration extracts data from the JSONPlaceholder API,
    transforms it, and loads it to CSV and SQLite.
    
    Returns:
        Dict: Configuration dictionary
    """
    config = {
        "name": "API ETL Example",
        "description": "Example ETL pipeline for extracting, transforming, and loading API data",
        "job_id": "api_etl_example",
        
        "extractors": {
            "json_placeholder_api": {
                "type": "API",
                "url": "https://jsonplaceholder.typicode.com",
                "endpoint": "/users",
                "method": "GET",
                "headers": {
                    "Accept": "application/json"
                },
                "pagination": {
                    "type": "none"  # JSONPlaceholder doesn't require pagination for this endpoint
                }
            }
        },
        
        "transformers": {
            "flatten_address": {
                "type": "Flattening",
                "flatten_fields": [
                    {
                        "path": "address",
                        "prefix": "address",
                        "drop_original": True
                    },
                    {
                        "path": "company",
                        "prefix": "company",
                        "drop_original": True
                    }
                ]
            },
            "clean_and_format": {
                "type": "JSON",
                "rename_fields": {
                    "id": "user_id",
                    "name": "full_name",
                    "username": "user_name",
                    "email": "email_address"
                },
                "cast_types": {
                    "user_id": "int",
                    "address_zipcode": "str",
                    "company_bs": "str"
                },
                "expressions": {
                    "user_location": "address_city + ', ' + address_zipcode",
                    "user_contact": "email_address + ' (Phone: ' + phone + ')'",
                    "is_website_secure": "website.str.startswith('https')"
                }
            }
        },
        
        "loaders": {
            "users_csv": {
                "type": "CSV",
                "connection": {
                    "type": "file",
                    "path": "output/api_users.csv"
                },
                "options": {
                    "index": False,
                    "encoding": "utf-8"
                }
            },
            "users_db": {
                "type": "SQL",
                "connection": {
                    "type": "sqlite",
                    "database": "output/api_etl.db"
                },
                "options": {
                    "table_name": "users",
                    "if_exists": "replace",
                    "index": False
                }
            }
        }
    }
    
    return config

def run_api_etl_programmatically():
    """
    Run the API ETL process programmatically by creating and configuring components.
    
    This demonstrates how to instantiate and use the components directly in code.
    """
    logger.info("Starting API ETL process programmatically")
    
    try:
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
        
        # 1. Set up and run the extractor
        extractor_config = {
            "url": "https://jsonplaceholder.typicode.com",
            "endpoint": "/users",
            "method": "GET",
            "headers": {"Accept": "application/json"}
        }
        extractor = APIExtractor(extractor_config)
        
        # Extract data
        logger.info("Extracting data from API")
        data = extractor.extract()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        logger.info(f"Extracted {len(df)} records from API")
        
        # 2. Set up and run transformers
        # First, flatten nested structures
        flattening_config = {
            "flatten_fields": [
                {
                    "path": "address",
                    "prefix": "address",
                    "drop_original": True
                },
                {
                    "path": "company",
                    "prefix": "company",
                    "drop_original": True
                }
            ]
        }
        flattening_transformer = FlatteningTransformer(flattening_config)
        
        # Apply flattening transformation
        logger.info("Applying flattening transformation")
        df = flattening_transformer.transform(df)
        
        # Next, apply JSON transformations
        json_transformer_config = {
            "rename_fields": {
                "id": "user_id",
                "name": "full_name",
                "username": "user_name",
                "email": "email_address"
            },
            "cast_types": {
                "user_id": "int"
            },
            "expressions": {
                "user_location": "address_city + ', ' + address_zipcode",
                "user_contact": "email_address + ' (Phone: ' + phone + ')'",
                "is_website_secure": "website.str.startswith('https')"
            }
        }
        json_transformer = JSONTransformer(json_transformer_config)
        
        # Apply JSON transformation
        logger.info("Applying JSON transformations")
        df = json_transformer.transform(df)
        
        # 3. Set up and run loaders
        # CSV Loader
        csv_loader_config = {
            "connection": {
                "type": "file",
                "path": "output/api_users_programmatic.csv"
            },
            "options": {
                "index": False,
                "encoding": "utf-8"
            }
        }
        csv_loader = CSVLoader(csv_loader_config)
        
        # Load to CSV
        logger.info("Loading data to CSV")
        csv_loader.load(df)
        
        # SQL Loader
        sql_loader_config = {
            "connection": {
                "type": "sqlite",
                "database": "output/api_etl_programmatic.db"
            },
            "options": {
                "table_name": "users",
                "if_exists": "replace",
                "index": False
            }
        }
        sql_loader = SQLLoader(sql_loader_config)
        
        # Load to SQLite
        logger.info("Loading data to SQLite")
        sql_loader.load(df)
        
        logger.info("API ETL process completed successfully")
        logger.info(f"CSV output: {os.path.abspath('output/api_users_programmatic.csv')}")
        logger.info(f"SQLite output: {os.path.abspath('output/api_etl_programmatic.db')}")
        
    except Exception as e:
        logger.error(f"Error during API ETL process: {str(e)}")
        raise

def run_api_etl_with_pipeline():
    """
    Run the API ETL process using the ETLPipeline class.
    
    This demonstrates how to use the pipeline for a more declarative approach.
    """
    logger.info("Starting API ETL process with pipeline")
    
    try:
        # Create the configuration
        config = create_sample_config()
        
        # Create output directory
        os.makedirs("output", exist_ok=True)
        
        # Create and run the pipeline
        pipeline = ETLPipeline(config=config, output_dir="output", job_id="api_etl_example")
        
        # Set up and run
        pipeline.setup().run()
        
        # Get metrics
        metrics = pipeline.get_metrics()
        logger.info(f"Pipeline metrics: {json.dumps(metrics, indent=2)}")
        
        logger.info("API ETL pipeline completed successfully")
        logger.info(f"CSV output: {os.path.abspath('output/api_users.csv')}")
        logger.info(f"SQLite output: {os.path.abspath('output/api_etl.db')}")
        
    except Exception as e:
        logger.error(f"Error during API ETL pipeline: {str(e)}")
        raise

def run_api_etl_from_config_file():
    """
    Run the API ETL process from a configuration file.
    
    This demonstrates how to run a pipeline from a configuration file.
    """
    # Create the configuration
    config = create_sample_config()
    
    # Write configuration to file
    config_path = "output/api_etl_config.json"
    os.makedirs("output", exist_ok=True)
    
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    
    logger.info(f"Created configuration file: {os.path.abspath(config_path)}")
    
    # Run the pipeline from the config file
    try:
        logger.info("Starting API ETL process from config file")
        metrics = run_pipeline_from_config(config_path)
        logger.info(f"Pipeline metrics: {json.dumps(metrics, indent=2)}")
        logger.info("API ETL from config file completed successfully")
    except Exception as e:
        logger.error(f"Error running API ETL from config file: {str(e)}")
        raise

if __name__ == "__main__":
    print("=" * 80)
    print("API ETL EXAMPLE")
    print("=" * 80)
    print("\nThis script demonstrates three ways to run an API ETL process:")
    print("1. Programmatically instantiating components")
    print("2. Using the ETLPipeline class")
    print("3. From a configuration file")
    print("\nAll outputs will be saved to the 'output' directory.")
    
    # Ask user which method to run
    choice = input("\nEnter the number of the method to run (1, 2, 3), or 'all': ")
    
    if choice == "1" or choice.lower() == "all":
        print("\n" + "=" * 40)
        print("RUNNING PROGRAMMATIC API ETL")
        print("=" * 40)
        run_api_etl_programmatically()
    
    if choice == "2" or choice.lower() == "all":
        print("\n" + "=" * 40)
        print("RUNNING API ETL WITH PIPELINE")
        print("=" * 40)
        run_api_etl_with_pipeline()
    
    if choice == "3" or choice.lower() == "all":
        print("\n" + "=" * 40)
        print("RUNNING API ETL FROM CONFIG FILE")
        print("=" * 40)
        run_api_etl_from_config_file()
    
    print("\n" + "=" * 80)
    print("API ETL EXAMPLE COMPLETED")
    print("=" * 80)
