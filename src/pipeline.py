"""
ETL Pipeline module for the Data Warehouse ETL Framework.
Provides functionality to orchestrate the ETL process.
"""
import importlib
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from src.extractors.base_extractor import BaseExtractor
from src.transformers.base_transformer import BaseTransformer
from src.loaders.base_loader import BaseLoader
from src.utils.config_manager import ConfigManager
from src.utils.logging_utils import get_etl_logger, generate_job_id

logger = get_etl_logger(__name__, component="Pipeline")

class ETLPipeline:
    """
    Orchestrator for the Extract-Transform-Load pipeline.
    """
    
    def __init__(self, config_path: str = None, config: Dict[str, Any] = None, output_dir: str = "output", job_id: str = None):
        """
        Initialize the ETL pipeline with configuration.
        
        Args:
            config_path: Path to the YAML configuration file
            config: Dictionary containing configuration (takes precedence over config_path)
            output_dir: Directory for storing output files
            job_id: Custom job ID for this ETL run
        """
        # Use provided job_id or generate a new one
        self.job_id = job_id if job_id else generate_job_id()
        self.logger = get_etl_logger(__name__, job_id=self.job_id, component="Pipeline")
        self.logger.info("Initializing ETL pipeline")
        
        # Set output directory
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f"Created output directory: {self.output_dir}")
        
        # Load configuration
        if config:
            self.config = config
        elif config_path:
            config_manager = ConfigManager(config_path)
            self.config = config_manager.get_config()
        else:
            self.logger.error("No configuration provided for ETL pipeline")
            raise ValueError("No configuration provided for ETL pipeline")
        
        # Initialize components
        self.extractors = []
        self.transformers = []
        self.loaders = []
        
        # Track metrics
        self.metrics = {
            "job_id": self.job_id,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "extraction_rows": 0,
            "transformation_rows": 0,
            "loading_rows": 0,
            "errors": 0,
            "status": "initialized"
        }
    
    def setup(self) -> 'ETLPipeline':
        """
        Set up the ETL pipeline components based on configuration.
        
        Returns:
            Self for method chaining
        """
        self.logger.info("Setting up ETL pipeline components")
        
        try:
            # Set up extractors
            self._setup_extractors()
            
            # Set up transformers
            self._setup_transformers()
            
            # Set up loaders
            self._setup_loaders()
            
            self.logger.info(f"ETL pipeline setup complete with {len(self.extractors)} extractors, "
                           f"{len(self.transformers)} transformers, and {len(self.loaders)} loaders")
            
            return self
            
        except Exception as e:
            self.logger.error(f"Error setting up ETL pipeline: {str(e)}")
            self.metrics["errors"] += 1
            self.metrics["status"] = "setup_failed"
            raise
    
    def _setup_extractors(self) -> None:
        """
        Set up data extractors based on configuration.
        """
        extractor_configs = self.config.get("extractors", {})
        if not extractor_configs:
            self.logger.warning("No extractors configured")
            return
        
        for name, config in extractor_configs.items():
            try:
                extractor_type = config.get("type")
                if not extractor_type:
                    self.logger.error(f"Extractor '{name}' is missing 'type' configuration")
                    continue
                
                # Import the extractor class
                if "." in extractor_type:
                    # Fully qualified path provided
                    module_path, class_name = extractor_type.rsplit(".", 1)
                    module = importlib.import_module(module_path)
                    extractor_class = getattr(module, class_name)
                else:
                    # Standard extractor from our package
                    module_name = extractor_type.lower()
                    module = importlib.import_module(f"src.extractors.{module_name}_extractor")
                    extractor_class = getattr(module, f"{extractor_type}Extractor")
                
                # Create extractor instance
                extractor = extractor_class(config)
                self.extractors.append(extractor)
                self.logger.info(f"Added extractor: {extractor.__class__.__name__}")
                
            except (ImportError, AttributeError) as e:
                self.logger.error(f"Failed to load extractor '{name}': {str(e)}")
                self.metrics["errors"] += 1
            except Exception as e:
                self.logger.error(f"Error initializing extractor '{name}': {str(e)}")
                self.metrics["errors"] += 1
    
    def _setup_transformers(self) -> None:
        """
        Set up data transformers based on configuration.
        """
        transformer_configs = self.config.get("transformers", {})
        if not transformer_configs:
            self.logger.warning("No transformers configured")
            return
        
        for name, config in transformer_configs.items():
            try:
                transformer_type = config.get("type")
                if not transformer_type:
                    self.logger.error(f"Transformer '{name}' is missing 'type' configuration")
                    continue
                
                # Import the transformer class
                if "." in transformer_type:
                    # Fully qualified path provided
                    module_path, class_name = transformer_type.rsplit(".", 1)
                    module = importlib.import_module(module_path)
                    transformer_class = getattr(module, class_name)
                else:
                    # Standard transformer from our package
                    module = importlib.import_module(f"src.transformers.{transformer_type.lower()}_transformer")
                    class_name = "".join(word.capitalize() for word in transformer_type.split("_"))
                    if not class_name.endswith("Transformer"):
                        class_name += "Transformer"
                    transformer_class = getattr(module, class_name)
                
                # Create transformer instance
                transformer = transformer_class(config)
                self.transformers.append(transformer)
                self.logger.info(f"Added transformer: {transformer.__class__.__name__}")
                
            except (ImportError, AttributeError) as e:
                self.logger.error(f"Failed to load transformer '{name}': {str(e)}")
                self.metrics["errors"] += 1
            except Exception as e:
                self.logger.error(f"Error initializing transformer '{name}': {str(e)}")
                self.metrics["errors"] += 1
    
    def _setup_loaders(self) -> None:
        """
        Set up data loaders based on configuration.
        """
        if "loaders" not in self.config:
            self.logger.warning("No loaders defined in configuration")
            return
        
        loaders_config = self.config["loaders"]
        self.logger.info(f"Setting up {len(loaders_config)} loaders")
        
        for name, loader_config in loaders_config.items():
            try:
                # Set output directory for the loader
                if not loader_config.get("output_path") and "file" in loader_config.get("connection", {}).get("type", ""):
                    # For file-based loaders, set the output path to be in the output directory
                    if not "connection" in loader_config:
                        loader_config["connection"] = {}
                    if not "output_path" in loader_config["connection"]:
                        loader_config["connection"]["output_path"] = os.path.join(self.output_dir, f"{name}_output")
                        self.logger.info(f"Set output path for loader '{name}' to {loader_config['connection']['output_path']}")
                
                # Get loader type
                if "type" not in loader_config:
                    self.logger.error(f"Loader '{name}' is missing required 'type' field")
                    self.metrics["errors"] += 1
                    continue
                
                loader_type = loader_config["type"]
                custom_loader = loader_config.get("custom_loader")
                
                # Create loader instance
                if custom_loader:
                    # Custom loader from user-provided class
                    module_name, class_name = custom_loader.rsplit(".", 1)
                    module = importlib.import_module(module_name)
                    loader_class = getattr(module, class_name)
                else:
                    # Standard loader from our package
                    module_name = loader_type.lower()
                    module = importlib.import_module(f"src.loaders.{module_name}_loader")
                    loader_class = getattr(module, f"{loader_type}Loader")
                
                # Create loader instance
                loader = loader_class(loader_config)
                self.loaders.append(loader)
                self.logger.info(f"Added loader: {loader.__class__.__name__}")
                
            except (ImportError, AttributeError) as e:
                self.logger.error(f"Failed to load loader '{name}': {str(e)}")
                self.metrics["errors"] += 1
            except Exception as e:
                self.logger.error(f"Error initializing loader '{name}': {str(e)}")
                self.metrics["errors"] += 1
    
    def run(self) -> Dict[str, Any]:
        """
        Run the ETL pipeline.
        
        Returns:
            Dictionary with ETL pipeline metrics
        """
        self.logger.info(f"Starting ETL pipeline job {self.job_id}")
        self.metrics["start_time"] = time.time()
        self.metrics["status"] = "running"
        
        try:
            # Extract data
            extracted_data = self._extract()
            if not extracted_data:
                self.logger.warning("No data extracted, pipeline completed with empty result")
                self.metrics["status"] = "completed_empty"
                self.metrics["end_time"] = time.time()
                self.metrics["duration_seconds"] = self.metrics["end_time"] - self.metrics["start_time"]
                return self.metrics
            
            # Transform data
            transformed_data = self._transform(extracted_data)
            if not transformed_data:
                self.logger.warning("No data after transformation, pipeline completed with empty result")
                self.metrics["status"] = "completed_empty"
                self.metrics["end_time"] = time.time()
                self.metrics["duration_seconds"] = self.metrics["end_time"] - self.metrics["start_time"]
                return self.metrics
            
            # Load data
            load_success = self._load(transformed_data)
            
            # Update metrics
            self.metrics["end_time"] = time.time()
            self.metrics["duration_seconds"] = self.metrics["end_time"] - self.metrics["start_time"]
            
            if load_success:
                self.metrics["status"] = "completed_success"
                self.logger.info(f"ETL pipeline completed successfully in {self.metrics['duration_seconds']:.2f} seconds")
            else:
                self.metrics["status"] = "completed_with_errors"
                self.logger.warning(f"ETL pipeline completed with errors in {self.metrics['duration_seconds']:.2f} seconds")
            
            return self.metrics
            
        except Exception as e:
            self.logger.error(f"Error running ETL pipeline: {str(e)}")
            self.metrics["errors"] += 1
            self.metrics["status"] = "failed"
            self.metrics["end_time"] = time.time()
            self.metrics["duration_seconds"] = self.metrics["end_time"] - self.metrics["start_time"]
            raise
        finally:
            self.logger.info(f"ETL pipeline job {self.job_id} finished with status: {self.metrics['status']}")
            self._log_metrics()
    
    def _extract(self) -> List[pd.DataFrame]:
        """
        Run the extraction phase of the ETL pipeline.
        
        Returns:
            List of extracted DataFrames
        """
        if not self.extractors:
            self.logger.warning("No extractors configured, skipping extraction phase")
            return []
        
        self.logger.info(f"Starting extraction phase with {len(self.extractors)} extractors")
        extracted_data = []
        
        for extractor in self.extractors:
            try:
                self.logger.info(f"Running extractor: {extractor.__class__.__name__}")
                data = extractor.extract()
                
                if isinstance(data, pd.DataFrame):
                    if data.empty:
                        self.logger.warning(f"Extractor {extractor.__class__.__name__} returned empty DataFrame")
                    else:
                        rows = len(data)
                        self.metrics["extraction_rows"] += rows
                        self.logger.info(f"Extractor {extractor.__class__.__name__} extracted {rows} rows")
                    extracted_data.append(data)
                else:
                    self.logger.error(f"Extractor {extractor.__class__.__name__} returned invalid data type: {type(data)}")
                    self.metrics["errors"] += 1
            
            except Exception as e:
                self.logger.error(f"Error in extractor {extractor.__class__.__name__}: {str(e)}")
                self.metrics["errors"] += 1
        
        total_extracted = sum(len(df) for df in extracted_data if isinstance(df, pd.DataFrame))
        self.logger.info(f"Extraction phase completed with {total_extracted} total rows extracted")
        
        return extracted_data
    
    def _transform(self, data: List[pd.DataFrame]) -> List[pd.DataFrame]:
        """
        Run the transformation phase of the ETL pipeline.
        
        Args:
            data: List of DataFrames to transform
            
        Returns:
            List of transformed DataFrames
        """
        if not data:
            self.logger.warning("No data to transform, skipping transformation phase")
            return []
        
        if not self.transformers:
            self.logger.warning("No transformers configured, passing through data without transformation")
            return data
        
        self.logger.info(f"Starting transformation phase with {len(self.transformers)} transformers")
        transformed_data = data
        
        for transformer in self.transformers:
            try:
                self.logger.info(f"Running transformer: {transformer.__class__.__name__}")
                transformed_data = transformer.transform(transformed_data)
                
                if not transformed_data:
                    self.logger.warning(f"Transformer {transformer.__class__.__name__} returned empty result")
                    break
                
                if isinstance(transformed_data, list):
                    # If it's a list of DataFrames, count total rows
                    total_rows = sum(len(df) for df in transformed_data if isinstance(df, pd.DataFrame))
                    self.logger.info(f"Transformer {transformer.__class__.__name__} produced {total_rows} rows in {len(transformed_data)} DataFrames")
                elif isinstance(transformed_data, pd.DataFrame):
                    # If it's a single DataFrame, count rows and convert to list
                    rows = len(transformed_data)
                    self.logger.info(f"Transformer {transformer.__class__.__name__} produced {rows} rows")
                    transformed_data = [transformed_data]
                else:
                    self.logger.error(f"Transformer {transformer.__class__.__name__} returned invalid data type: {type(transformed_data)}")
                    self.metrics["errors"] += 1
                    transformed_data = []
                    break
            
            except Exception as e:
                self.logger.error(f"Error in transformer {transformer.__class__.__name__}: {str(e)}")
                self.metrics["errors"] += 1
                transformed_data = []
                break
        
        # Update transformation metrics
        total_transformed = sum(len(df) for df in transformed_data if isinstance(df, pd.DataFrame))
        self.metrics["transformation_rows"] = total_transformed
        self.logger.info(f"Transformation phase completed with {total_transformed} total rows")
        
        return transformed_data
    
    def _load(self, data: List[pd.DataFrame]) -> bool:
        """
        Run the loading phase of the ETL pipeline.
        
        Args:
            data: List of DataFrames to load
            
        Returns:
            True if loading was successful, False otherwise
        """
        if not data:
            self.logger.warning("No data to load, skipping loading phase")
            return True
        
        if not self.loaders:
            self.logger.warning("No loaders configured, data will not be loaded to any destination")
            return False
        
        self.logger.info(f"Starting loading phase with {len(self.loaders)} loaders")
        success = True
        
        for loader in self.loaders:
            try:
                self.logger.info(f"Running loader: {loader.__class__.__name__}")
                
                # First validate the destination
                if not loader.validate_destination():
                    self.logger.error(f"Loader {loader.__class__.__name__} failed destination validation")
                    self.metrics["errors"] += 1
                    success = False
                    continue
                
                # Then load the data
                loader_success = loader.load(data)
                
                if loader_success:
                    # Update loading metrics
                    total_loaded = sum(len(df) for df in data if isinstance(df, pd.DataFrame))
                    self.metrics["loading_rows"] = total_loaded
                    self.logger.info(f"Loader {loader.__class__.__name__} successfully loaded {total_loaded} rows")
                else:
                    self.logger.error(f"Loader {loader.__class__.__name__} failed to load data")
                    self.metrics["errors"] += 1
                    success = False
            
            except Exception as e:
                self.logger.error(f"Error in loader {loader.__class__.__name__}: {str(e)}")
                self.metrics["errors"] += 1
                success = False
        
        self.logger.info(f"Loading phase completed {'successfully' if success else 'with errors'}")
        return success
    
    def _log_metrics(self) -> None:
        """
        Log the ETL pipeline metrics.
        """
        self.logger.info(f"ETL Job ID: {self.metrics['job_id']}")
        self.logger.info(f"Status: {self.metrics['status']}")
        
        if self.metrics['start_time'] and self.metrics['end_time']:
            self.logger.info(f"Duration: {self.metrics['duration_seconds']:.2f} seconds")
        
        self.logger.info(f"Rows extracted: {self.metrics['extraction_rows']}")
        self.logger.info(f"Rows after transformation: {self.metrics['transformation_rows']}")
        self.logger.info(f"Rows loaded: {self.metrics['loading_rows']}")
        self.logger.info(f"Errors: {self.metrics['errors']}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get the ETL pipeline metrics.
        
        Returns:
            Dictionary with ETL pipeline metrics
        """
        return self.metrics
        
# Helper function to run a pipeline from a config file
def run_pipeline_from_config(config_path: str) -> Dict[str, Any]:
    """
    Helper function to run an ETL pipeline from a configuration file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary with ETL pipeline metrics
    """
    pipeline = ETLPipeline(config_path)
    pipeline.setup()
    return pipeline.run()
