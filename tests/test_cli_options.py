"""
Test suite for ETL CLI command-line options.
Tests all supported command-line arguments and their combinations.
"""
import os
import sys
import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cli import parse_args, validate_config, main
from src.pipeline import ETLPipeline


class TestCLIOptions(unittest.TestCase):
    """Test case for command line interface options."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
        self.test_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                        'config', 'sample_etl_config.yaml')
        self.test_log = os.path.join(self.temp_dir, "test_etl.log")
        self.test_output_dir = os.path.join(self.temp_dir, "test_output")
        
        # Create a test output directory
        if not os.path.exists(self.test_output_dir):
            os.makedirs(self.test_output_dir)
            
        # Verify that test config exists
        self.assertTrue(os.path.exists(self.test_config), 
                      f"Test config file not found: {self.test_config}")

    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)

    def test_parse_args_required_config(self):
        """Test that config is required."""
        with patch('sys.argv', ['main.py']):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_parse_args_with_config(self):
        """Test parse_args with config option."""
        with patch('sys.argv', ['main.py', '--config', self.test_config]):
            args = parse_args()
            self.assertEqual(args.config, self.test_config)
            self.assertEqual(args.log_level, "INFO")  # Default value
            self.assertFalse(args.validate_only)  # Default value
            self.assertFalse(args.dry_run)  # Default value
            self.assertEqual(args.output_dir, "output")  # Default value
            self.assertIsNone(args.job_id)  # Default value

    def test_parse_args_all_options(self):
        """Test parse_args with all options specified."""
        test_job_id = "test_job_123"
        with patch('sys.argv', [
            'main.py',
            '--config', self.test_config,
            '--log-level', 'DEBUG',
            '--log-file', self.test_log,
            '--console-level', 'WARNING',
            '--validate-only',
            '--dry-run',
            '--output-dir', self.test_output_dir,
            '--job-id', test_job_id
        ]):
            args = parse_args()
            self.assertEqual(args.config, self.test_config)
            self.assertEqual(args.log_level, "DEBUG")
            self.assertEqual(args.log_file, self.test_log)
            self.assertEqual(args.console_level, "WARNING")
            self.assertTrue(args.validate_only)
            self.assertTrue(args.dry_run)
            self.assertEqual(args.output_dir, self.test_output_dir)
            self.assertEqual(args.job_id, test_job_id)

    @patch('src.utils.config_manager.ConfigManager')
    def test_validate_config_success(self, mock_config_manager):
        """Test successful config validation."""
        # Mock the config manager
        mock_config_instance = MagicMock()
        mock_config_instance.get_config.return_value = {
            "extractors": {"test_extractor": {"type": "CSV"}},
            "transformers": {"test_transformer": {"type": "Cleaning"}},
            "loaders": {"test_loader": {"type": "CSV"}},
            "pipeline": {}
        }
        mock_config_manager.return_value = mock_config_instance
        
        result = validate_config(self.test_config)
        self.assertTrue(result)

    def test_validate_config_file_not_found(self):
        """Test config validation with a non-existent file."""
        # Use a non-existent file path
        non_existent_path = os.path.join(self.temp_dir, "does_not_exist.yaml")
        
        # Ensure the file doesn't exist
        self.assertFalse(os.path.exists(non_existent_path))
        
        # The validation should fail for a non-existent file
        with patch('src.cli.logging.error'):  # Suppress error logs
            result = validate_config(non_existent_path)
            self.assertFalse(result)

    @patch('src.cli.validate_config')
    def test_main_validate_only(self, mock_validate_config):
        """Test main function with validate-only option."""
        mock_validate_config.return_value = True
        
        with patch('sys.argv', ['main.py', '--config', self.test_config, '--validate-only']):
            with patch('src.cli.logger'):  # Suppress logging
                # Just check it runs without errors
                main()
                # Verify validate_config was called
                mock_validate_config.assert_called_once_with(self.test_config)

    @patch('src.cli.validate_config')
    @patch('src.cli.ETLPipeline')
    def test_main_dry_run(self, mock_etl_pipeline, mock_validate_config):
        """Test main function with dry-run option."""
        mock_validate_config.return_value = True
        
        # Create a pipeline instance that doesn't try to compare with integers
        pipeline_instance = MagicMock()
        # Avoid operations that compare MagicMock with integers
        pipeline_instance.setup.return_value = None
        pipeline_instance.run.return_value = {"status": "completed", "errors": 0}
        mock_etl_pipeline.return_value = pipeline_instance
        
        with patch('sys.argv', ['main.py', '--config', self.test_config, '--dry-run']):
            with patch('src.cli.logger'):  # Suppress logging
                # Just check it runs without errors
                main()
                # Verify pipeline was created
                mock_etl_pipeline.assert_called_once()

    @patch('src.cli.validate_config')
    @patch('src.cli.ETLPipeline')
    def test_main_full_run(self, mock_etl_pipeline, mock_validate_config):
        """Test main function with full run."""
        mock_validate_config.return_value = True
        
        # Create a pipeline instance that doesn't try to compare with integers
        pipeline_instance = MagicMock()
        # Avoid operations that compare MagicMock with integers
        pipeline_instance.setup.return_value = None
        pipeline_instance.run.return_value = {"status": "completed", "errors": 0}
        mock_etl_pipeline.return_value = pipeline_instance
        
        with patch('sys.argv', ['main.py', '--config', self.test_config]):
            with patch('src.cli.logger'):  # Suppress logging
                # Just check it runs without errors
                main()
                # Verify pipeline was created
                mock_etl_pipeline.assert_called_once()

    @patch('src.cli.validate_config')
    @patch('src.cli.ETLPipeline')
    def test_main_with_custom_job_id(self, mock_etl_pipeline, mock_validate_config):
        """Test main function with custom job ID."""
        mock_validate_config.return_value = True
        
        # Create a pipeline instance that doesn't try to compare with integers
        pipeline_instance = MagicMock()
        # Avoid operations that compare MagicMock with integers
        pipeline_instance.setup.return_value = None
        pipeline_instance.run.return_value = {"status": "completed", "errors": 0}
        mock_etl_pipeline.return_value = pipeline_instance
        
        test_job_id = "test_job_123"
        with patch('sys.argv', ['main.py', '--config', self.test_config, '--job-id', test_job_id]):
            with patch('src.cli.logger'):  # Suppress logging
                # Just check it runs without errors
                main()
                
                # Get the call arguments
                call_args, call_kwargs = mock_etl_pipeline.call_args
                # Verify job_id was passed correctly
                self.assertEqual(call_kwargs.get('job_id'), test_job_id)

    @patch('src.cli.validate_config')
    @patch('src.cli.ETLPipeline')
    def test_main_with_custom_output_dir(self, mock_etl_pipeline, mock_validate_config):
        """Test main function with custom output directory."""
        mock_validate_config.return_value = True
        
        # Create a pipeline instance that doesn't try to compare with integers
        pipeline_instance = MagicMock()
        # Avoid operations that compare MagicMock with integers
        pipeline_instance.setup.return_value = None  
        pipeline_instance.run.return_value = {"status": "completed", "errors": 0}
        mock_etl_pipeline.return_value = pipeline_instance
        
        with patch('sys.argv', ['main.py', '--config', self.test_config, '--output-dir', self.test_output_dir]):
            with patch('src.cli.logger'):  # Suppress logging
                # Just check it runs without errors
                main()
                
                # Get the call arguments
                call_args, call_kwargs = mock_etl_pipeline.call_args
                # Verify output_dir was passed correctly
                self.assertEqual(call_kwargs.get('output_dir'), self.test_output_dir)
