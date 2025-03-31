"""
Main entry point for the Data Warehouse ETL Framework.
"""
import sys
import os

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.cli import main

if __name__ == "__main__":
    main()
