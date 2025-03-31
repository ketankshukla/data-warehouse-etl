"""
Test runner for the ETL Framework.
Runs all tests in the 'tests' directory and generates a summary report.
"""
import os
import sys
import unittest
import time
import argparse
import importlib
import pkgutil
import inspect
import re

def discover_component_tests(component):
    """
    Discover tests for a specific component by looking at class/method names and docstrings.
    
    Args:
        component (str): Component to look for ('api', 'csv', 'db', 'json', etc.)
    
    Returns:
        unittest.TestSuite: Suite containing tests for the specified component
    """
    # Component mapping - maps component name to patterns to search for in test names and docstrings
    component_patterns = {
        'api': [r'api', r'rest', r'http', r'request'],
        'csv': [r'csv', r'file', r'delimited'],
        'db': [r'db', r'sql', r'database', r'sqlite'],
        'json': [r'json'],
        'transformer': [r'transform', r'flattening', r'json_transformer'],
        'loader': [r'load', r'output'],
        'extractor': [r'extract', r'input', r'source'],
        'pipeline': [r'pipeline', r'etl', r'workflow'],
        'cli': [r'cli', r'command', r'argument']
    }
    
    # If component doesn't have specific patterns, use the component name
    if component not in component_patterns:
        component_patterns[component] = [re.escape(component)]
    
    # Create an empty test suite
    suite = unittest.TestSuite()
    
    # Load all test modules
    for finder, name, is_pkg in pkgutil.iter_modules(['tests']):
        if name.startswith('test_'):
            module = importlib.import_module(f'tests.{name}')
            
            # Find all test classes in the module
            for cls_name, cls_obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls_obj, unittest.TestCase) and cls_obj != unittest.TestCase:
                    # Check if class name or docstring matches component
                    class_matches = any(re.search(pattern, cls_name.lower()) for pattern in component_patterns[component])
                    docstring_matches = cls_obj.__doc__ and any(re.search(pattern, cls_obj.__doc__.lower()) for pattern in component_patterns[component])
                    
                    # If the class matches or has a matching docstring, add all its test methods
                    if class_matches or docstring_matches:
                        tests = unittest.defaultTestLoader.loadTestsFromTestCase(cls_obj)
                        suite.addTests(tests)
                    else:
                        # Add individual test methods that match the component
                        for method_name in dir(cls_obj):
                            if method_name.startswith('test_'):
                                method_obj = getattr(cls_obj, method_name)
                                if callable(method_obj):
                                    method_matches = any(re.search(pattern, method_name.lower()) for pattern in component_patterns[component])
                                    method_doc = getattr(method_obj, '__doc__', None)
                                    method_doc_matches = method_doc and any(re.search(pattern, method_doc.lower()) for pattern in component_patterns[component])
                                    
                                    if method_matches or method_doc_matches:
                                        test = cls_obj(method_name)
                                        suite.addTest(test)
    
    return suite

def run_tests(component=None):
    """
    Run test cases in the tests directory.
    
    Args:
        component (str, optional): Component to test ('api', 'csv', 'db', 'json', etc.).
                                   If None, all tests are run.
    """
    start_time = time.time()
    
    if component:
        print(f"Running tests for component: {component}")
        
        if component == "all":
            # Run all tests
            test_suite = unittest.defaultTestLoader.discover('tests', pattern="test_*.py")
        else:
            # First try with traditional file pattern approach
            file_pattern = f"test_{component}*.py"
            test_suite = unittest.defaultTestLoader.discover('tests', pattern=file_pattern)
            
            # If no tests found with file pattern, try component-based discovery
            if not test_suite.countTestCases():
                test_suite = discover_component_tests(component)
    else:
        # Default: run all tests
        print("Running all tests")
        test_suite = unittest.defaultTestLoader.discover('tests', pattern="test_*.py")
    
    # Run the tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    # Calculate execution time
    execution_time = time.time() - start_time
    
    # Print summary
    print("\n" + "="*70)
    print(f"TEST SUMMARY")
    print("="*70)
    print(f"Tests executed: {result.testsRun}")
    print(f"Tests passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Tests failed: {len(result.failures)}")
    print(f"Test errors: {len(result.errors)}")
    print(f"Execution time: {execution_time:.2f} seconds")
    print("="*70)
    
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser(description='Run ETL Framework tests.')
    parser.add_argument('component', nargs='?', default=None,
                        help='Component to test: "api", "csv", "db", "json", "transformer", "loader", "extractor", "pipeline", "cli", or "all". If not provided, all tests are run.')
    
    args = parser.parse_args()
    
    # Run tests for the specified component, or all tests if no component is specified
    sys.exit(run_tests(args.component))
