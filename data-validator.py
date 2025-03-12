#!/usr/bin/env python3
"""
Data Validation Tool for CSV and Excel files

This script validates CSV and Excel files before database ingestion by checking for:
- File format and accessibility
- Structural issues (inconsistent row lengths, column counts)
- Missing values in required fields
- Duplicate primary keys
- Data type consistency
- Value range validation
- Date format validation
- Special character detection
- Custom validation rules

Usage:
    python data_validator.py input_file [--pk PRIMARY_KEY] [--required COLUMNS] [--config CONFIG_FILE]

Arguments:
    input_file          Path to the CSV or Excel file to validate
    --pk                Column name(s) to use as primary key(s) for duplicate detection
    --required          Comma-separated list of columns that must not contain empty values
    --config            Path to JSON configuration file with validation rules

Examples:
    python data_validator.py data.csv --pk id
    python data_validator.py data.xlsx --pk "id,email" --required "name,email,phone"
    python data_validator.py data.csv --config validation_rules.json
"""

import os
import sys
import csv
import json
import argparse
import re
import datetime
from collections import defaultdict, Counter
from typing import List, Dict, Any, Set, Tuple, Optional, Union

try:
    import pandas as pd
    import numpy as np
    from colorama import Fore, Style, init
    from tabulate import tabulate
except ImportError:
    print("Required packages not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "numpy", "colorama", "tabulate", "openpyxl"])
    import pandas as pd
    import numpy as np
    from colorama import Fore, Style, init
    from tabulate import tabulate

# Initialize colorama
init()

class DataValidator:
    """
    A class to validate CSV and Excel files for database ingestion.
    """

    def __init__(self, file_path: str, pk_columns: List[str] = None, required_columns: List[str] = None, 
                 config_path: str = None):
        """
        Initialize the DataValidator.

        Args:
            file_path (str): Path to the file to validate
            pk_columns (List[str], optional): Columns to use as primary keys
            required_columns (List[str], optional): Columns that must not be empty
            config_path (str, optional): Path to configuration file with validation rules
        """
        self.file_path = file_path
        self.pk_columns = pk_columns or []
        self.required_columns = required_columns or []
        self.config_path = config_path
        self.config = self._load_config() if config_path else {}
        
        self.file_extension = os.path.splitext(file_path)[1].lower()
        self.data = None
        self.raw_data = None
        self.column_names = []
        self.issue_count = 0
        self.warnings = []
        self.errors = []
        
    def _load_config(self) -> Dict:
        """Load validation rules from a configuration file."""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"{Fore.RED}Error loading configuration file: {e}{Style.RESET_ALL}")
            sys.exit(1)
    
    def load_file(self) -> bool:
        """
        Load the file into memory for validation.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.file_extension == '.csv':
                # First attempt to read raw data to check for CSV format issues
                self._load_raw_csv()
                
                # Then load with pandas for structured analysis
                self.data = pd.read_csv(self.file_path, low_memory=False, skip_blank_lines=False)
            elif self.file_extension in ['.xls', '.xlsx', '.xlsm']:
                self.data = pd.read_excel(self.file_path)
            else:
                self.errors.append(f"Unsupported file extension: {self.file_extension}")
                return False
                
            # Get column names
            self.column_names = list(self.data.columns)
            
            # Check if primary key columns exist in the dataset
            missing_pk_cols = [col for col in self.pk_columns if col not in self.column_names]
            if missing_pk_cols:
                self.errors.append(f"Primary key column(s) not found in the dataset: {', '.join(missing_pk_cols)}")
                
            # Check if required columns exist in the dataset
            missing_req_cols = [col for col in self.required_columns if col not in self.column_names]
            if missing_req_cols:
                self.errors.append(f"Required column(s) not found in the dataset: {', '.join(missing_req_cols)}")
                
            return True
        except Exception as e:
            self.errors.append(f"Error loading file: {str(e)}")
            return False
            
    def _load_raw_csv(self) -> None:
        """Load CSV file in raw format to check for structural issues."""
        self.raw_data = []
        try:
            with open(self.file_path, 'r', newline='', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    self.raw_data.append(row)
        except UnicodeDecodeError:
            # Try with different encodings if UTF-8 fails
            encodings = ['latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    self.raw_data = []
                    with open(self.file_path, 'r', newline='', encoding=encoding) as file:
                        csv_reader = csv.reader(file)
                        for row in csv_reader:
                            self.raw_data.append(row)
                    self.warnings.append(f"File encoded with {encoding} instead of UTF-8")
                    break
                except UnicodeDecodeError:
                    continue
            if not self.raw_data:
                self.errors.append("Could not determine file encoding")
        except Exception as e:
            self.errors.append(f"Error reading raw CSV data: {str(e)}")
    
    def validate(self) -> Dict[str, Any]:
        """
        Run all validation checks and return a summary of results.
        
        Returns:
            Dict[str, Any]: Validation results
        """
        if not self.load_file():
            return self._generate_report()
            
        # Run all validation checks
        self.check_file_structure()
        self.check_duplicate_primary_keys()
        self.check_required_fields()
        self.check_data_types()
        self.check_data_ranges()
        self.check_date_formats()
        self.check_special_characters()
        self.check_custom_validation_rules()
        
        return self._generate_report()
    
    def check_file_structure(self) -> None:
        """Check for structural issues in the file."""
        # Check for inconsistent row lengths in raw CSV data
        if self.file_extension == '.csv' and self.raw_data:
            if len(self.raw_data) > 0:
                header_length = len(self.raw_data[0])
                inconsistent_rows = []
                
                for i, row in enumerate(self.raw_data[1:], 2):
                    if len(row) != header_length:
                        inconsistent_rows.append((i, len(row), header_length))
                        if len(inconsistent_rows) >= 10:  # Limit to first 10 issues
                            break
                            
                if inconsistent_rows:
                    self.issue_count += len(inconsistent_rows)
                    inconsistent_details = [f"Row {row}: found {actual} columns, expected {expected}" 
                                           for row, actual, expected in inconsistent_rows]
                    self.errors.append(f"Inconsistent row lengths detected in {len(inconsistent_rows)} rows. "
                                      f"First few issues: {'; '.join(inconsistent_details[:3])}")
        
        # Check for empty rows
        empty_rows = self.data.isna().all(axis=1).sum()
        if empty_rows > 0:
            self.issue_count += empty_rows
            self.warnings.append(f"Found {empty_rows} empty rows in the dataset")
            
        # Check for empty columns
        empty_cols = [col for col in self.column_names if self.data[col].isna().all()]
        if empty_cols:
            self.issue_count += len(empty_cols)
            self.warnings.append(f"Found {len(empty_cols)} empty columns: {', '.join(empty_cols)}")
            
        # Check for columns with mostly empty values (>80% empty)
        sparse_threshold = 0.8
        sparse_cols = []
        for col in self.column_names:
            empty_rate = self.data[col].isna().mean()
            if empty_rate > sparse_threshold:
                sparse_cols.append((col, f"{empty_rate:.1%}"))
                
        if sparse_cols:
            self.issue_count += len(sparse_cols)
            cols_info = [f"{col} ({rate} empty)" for col, rate in sparse_cols]
            self.warnings.append(f"Found {len(sparse_cols)} columns with mostly empty values: {', '.join(cols_info)}")
    
    def check_duplicate_primary_keys(self) -> None:
        """Check for duplicate primary keys in the specified columns."""
        if not self.pk_columns or not all(col in self.column_names for col in self.pk_columns):
            return
            
        # Create a composite key if multiple columns specified
        if len(self.pk_columns) > 1:
            # Create a new column combining all PK columns
            composite_key = self.data[self.pk_columns].astype(str).agg('-'.join, axis=1)
            duplicate_keys = composite_key[composite_key.duplicated()].unique()
            
            if len(duplicate_keys) > 0:
                self.issue_count += len(duplicate_keys)
                example_dupes = ', '.join(duplicate_keys[:3])
                self.errors.append(
                    f"Found {len(duplicate_keys)} duplicate composite primary keys "
                    f"({', '.join(self.pk_columns)}). Examples: {example_dupes}"
                )
                
                # Find the rows with duplicate keys
                for key in duplicate_keys[:5]:  # Limit to first 5 duplicates
                    dupe_indices = composite_key[composite_key == key].index.tolist()
                    self.errors.append(f"Duplicate key '{key}' found in rows: {', '.join(map(str, dupe_indices))}")
        else:
            # Single column primary key
            pk_col = self.pk_columns[0]
            duplicate_mask = self.data.duplicated(subset=[pk_col], keep=False)
            duplicate_keys = self.data.loc[duplicate_mask, pk_col].unique()
            
            if len(duplicate_keys) > 0:
                self.issue_count += len(duplicate_keys)
                example_dupes = ', '.join(map(str, duplicate_keys[:3]))
                self.errors.append(
                    f"Found {len(duplicate_keys)} duplicate values in primary key column '{pk_col}'. "
                    f"Examples: {example_dupes}"
                )
                
                # Find the rows with duplicate keys
                for key in duplicate_keys[:5]:  # Limit to first 5 duplicates
                    dupe_indices = self.data.index[self.data[pk_col] == key].tolist()
                    self.errors.append(f"Duplicate key '{key}' found in rows: {', '.join(map(str, dupe_indices))}")
    
    def check_required_fields(self) -> None:
        """Check for missing values in required fields."""
        if not self.required_columns:
            return
            
        for col in self.required_columns:
            if col not in self.column_names:
                continue
                
            missing_count = self.data[col].isna().sum()
            if missing_count > 0:
                self.issue_count += missing_count
                missing_rows = self.data.index[self.data[col].isna()].tolist()
                example_rows = ', '.join(map(str, missing_rows[:5]))
                self.errors.append(
                    f"Required column '{col}' has {missing_count} missing values. "
                    f"First few in rows: {example_rows}"
                )
    
    def check_data_types(self) -> None:
        """Check for data type consistency in each column."""
        # Get data type configurations from config if available
        data_type_rules = self.config.get('data_types', {})
        
        for col in self.column_names:
            expected_type = data_type_rules.get(col, None)
            
            # Skip columns without specified types
            if not expected_type:
                # Try to infer type for common fields
                if any(name in col.lower() for name in ['id', 'code', 'number']):
                    expected_type = 'integer'
                elif any(name in col.lower() for name in ['date', 'time']):
                    expected_type = 'date'
                elif any(name in col.lower() for name in ['price', 'amount', 'cost', 'total']):
                    expected_type = 'numeric'
                else:
                    continue
            
            # Skip checking completely empty columns
            if self.data[col].isna().all():
                continue
                
            # Check if values match expected type
            issues = []
            sample_size = min(1000, len(self.data))  # Limit check to first 1000 rows for performance
            sample_data = self.data[col].head(sample_size).copy()
            non_null_indices = sample_data.dropna().index
            
            if expected_type == 'integer':
                # Convert to string and check if values match integer pattern
                for idx in non_null_indices:
                    val = str(sample_data[idx]).strip()
                    if not re.match(r'^-?\d+$', val):
                        issues.append((idx, val))
                        
            elif expected_type == 'numeric':
                # Convert to string and check if values match numeric pattern
                for idx in non_null_indices:
                    val = str(sample_data[idx]).strip()
                    if not re.match(r'^-?\d+(\.\d+)?$', val):
                        issues.append((idx, val))
                        
            elif expected_type == 'date':
                # Check if date can be parsed
                for idx in non_null_indices:
                    val = sample_data[idx]
                    try:
                        if isinstance(val, str):
                            pd.to_datetime(val)
                        elif not (isinstance(val, pd.Timestamp) or 
                                 isinstance(val, datetime.date) or 
                                 isinstance(val, datetime.datetime)):
                            issues.append((idx, val))
                    except:
                        issues.append((idx, val))
            
            if issues:
                self.issue_count += len(issues)
                example_issues = [f"Row {idx}: '{val}'" for idx, val in issues[:3]]
                self.warnings.append(
                    f"Column '{col}' has {len(issues)} values that don't match expected type '{expected_type}'. "
                    f"Examples: {'; '.join(example_issues)}"
                )
    
    def check_data_ranges(self) -> None:
        """Check if numeric values are within specified ranges."""
        range_rules = self.config.get('ranges', {})
        
        for col, rule in range_rules.items():
            if col not in self.column_names:
                continue
                
            min_val = rule.get('min')
            max_val = rule.get('max')
            
            if min_val is not None:
                below_min = self.data[self.data[col] < min_val]
                if not below_min.empty:
                    self.issue_count += len(below_min)
                    example_values = below_min[col].head(3).tolist()
                    self.warnings.append(
                        f"Column '{col}' has {len(below_min)} values below minimum {min_val}. "
                        f"Examples: {', '.join(map(str, example_values))}"
                    )
                    
            if max_val is not None:
                above_max = self.data[self.data[col] > max_val]
                if not above_max.empty:
                    self.issue_count += len(above_max)
                    example_values = above_max[col].head(3).tolist()
                    self.warnings.append(
                        f"Column '{col}' has {len(above_max)} values above maximum {max_val}. "
                        f"Examples: {', '.join(map(str, example_values))}"
                    )
    
    def check_date_formats(self) -> None:
        """Check if date columns have consistent formats."""
        date_format_rules = self.config.get('date_formats', {})
        
        # Check columns that appear to contain dates
        for col in self.column_names:
            # Skip if explicit rule is not provided and column doesn't look like a date column
            if col not in date_format_rules and not any(keyword in col.lower() for keyword in ['date', 'time', 'day', 'month', 'year']):
                continue
                
            # Skip checking completely empty columns
            if self.data[col].isna().all():
                continue
                
            # Get non-null values as strings for checking
            values = self.data[col].dropna().astype(str)
            
            # Skip if all values are already datetime objects
            if all(isinstance(x, (pd.Timestamp, datetime.date, datetime.datetime)) for x in self.data[col].dropna()):
                continue
                
            # Check for inconsistent date formats
            date_patterns = defaultdict(int)
            invalid_dates = []
            
            for val in values.head(1000):  # Limit to first 1000 for performance
                # Detect common date patterns
                if re.match(r'^\d{4}-\d{2}-\d{2}', val):
                    date_patterns['YYYY-MM-DD'] += 1
                elif re.match(r'^\d{2}-\d{2}-\d{4}', val):
                    date_patterns['MM-DD-YYYY'] += 1
                elif re.match(r'^\d{2}/\d{2}/\d{4}', val):
                    date_patterns['MM/DD/YYYY'] += 1
                elif re.match(r'^\d{4}/\d{2}/\d{2}', val):
                    date_patterns['YYYY/MM/DD'] += 1
                elif re.match(r'^\d{2}/\d{2}/\d{2}', val):
                    date_patterns['MM/DD/YY'] += 1
                elif re.match(r'^\d{2}-[A-Za-z]{3}-\d{4}', val):
                    date_patterns['DD-MMM-YYYY'] += 1
                elif re.match(r'^\d{2}-[A-Za-z]{3}-\d{2}', val):
                    date_patterns['DD-MMM-YY'] += 1
                else:
                    try:
                        pd.to_datetime(val)
                        date_patterns['other_valid'] += 1
                    except:
                        invalid_dates.append(val)
            
            # Report issues if mixed formats or invalid dates
            if len(date_patterns) > 1:
                self.issue_count += 1
                formats = [f"{fmt} ({count})" for fmt, count in date_patterns.items()]
                self.warnings.append(
                    f"Column '{col}' contains mixed date formats: {', '.join(formats)}"
                )
                
            if invalid_dates:
                self.issue_count += len(invalid_dates)
                examples = invalid_dates[:3]
                self.warnings.append(
                    f"Column '{col}' contains {len(invalid_dates)} values that cannot be parsed as dates. "
                    f"Examples: {', '.join(examples)}"
                )
    
    def check_special_characters(self) -> None:
        """Check for problematic special characters in string columns."""
        # Characters that might cause issues in databases or applications
        problematic_chars = r'[\x00-\x1F\x7F-\x9F\u2028\u2029\'"\\\[\]\{\}\|\^~`]'
        
        for col in self.column_names:
            # Skip non-string columns
            if not pd.api.types.is_string_dtype(self.data[col]):
                continue
                
            # Check for problematic special characters
            has_special_chars = self.data[col].astype(str).str.contains(problematic_chars, regex=True, na=False)
            count = has_special_chars.sum()
            
            if count > 0:
                self.issue_count += count
                problematic_rows = self.data.index[has_special_chars].tolist()
                example_values = self.data.loc[problematic_rows[:3], col].tolist()
                self.warnings.append(
                    f"Column '{col}' has {count} values with potentially problematic special characters. "
                    f"Examples: {', '.join(map(str, example_values))}"
                )
    
    def check_custom_validation_rules(self) -> None:
        """Apply custom validation rules from configuration."""
        custom_rules = self.config.get('custom_rules', {})
        
        for rule_name, rule_config in custom_rules.items():
            column = rule_config.get('column')
            if not column or column not in self.column_names:
                continue
                
            rule_type = rule_config.get('type')
            if not rule_type:
                continue
                
            # Apply different types of custom rules
            if rule_type == 'regex':
                pattern = rule_config.get('pattern')
                if not pattern:
                    continue
                    
                # Check if values match the regex pattern
                non_matching = ~self.data[column].astype(str).str.match(pattern, na=False)
                count = non_matching.sum()
                
                if count > 0:
                    self.issue_count += count
                    example_values = self.data.loc[non_matching, column].head(3).tolist()
                    self.warnings.append(
                        f"Custom rule '{rule_name}': {count} values in column '{column}' "
                        f"don't match pattern '{pattern}'. Examples: {', '.join(map(str, example_values))}"
                    )
                    
            elif rule_type == 'enum':
                allowed_values = rule_config.get('values', [])
                if not allowed_values:
                    continue
                    
                # Check if values are in the allowed list
                invalid_mask = ~self.data[column].isin(allowed_values) & ~self.data[column].isna()
                count = invalid_mask.sum()
                
                if count > 0:
                    self.issue_count += count
                    invalid_values = self.data.loc[invalid_mask, column].unique().tolist()
                    examples = ', '.join(map(str, invalid_values[:5]))
                    self.warnings.append(
                        f"Custom rule '{rule_name}': {count} values in column '{column}' "
                        f"are not in the allowed list. Invalid values include: {examples}"
                    )
                    
            elif rule_type == 'dependency':
                depends_on = rule_config.get('depends_on')
                condition = rule_config.get('condition', 'not_null')
                
                if not depends_on or depends_on not in self.column_names:
                    continue
                    
                if condition == 'not_null':
                    # If depends_on is not null, column should not be null
                    dep_not_null = ~self.data[depends_on].isna()
                    col_is_null = self.data[column].isna()
                    violations = dep_not_null & col_is_null
                    count = violations.sum()
                    
                    if count > 0:
                        self.issue_count += count
                        example_rows = self.data.index[violations].tolist()[:5]
                        self.warnings.append(
                            f"Custom rule '{rule_name}': {count} rows have values in '{depends_on}' "
                            f"but missing values in '{column}'. Example rows: {', '.join(map(str, example_rows))}"
                        )
    
    def _generate_report(self) -> Dict[str, Any]:
        """Generate a comprehensive validation report."""
        report = {
            "file_name": os.path.basename(self.file_path),
            "file_type": self.file_extension,
            "row_count": len(self.data) if self.data is not None else 0,
            "column_count": len(self.column_names) if self.column_names else 0,
            "issue_count": self.issue_count,
            "errors": self.errors,
            "warnings": self.warnings,
            "passed": self.issue_count == 0 and not self.errors
        }
        
        # Add column statistics if data was loaded successfully
        if self.data is not None:
            report["columns"] = {}
            for col in self.column_names:
                column_stats = {
                    "type": str(self.data[col].dtype),
                    "missing_values": int(self.data[col].isna().sum()),
                    "missing_percentage": float(self.data[col].isna().mean() * 100),
                    "unique_values": int(self.data[col].nunique())
                }
                
                # Add numeric stats if applicable
                if pd.api.types.is_numeric_dtype(self.data[col]):
                    column_stats.update({
                        "min": self.data[col].min() if not self.data[col].isna().all() else None,
                        "max": self.data[col].max() if not self.data[col].isna().all() else None,
                        "mean": float(self.data[col].mean()) if not self.data[col].isna().all() else None,
                    })
                # Add string stats if applicable
                elif pd.api.types.is_string_dtype(self.data[col]):
                    non_null = self.data[col].dropna()
                    if not non_null.empty:
                        length_stats = non_null.str.len().describe()
                        column_stats.update({
                            "min_length": int(length_stats['min']),
                            "max_length": int(length_stats['max']),
                            "mean_length": float(length_stats['mean']),
                        })
                
                report["columns"][col] = column_stats
        
        return report

    def print_report(self, report: Dict[str, Any]) -> None:
        """
        Print the validation report in a human-readable format.
        
        Args:
            report (Dict[str, Any]): The validation report
        """
        # File information
        print(f"\n{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}DATA VALIDATION REPORT{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
        
        print(f"\n{Fore.CYAN}FILE INFORMATION:{Style.RESET_ALL}")
        print(f"File: {report['file_name']}")
        print(f"Type: {report['file_type']}")
        print(f"Rows: {report['row_count']}")
        print(f"Columns: {report['column_count']}")
        
        # Summary
        print(f"\n{Fore.CYAN}VALIDATION SUMMARY:{Style.RESET_ALL}")
        if report['passed']:
            print(f"{Fore.GREEN}✓ Validation passed with no critical issues{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}✗ Validation failed with {len(report['errors'])} errors and {len(report['warnings'])} warnings{Style.RESET_ALL}")
            print(f"Total issues: {report['issue_count']}")
        
        # Errors
        if report['errors']:
            print(f"\n{Fore.RED}ERRORS:{Style.RESET_ALL}")
            for i, error in enumerate(report['errors'], 1):
                print(f"{i}. {error}")
        
        # Warnings
        if report['warnings']:
            print(f"\n{Fore.YELLOW}WARNINGS:{Style.RESET_ALL}")
            for i, warning in enumerate(report['warnings'], 1):
                print(f"{i}. {warning}")
        
        # Column statistics
        if 'columns' in report and report['columns']:
            print(f"\n{Fore.CYAN}COLUMN STATISTICS:{Style.RESET_ALL}")
            
            # Prepare data for tabulate
            table_data = []
            for col_name, stats in report['columns'].items():
                row = [
                    col_name,
                    stats['type'],
                    stats['missing_values'],
                    f"{stats['missing_percentage']:.1f}%",
                    stats['unique_values']
                ]
                
                # Add numeric stats if available
                if 'min' in stats and stats['min'] is not None:
                    row.append(f"{stats['min']} - {stats['max']}")
                else:
                    row.append("N/A")
                
                table_data.append(row)
            
            headers = ["Column", "Type", "Missing", "Missing %", "Unique", "Range"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        print(f"\n{Fore.CYAN}{'='*80}{Style.RESET_ALL}")

def main():
    """Main function to run the data validator from command line."""
    parser = argparse.ArgumentParser(description='Validate CSV and Excel files for database ingestion')
    parser.add_argument('input_file', help='Path to the CSV or Excel file to validate')
    parser.add_argument('--pk', help='Column name(s) to use as primary key(s), comma-separated')
    parser.add_argument('--required', help='Column name(s) that must not be empty, comma-separated')
    parser.add_argument('--config', help='Path to JSON configuration file with validation rules')
    parser.add_argument('--output', help='Path to save JSON report (optional)')
    args = parser.parse_args()
    
    # Parse comma-separated column names
    pk_columns = args.pk.split(',') if args.pk else []
    required_columns = args.required.split(',') if args.required else []
    
    # Create validator and run validation
    validator = DataValidator(
        file_path=args.input_file,
        pk_columns=pk_columns,
        required_columns=required_columns,
        config_path=args.config
    )
    
    report = validator.validate()
    validator.print_report(report)
    
    # Save report to file if output path is provided
    if args.output:
        try:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"\nReport saved to {args.output}")
        except Exception as e:
            print(f"\nError saving report: {e}")

            # Exit with error code if validation failed
            if not report['passed']:
                sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()