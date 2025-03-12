#!/usr/bin/env python3
"""
Large File Splitter for Spreadsheet Applications

This script takes a large CSV or Excel file and splits it into multiple smaller files
that are compatible with spreadsheet applications like Google Sheets, Excel, or LibreOffice.

Features:
1. Supports both CSV and Excel files as input
2. Allows splitting by number of rows or file size
3. Preserves headers in all output files
4. Provides detailed progress reporting
5. Configurable output format

Author: Stefanos Tzortzoglou with Claude
Date: March 12, 2025

How to Use the Script
Basic Usage
python file_splitter.py large_data_file.csv
This will:

Create a new directory named after your input file (e.g., large_data_file_split_20250312_123456/)
Split the file into multiple parts with appropriate size for Excel (default)
Use the same format as the input file for output files

Advanced Usage
python file_splitter.py large_data_file.xlsx --output-dir=split_files --target-app=google_sheets --output-format=csv --max-rows=30000 --max-size-mb=15
This will:

Create a directory called split_files/
Split the Excel file into CSV files optimized for Google Sheets
Limit each file to 30,000 rows and 15 MB
Show detailed progress while processing

Command-Line Arguments

input_file: Path to the large CSV or Excel file to split
--output-dir: Directory to save output files (optional)
--target-app: Target application (excel, google_sheets, or libreoffice)
--output-format: Format for output files (csv or excel)
--max-rows: Maximum number of rows per output file
--max-size-mb: Maximum file size in MB per output file
--chunk-size: Number of rows to process at a time (for memory efficiency)

Application-Specific Defaults
The script includes sensible defaults for different spreadsheet applications:
ApplicationRow LimitSize Limit (MB)NotesExcel200,00020Excel's actual limit is 1,048,576 rowsGoogle Sheets50,00015Google's actual limit is 10M rows or 10 MB for importLibreOffice200,00020LibreOffice's actual limit is 1,048,576 rows
These conservative defaults ensure good performance in the target applications.
How to Install and Run

Install Python Dependencies:
pip install pandas openpyxl tqdm

Save the Script:
Save the provided code as file_splitter.py
Run the Script:
python file_splitter.py your_large_file.csv

Check Output:
The script will create a new directory with multiple files named like:
your_large_file_part001.csv
your_large_file_part002.csv
your_large_file_part003.csv
etc.


How to Modify the Script
The script is well-documented and modular, making it easy to modify:

Change Default Limits:

Edit the LIMITS class constant in the FileSplitter class


Add Support for New Applications:

Add a new entry to the LIMITS dictionary with appropriate values


Modify Output File Naming:

Edit the get_output_filename method


Change the Processing Logic:

Modify the split_file method



Performance Considerations

Memory Usage: The script is designed to be memory-efficient, processing files in chunks
Excel Files: Processing Excel files requires more memory than CSV files
Very Large Files: For files over 1 GB, consider using CSV format for better performance
Chunk Size: Adjust the --chunk-size parameter if you experience memory issues

Example Use Cases

Preparing Data for Team Analysis:
Split a large dataset into Excel files for distribution to multiple team members
Google Sheets Import:
Break up files that exceed Google Sheets' import limits
Performance Optimization:
Split large files that are slow to open in spreadsheet applications

Troubleshooting
If you encounter issues:

Memory Errors: Reduce the chunk size with --chunk-size=5000
Excel Limitations: Use CSV output format for very large files
Progress Seems Stuck: Excel files take longer to read than CSV; be patient
CSV Encoding Issues: If you have special characters, modify the script to specify encoding

"""

import pandas as pd
import os
import argparse
import time
import sys
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import math


class FileSplitter:
    """
    A class to split large CSV or Excel files into smaller chunks.
    
    This class handles reading large files and splitting them into smaller files
    that are more compatible with spreadsheet applications like Google Sheets,
    Excel, or LibreOffice.
    """
    
    # Class constants for different spreadsheet application limits
    LIMITS = {
        'excel': {
            'rows': 1048576,  # Excel row limit
            'size_mb': 20     # Conservative size for good performance
        },
        'google_sheets': {
            'rows': 10000000,  # Google Sheets row limit (10 million as of 2025)
            'size_mb': 15     # Google Sheets recommended file size limit
        },
        'libreoffice': {
            'rows': 1048576,  # LibreOffice Calc row limit (same as Excel)
            'size_mb': 20     # Conservative size for good performance
        }
    }
    
    def __init__(self, input_file, output_dir=None, target_app='excel', output_format=None, 
                 max_rows=None, max_size_mb=None, chunk_size=10000):
        """
        Initialize the FileSplitter with input and output details.
        
        Args:
            input_file (str): Path to the input CSV or Excel file
            output_dir (str, optional): Directory to save output files. If not provided,
                                        a directory named after the input file will be created.
            target_app (str, optional): Target spreadsheet application ('excel', 'google_sheets', 'libreoffice').
                                        Determines default limits. Defaults to 'excel'.
            output_format (str, optional): Format of the output files ('csv' or 'excel').
                                           If not provided, it will be inferred from the input file.
            max_rows (int, optional): Maximum number of rows per output file.
                                      Overrides the target_app setting.
            max_size_mb (int, optional): Maximum file size in MB per output file.
                                         Overrides the target_app setting.
            chunk_size (int, optional): Number of rows to process at a time.
        """
        self.input_file = input_file
        self.chunk_size = chunk_size
        
        # Determine input file extension
        self.input_ext = os.path.splitext(input_file)[1].lower()
        
        # Set output directory
        if output_dir is None:
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = f"{base_name}_split_{timestamp}"
        
        self.output_dir = output_dir
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        # Set target application and limits
        self.target_app = target_app
        
        if max_rows is None:
            self.max_rows = self.LIMITS[target_app]['rows']
            # Use a more conservative default limit for better performance
            if target_app == 'excel':
                self.max_rows = min(self.max_rows, 200000)
            elif target_app == 'google_sheets':
                self.max_rows = min(self.max_rows, 50000)
            elif target_app == 'libreoffice':
                self.max_rows = min(self.max_rows, 200000)
        else:
            self.max_rows = max_rows
            
        self.max_size_mb = max_size_mb if max_size_mb is not None else self.LIMITS[target_app]['size_mb']
        
        # Determine output format from input if not specified
        if output_format is None:
            self.output_format = 'excel' if self.input_ext in ['.xlsx', '.xls'] else 'csv'
        else:
            self.output_format = output_format.lower()
            
        # Set output file extension
        self.output_ext = '.xlsx' if self.output_format == 'excel' else '.csv'
            
        # Initialize data and metrics
        self.data = None
        self.headers = None
        self.start_time = None
        self.total_rows = 0
        self.processed_rows = 0
        self.num_files_created = 0
        self.avg_row_size_bytes = 0
        
    def load_file_info(self):
        """
        Load basic file information to estimate splitting parameters.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        print(f"Analyzing file: {self.input_file}")
        self.start_time = time.time()
        
        try:
            # Get file size
            file_size_bytes = os.path.getsize(self.input_file)
            file_size_mb = file_size_bytes / (1024 * 1024)
            print(f"File size: {file_size_mb:.2f} MB")
            
            # Count rows and estimate average row size
            if self.input_ext in ['.csv']:
                print("Counting rows in CSV file (this might take a moment for large files)...")
                with open(self.input_file, 'r') as f:
                    # Get first line to extract headers
                    first_line = f.readline().strip()
                    # Count remaining lines
                    self.total_rows = sum(1 for _ in f) + 1  # Including header
                
                # Estimate average row size
                if self.total_rows > 1:
                    self.avg_row_size_bytes = file_size_bytes / self.total_rows
                
            elif self.input_ext in ['.xlsx', '.xls']:
                print("Reading Excel file information (this might take longer than CSV)...")
                # Load Excel file info without reading all data
                xl = pd.ExcelFile(self.input_file)
                sheet_name = xl.sheet_names[0]  # Use first sheet
                xl_info = xl.parse(sheet_name, nrows=0)
                
                # Get actual row count - need to read the file
                sample_chunk = pd.read_excel(self.input_file, sheet_name=sheet_name, nrows=self.chunk_size)
                self.headers = list(sample_chunk.columns)
                
                # For Excel, we need to read the file to get an accurate row count
                print("Counting rows in Excel file (this might take a while)...")
                df_info = pd.read_excel(self.input_file, sheet_name=sheet_name)
                self.total_rows = len(df_info)
                
                # Estimate average row size
                if self.total_rows > 1:
                    self.avg_row_size_bytes = file_size_bytes / self.total_rows
                
                # Clean up to free memory
                del df_info
            else:
                print(f"Unsupported file format: {self.input_ext}")
                return False
            
            print(f"Total rows: {self.total_rows:,}")
            print(f"Estimated average row size: {self.avg_row_size_bytes:.2f} bytes")
            
            # Calculate splitting parameters
            max_size_bytes = self.max_size_mb * 1024 * 1024
            rows_per_file_by_size = int(max_size_bytes / self.avg_row_size_bytes) if self.avg_row_size_bytes > 0 else self.max_rows
            rows_per_file = min(self.max_rows, rows_per_file_by_size)
            
            estimated_num_files = math.ceil(self.total_rows / rows_per_file)
            print(f"Will split into approximately {estimated_num_files} files with up to {rows_per_file:,} rows each")
            
            return True
        except Exception as e:
            print(f"Error analyzing file: {e}")
            return False
    
    def get_output_filename(self, file_index):
        """
        Generate an output filename based on the input filename and part number.
        
        Args:
            file_index (int): Index of the output file (0-based)
            
        Returns:
            str: Path to the output file
        """
        base_name = os.path.splitext(os.path.basename(self.input_file))[0]
        return os.path.join(self.output_dir, f"{base_name}_part{file_index + 1:03d}{self.output_ext}")
    
    def split_file(self):
        """
        Split the input file into multiple smaller files based on configured limits.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        print("\n" + "="*50)
        print("STARTING FILE SPLITTING")
        print("="*50)
        
        if not self.load_file_info():
            return False
        
        print("\nSplitting file...")
        split_start_time = time.time()
        
        try:
            # Calculate rows per file based on size and row limits
            max_size_bytes = self.max_size_mb * 1024 * 1024
            rows_per_file_by_size = int(max_size_bytes / self.avg_row_size_bytes) if self.avg_row_size_bytes > 0 else self.max_rows
            rows_per_file = min(self.max_rows, rows_per_file_by_size)
            
            if rows_per_file <= 0:
                rows_per_file = self.chunk_size
            
            print(f"Using {rows_per_file:,} rows per output file")
            
            # Read and process input file in chunks
            reader = None
            current_chunk = None
            output_chunk = []
            current_output_rows = 0
            file_index = 0
            
            # Setup appropriate reader based on file type
            if self.input_ext in ['.csv']:
                reader = pd.read_csv(self.input_file, chunksize=self.chunk_size)
                # For CSV, we'll get headers from the first chunk
            elif self.input_ext in ['.xlsx', '.xls']:
                # For Excel, we read the whole file but process it in chunks
                # This is because pandas doesn't support true chunked reading for Excel
                print("Reading Excel file (this may take a while)...")
                full_data = pd.read_excel(self.input_file)
                self.headers = list(full_data.columns)
                
                # Create a chunk generator to simulate chunked reading
                def excel_chunk_generator(data, chunk_size):
                    for i in range(0, len(data), chunk_size):
                        yield data.iloc[i:i + chunk_size]
                
                reader = excel_chunk_generator(full_data, self.chunk_size)
                # Clean up to free memory after we've created the generator
                del full_data
            else:
                print(f"Unsupported file format: {self.input_ext}")
                return False
            
            # Process chunks with progress tracking
            with tqdm(total=self.total_rows, desc="Processing rows", unit="row") as pbar:
                for chunk in reader:
                    self.processed_rows += len(chunk)
                    
                    # Store headers if this is the first chunk for CSV
                    if self.headers is None and self.input_ext in ['.csv']:
                        self.headers = list(chunk.columns)
                    
                    # Process this chunk
                    for _, row in chunk.iterrows():
                        # If we've reached the row limit for this output file, write it
                        if current_output_rows >= rows_per_file:
                            self._write_output_file(output_chunk, file_index)
                            output_chunk = []
                            current_output_rows = 0
                            file_index += 1
                            self.num_files_created += 1
                        
                        # Add this row to the current output chunk
                        output_chunk.append(row)
                        current_output_rows += 1
                    
                    # Update progress
                    pbar.update(len(chunk))
                    
                    # Provide periodic status updates
                    if self.processed_rows % 50000 == 0 or self.processed_rows == self.total_rows:
                        elapsed = time.time() - self.start_time
                        rows_per_sec = self.processed_rows / elapsed if elapsed > 0 else 0
                        print(f"\nProcessed {self.processed_rows:,}/{self.total_rows:,} rows "
                              f"({self.processed_rows/self.total_rows*100:.1f}%) "
                              f"at {rows_per_sec:.1f} rows/sec")
                        print(f"Created {self.num_files_created} files so far")
            
            # Write the final output file if there are any rows left
            if output_chunk:
                self._write_output_file(output_chunk, file_index)
                self.num_files_created += 1
            
            split_time = time.time() - split_start_time
            total_time = time.time() - self.start_time
            
            print("\n" + "="*50)
            print("FILE SPLITTING COMPLETED")
            print("="*50)
            print(f"Total processing time: {total_time:.2f} seconds")
            print(f"Splitting time: {split_time:.2f} seconds")
            print(f"Created {self.num_files_created} files in {self.output_dir}")
            return True
            
        except KeyboardInterrupt:
            print("\nProcess interrupted by user.")
            return False
        except Exception as e:
            print(f"\nError during file splitting: {e}")
            return False
    
    def _write_output_file(self, rows, file_index):
        """
        Write a set of rows to an output file.
        
        Args:
            rows (list): List of pandas Series objects (rows)
            file_index (int): Index of the output file
            
        Returns:
            bool: True if successful, False otherwise
        """
        output_file = self.get_output_filename(file_index)
        
        try:
            # Convert rows to DataFrame
            df = pd.DataFrame(rows, columns=self.headers)
            
            # Write to file based on output format
            if self.output_format == 'csv':
                df.to_csv(output_file, index=False)
            else:  # excel
                df.to_excel(output_file, index=False)
            
            file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
            print(f"Created file {file_index + 1}: {output_file} ({len(df):,} rows, {file_size_mb:.2f} MB)")
            
            return True
        except Exception as e:
            print(f"Error writing output file {output_file}: {e}")
            return False


def main():
    """
    Main function to run the file splitter from command line arguments.
    """
    parser = argparse.ArgumentParser(description='Split large CSV/Excel files into smaller files for spreadsheet applications.')
    parser.add_argument('input_file', help='Path to the input CSV or Excel file')
    parser.add_argument('--output-dir', help='Directory to save output files')
    parser.add_argument('--target-app', choices=['excel', 'google_sheets', 'libreoffice'], default='excel',
                      help='Target spreadsheet application (determines default limits)')
    parser.add_argument('--output-format', choices=['csv', 'excel'], 
                      help='Format of the output files')
    parser.add_argument('--max-rows', type=int, 
                      help='Maximum number of rows per output file')
    parser.add_argument('--max-size-mb', type=int, 
                      help='Maximum file size in MB per output file')
    parser.add_argument('--chunk-size', type=int, default=10000, 
                      help='Number of rows to process at a time')
    args = parser.parse_args()
    
    print(f"Large File Splitter for Spreadsheet Applications")
    print(f"Input file: {args.input_file}")
    print(f"Output directory: {args.output_dir or 'Auto-generated'}")
    print(f"Target application: {args.target_app}")
    print(f"Output format: {args.output_format or 'Auto-detected'}")
    print(f"Maximum rows per file: {args.max_rows or 'Default for ' + args.target_app}")
    print(f"Maximum file size (MB): {args.max_size_mb or 'Default for ' + args.target_app}")
    print(f"Processing chunk size: {args.chunk_size}")
    
    splitter = FileSplitter(
        args.input_file,
        args.output_dir,
        args.target_app,
        args.output_format,
        args.max_rows,
        args.max_size_mb,
        args.chunk_size
    )
    
    try:
        success = splitter.split_file()
        
        if success:
            print("\nFile splitting completed successfully.")
            sys.exit(0)
        else:
            print("\nFile splitting failed.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
