#!/usr/bin/env python3
"""
CSV/Excel File Processor

This script processes input CSV or Excel files by:
1. Reading input files
2. Validating column headers
3. Renaming specified columns
4. Transforming data (splitting rows based on tags)
5. Generating unique identifiers
6. Outputting the processed data to a new file

Author: Claude
Date: March 12, 2025
"""

import pandas as pd
import os
import argparse
import re
import uuid
import time
import sys
from datetime import datetime
from tqdm import tqdm

class FileProcessor:
    """
    A class to process CSV and Excel files with specific transformations.
    
    This class handles the reading, transformation, and writing of data files
    based on configurable column mappings and transformations.
    """
    
    def __init__(self, input_file, output_file=None, output_format=None, chunk_size=10000):
        """
        Initialize the FileProcessor with input and output file details.
        
        Args:
            input_file (str): Path to the input CSV or Excel file
            output_file (str, optional): Path to the output file. If not provided,
                                         a name will be generated based on the input file.
            output_format (str, optional): Format of the output file ('csv' or 'excel').
                                           If not provided, it will be inferred from the output file extension.
            chunk_size (int, optional): Number of rows to process at a time for large files.
        """
        self.input_file = input_file
        self.chunk_size = chunk_size
        
        # Determine input file extension
        self.input_ext = os.path.splitext(input_file)[1].lower()
        
        # Set output file if not provided
        if output_file is None:
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"{base_name}_processed_{timestamp}"
            
            # Add appropriate extension based on output_format or input format
            if output_format:
                output_file += ".xlsx" if output_format.lower() == 'excel' else ".csv"
            else:
                output_file += self.input_ext
        
        self.output_file = output_file
        
        # Determine output format from extension if not specified
        if output_format is None:
            self.output_format = 'excel' if os.path.splitext(output_file)[1].lower() in ['.xlsx', '.xls'] else 'csv'
        else:
            self.output_format = output_format.lower()
            
        # Initialize data and configuration properties
        self.data = None
        self.expected_input_headers = []
        self.output_headers = []
        self.column_mapping = {}
        self.unique_id_prefix = datetime.now().strftime('%d%m%y')
        self.unique_id_counter = 1
        self.two_code_identifier = self._generate_two_code_identifier()
        
        # Performance monitoring
        self.start_time = None
        self.processed_rows = 0
        self.total_rows = 0
        
    def _generate_two_code_identifier(self):
        """
        Generate a random two-letter code identifier.
        
        Returns:
            str: A two-letter uppercase code
        """
        # Using the first two letters of a UUID to generate a random two-letter code
        return str(uuid.uuid4()).upper()[:2]
    
    def load_file(self):
        """
        Load the input file into a pandas DataFrame based on the file extension.
        
        Returns:
            bool: True if file loading was successful, False otherwise
        """
        print(f"Starting to load file: {self.input_file}")
        self.start_time = time.time()
        
        try:
            if self.input_ext in ['.csv']:
                # First count the number of rows for progress reporting
                print("Counting total rows (this might take a moment for large files)...")
                with open(self.input_file, 'r') as f:
                    self.total_rows = sum(1 for _ in f) - 1  # Subtract header row
                
                print(f"File contains approximately {self.total_rows:,} rows.")
                print("Loading data...")
                
                self.data = pd.read_csv(self.input_file)
            elif self.input_ext in ['.xlsx', '.xls']:
                print("Loading Excel file (this might take longer than CSV)...")
                self.data = pd.read_excel(self.input_file)
                self.total_rows = len(self.data)
            else:
                print(f"Unsupported file format: {self.input_ext}")
                return False
            
            load_time = time.time() - self.start_time
            print(f"File loaded successfully in {load_time:.2f} seconds.")
            print(f"File contains {len(self.data):,} rows and {len(self.data.columns)} columns.")
            
            # Memory usage information
            memory_usage = self.data.memory_usage(deep=True).sum() / (1024 * 1024)
            print(f"Memory usage: {memory_usage:.2f} MB")
            
            return True
        except Exception as e:
            print(f"Error loading file: {e}")
            return False
    
    def set_expected_input_headers(self, headers):
        """
        Set the expected column headers for the input file.
        
        Args:
            headers (list): List of expected column headers
        """
        self.expected_input_headers = headers
        print(f"Set {len(headers)} expected input headers.")
        
    def set_output_headers(self, headers):
        """
        Set the desired column headers for the output file.
        
        Args:
            headers (list): List of desired output column headers
        """
        self.output_headers = headers
        print(f"Set {len(headers)} output headers: {', '.join(headers)}")
        
    def set_column_mapping(self, mapping):
        """
        Set the mapping from input column headers to output column headers.
        
        Args:
            mapping (dict): Dictionary mapping input headers to output headers
        """
        self.column_mapping = mapping
        print(f"Set column mapping: {mapping}")
        
    def validate_input_headers(self):
        """
        Validate that all expected input headers exist in the loaded data.
        
        Returns:
            bool: True if validation passes, False otherwise
        """
        print("Validating input headers...")
        
        if self.data is None:
            print("No data loaded. Please load the file first.")
            return False
            
        if not self.expected_input_headers:
            print("Expected input headers not set.")
            return False
            
        missing_headers = [header for header in self.expected_input_headers if header not in self.data.columns]
        
        if missing_headers:
            print(f"Missing expected headers: {missing_headers}")
            return False
            
        print("Input headers validation passed.")
        return True
    
    def generate_unique_id(self):
        """
        Generate a unique identifier in the format "DDMMYY_XX_NNNNN".
        
        Returns:
            str: A unique identifier string
        """
        unique_id = f"{self.unique_id_prefix}_{self.two_code_identifier}_{self.unique_id_counter:05d}"
        self.unique_id_counter += 1
        return unique_id
    
    def transform_data(self, tag_column, split_tags=True):
        """
        Transform the data based on specified requirements:
        1. Rename columns according to mapping
        2. Split rows based on comma-separated tags if specified
        3. Generate unique IDs for each row
        
        Args:
            tag_column (str): The name of the column containing tags to split (in input file)
            split_tags (bool, optional): Whether to split rows based on tags. Defaults to True.
            
        Returns:
            pandas.DataFrame: The transformed DataFrame
        """
        if self.data is None:
            print("No data loaded. Please load the file first.")
            return None
            
        print("Starting data transformation...")
        transform_start_time = time.time()
        
        # Create a copy of the DataFrame with only the columns we need
        columns_to_keep = list(self.column_mapping.keys())
        print(f"Selecting and renaming {len(columns_to_keep)} columns...")
        
        transformed_data = self.data[columns_to_keep].copy()
        
        # Rename columns according to the mapping
        print("Renaming columns...")
        transformed_data.rename(columns=self.column_mapping, inplace=True)
        
        # Check if the tag column exists in the renamed DataFrame
        output_tag_column = self.column_mapping.get(tag_column, tag_column)
        if output_tag_column not in transformed_data.columns:
            print(f"Warning: Tag column '{output_tag_column}' not found in transformed data.")
            split_tags = False
        
        # Split rows based on tags if specified
        print("Processing rows and splitting tags...")
        result_data = []
        estimated_output_rows = 0
        
        # Process in chunks to save memory
        chunk_size = min(self.chunk_size, len(transformed_data))
        chunks = [transformed_data.iloc[i:i + chunk_size] for i in range(0, len(transformed_data), chunk_size)]
        
        print(f"Processing data in {len(chunks)} chunks of up to {chunk_size} rows each...")
        
        # Count total tags to estimate output size
        if split_tags:
            print("Estimating output size...")
            sample_size = min(1000, len(transformed_data))
            sample = transformed_data.sample(n=sample_size) if len(transformed_data) > 1000 else transformed_data
            
            tag_counts = []
            for _, row in sample.iterrows():
                tags_value = row[output_tag_column]
                if pd.isna(tags_value) or tags_value == '':
                    tag_counts.append(1)
                else:
                    tags = re.findall(r'"([^"]*)"', str(tags_value))
                    tag_counts.append(max(1, len(tags)))
            
            avg_tags_per_row = sum(tag_counts) / len(tag_counts)
            estimated_output_rows = int(len(transformed_data) * avg_tags_per_row)
            print(f"Estimated output rows: ~{estimated_output_rows:,} (average of {avg_tags_per_row:.2f} tags per row)")
            
        # Process the data in chunks with progress bar
        with tqdm(total=len(transformed_data), desc="Processing rows", unit="row") as pbar:
            for chunk_idx, chunk in enumerate(chunks):
                print(f"Processing chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk):,} rows)...")
                chunk_result = []
                
                for _, row in chunk.iterrows():
                    self.processed_rows += 1
                    
                    if split_tags:
                        tags_value = row[output_tag_column]
                        
                        # Skip if tags value is empty or NaN
                        if pd.isna(tags_value) or tags_value == '':
                            new_row = row.copy()
                            new_row['unique_id'] = self.generate_unique_id()
                            chunk_result.append(new_row)
                            continue
                        
                        # Parse the tags using regex to handle the specific format with quotes
                        tags = re.findall(r'"([^"]*)"', str(tags_value))
                        
                        if not tags:
                            # If no tags were found, keep the original value as a single tag
                            new_row = row.copy()
                            new_row['unique_id'] = self.generate_unique_id()
                            chunk_result.append(new_row)
                        else:
                            # Create a new row for each tag
                            for tag in tags:
                                new_row = row.copy()
                                new_row[output_tag_column] = tag
                                new_row['unique_id'] = self.generate_unique_id()
                                chunk_result.append(new_row)
                    else:
                        # If not splitting tags, just add unique IDs
                        new_row = row.copy()
                        new_row['unique_id'] = self.generate_unique_id()
                        chunk_result.append(new_row)
                    
                    # Update progress
                    pbar.update(1)
                    
                    # Provide periodic status updates
                    if self.processed_rows % 10000 == 0:
                        elapsed = time.time() - self.start_time
                        rows_per_sec = self.processed_rows / elapsed if elapsed > 0 else 0
                        print(f"Processed {self.processed_rows:,}/{self.total_rows:,} rows "
                              f"({self.processed_rows/self.total_rows*100:.1f}%) "
                              f"at {rows_per_sec:.1f} rows/sec")
                
                # Append chunk results to overall results
                result_data.extend(chunk_result)
                print(f"Chunk {chunk_idx + 1} complete. Current output size: {len(result_data):,} rows")
                
                # Free memory
                del chunk_result
        
        # Convert the list of rows to a DataFrame
        print(f"Converting {len(result_data):,} processed rows to DataFrame...")
        result_df = pd.DataFrame(result_data)
        
        # Ensure all output headers are in the DataFrame
        missing_columns = [header for header in self.output_headers if header not in result_df.columns]
        if missing_columns:
            print(f"Adding missing columns: {missing_columns}")
            for header in missing_columns:
                result_df[header] = ''
        
        # Reorder columns to match the specified output headers
        print("Reordering columns...")
        result_df = result_df[self.output_headers]
        
        transform_time = time.time() - transform_start_time
        print(f"Data transformation completed in {transform_time:.2f} seconds.")
        print(f"Output data contains {len(result_df):,} rows and {len(result_df.columns)} columns.")
        
        # Memory usage information
        memory_usage = result_df.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"Output memory usage: {memory_usage:.2f} MB")
        
        return result_df
    
    def save_output_file(self, data):
        """
        Save the transformed data to the output file.
        
        Args:
            data (pandas.DataFrame): The DataFrame to save
            
        Returns:
            bool: True if saving was successful, False otherwise
        """
        print(f"Saving data to {self.output_file}...")
        save_start_time = time.time()
        
        try:
            if self.output_format == 'csv':
                print("Writing CSV file...")
                data.to_csv(self.output_file, index=False)
            else:  # excel format
                print("Writing Excel file (this may take a while for large datasets)...")
                
                # For very large datasets, warn about potential Excel limitations
                if len(data) > 1000000:
                    print("WARNING: Excel has a limit of 1,048,576 rows. Some data may be truncated.")
                
                data.to_excel(self.output_file, index=False)
                
            save_time = time.time() - save_start_time
            print(f"Data successfully saved to {self.output_file} in {save_time:.2f} seconds.")
            
            total_time = time.time() - self.start_time
            print(f"Total processing time: {total_time:.2f} seconds.")
            print(f"Output file contains {len(data):,} rows and {len(data.columns)} columns.")
            
            # File size information
            file_size_mb = os.path.getsize(self.output_file) / (1024 * 1024)
            print(f"Output file size: {file_size_mb:.2f} MB")
            
            return True
        except Exception as e:
            print(f"Error saving output file: {e}")
            return False
    
    def process_file(self, tag_column, split_tags=True):
        """
        Process the input file according to the configuration.
        
        This method orchestrates the entire processing workflow:
        1. Load file
        2. Validate headers
        3. Transform data
        4. Save output file
        
        Args:
            tag_column (str): The name of the column containing tags to split (in input file)
            split_tags (bool, optional): Whether to split rows based on tags. Defaults to True.
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        print("\n" + "="*50)
        print("STARTING FILE PROCESSING")
        print("="*50)
        
        # Load the file
        if not self.load_file():
            return False
        
        # Validate the input headers if expected headers are set
        if self.expected_input_headers and not self.validate_input_headers():
            return False
        
        # Transform the data
        transformed_data = self.transform_data(tag_column, split_tags)
        if transformed_data is None:
            return False
        
        # Save the output file
        success = self.save_output_file(transformed_data)
        
        if success:
            print("\n" + "="*50)
            print("FILE PROCESSING COMPLETED SUCCESSFULLY")
            print("="*50)
        else:
            print("\n" + "="*50)
            print("FILE PROCESSING FAILED")
            print("="*50)
            
        return success

def main():
    """
    Main function to run the file processor from command line arguments.
    """
    parser = argparse.ArgumentParser(description='Process CSV/Excel files with specific transformations.')
    parser.add_argument('input_file', help='Path to the input CSV or Excel file')
    parser.add_argument('--output-file', help='Path to the output file')
    parser.add_argument('--output-format', choices=['csv', 'excel'], help='Format of the output file')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Number of rows to process at a time')
    args = parser.parse_args()
    
    print(f"CSV/Excel File Processor")
    print(f"Input file: {args.input_file}")
    print(f"Output file: {args.output_file or 'Auto-generated'}")
    print(f"Output format: {args.output_format or 'Auto-detected'}")
    print(f"Chunk size: {args.chunk_size}")
    
    processor = FileProcessor(args.input_file, args.output_file, args.output_format, args.chunk_size)
    
    # Set expected input headers
    processor.set_expected_input_headers([
        'Email Address', 'First Name', 'Last Name', 'Phone Number',
        'Opt-ins', 'School Name', 'Date of Birth', 'Gender',
        'Address 1', 'Address 2', 'Address 3', 'City',
        'County', 'Postcode', 'Family', 'TM account holder',
        'Online store account holder', 'iFollow account holder', 'Completed Form',
        'Country', 'Online store purchasers', 'TM ticket buyer',
        'Organisation name', 'Discount_17122024', 'Ticketmaster Events',
        'Mailchimp export 01/2024', 'iFollow Purchase season', 'Young Pies',
        'MEMBER_RATING', 'OPTIN_TIME', 'OPTIN_IP', 'CONFIRM_TIME',
        'CONFIRM_IP', 'LATITUDE', 'LONGITUDE', 'GMTOFF',
        'DSTOFF', 'TIMEZONE', 'CC', 'REGION',
        'LAST_CHANGED', 'LEID', 'EUID', 'NOTES',
        'TAGS'
    ])
    
    # Set output headers
    processor.set_output_headers([
        'email', 'first_name', 'last_name', 'campaign_tag', 'unique_id'
    ])
    
    # Set column mapping
    processor.set_column_mapping({
        'Email Address': 'email',
        'First Name': 'first_name',
        'Last Name': 'last_name',
        'TAGS': 'campaign_tag'
    })
    
    # Process the file
    try:
        success = processor.process_file('TAGS')
        
        if success:
            print("\nFile processing completed successfully.")
            sys.exit(0)
        else:
            print("\nFile processing failed.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()