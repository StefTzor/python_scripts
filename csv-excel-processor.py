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
from datetime import datetime

class FileProcessor:
    """
    A class to process CSV and Excel files with specific transformations.
    
    This class handles the reading, transformation, and writing of data files
    based on configurable column mappings and transformations.
    """
    
    def __init__(self, input_file, output_file=None, output_format=None):
        """
        Initialize the FileProcessor with input and output file details.
        
        Args:
            input_file (str): Path to the input CSV or Excel file
            output_file (str, optional): Path to the output file. If not provided,
                                         a name will be generated based on the input file.
            output_format (str, optional): Format of the output file ('csv' or 'excel').
                                           If not provided, it will be inferred from the output file extension.
        """
        self.input_file = input_file
        
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
        try:
            if self.input_ext in ['.csv']:
                self.data = pd.read_csv(self.input_file)
            elif self.input_ext in ['.xlsx', '.xls']:
                self.data = pd.read_excel(self.input_file)
            else:
                print(f"Unsupported file format: {self.input_ext}")
                return False
            
            print(f"File loaded successfully with {len(self.data)} rows and {len(self.data.columns)} columns.")
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
        
    def set_output_headers(self, headers):
        """
        Set the desired column headers for the output file.
        
        Args:
            headers (list): List of desired output column headers
        """
        self.output_headers = headers
        
    def set_column_mapping(self, mapping):
        """
        Set the mapping from input column headers to output column headers.
        
        Args:
            mapping (dict): Dictionary mapping input headers to output headers
        """
        self.column_mapping = mapping
        
    def validate_input_headers(self):
        """
        Validate that all expected input headers exist in the loaded data.
        
        Returns:
            bool: True if validation passes, False otherwise
        """
        if not self.data is not None:
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
            
        # Create a copy of the DataFrame to work with
        transformed_data = self.data.copy()
        
        # Rename columns according to the mapping
        columns_to_keep = {}
        for input_col, output_col in self.column_mapping.items():
            if input_col in transformed_data.columns:
                columns_to_keep[input_col] = output_col
            else:
                print(f"Warning: Input column '{input_col}' not found in data.")
        
        # Select only the columns we want to keep and rename them
        transformed_data = transformed_data[list(columns_to_keep.keys())].rename(columns=columns_to_keep)
        
        # Check if the tag column exists in the renamed DataFrame
        output_tag_column = self.column_mapping.get(tag_column, tag_column)
        if output_tag_column not in transformed_data.columns:
            print(f"Warning: Tag column '{output_tag_column}' not found in transformed data.")
            split_tags = False
        
        # Split rows based on tags if specified
        result_data = []
        if split_tags:
            for _, row in transformed_data.iterrows():
                tags_value = row[output_tag_column]
                
                # Skip if tags value is empty or NaN
                if pd.isna(tags_value) or tags_value == '':
                    new_row = row.copy()
                    new_row['unique_id'] = self.generate_unique_id()
                    result_data.append(new_row)
                    continue
                
                # Parse the tags using regex to handle the specific format with quotes
                tags = re.findall(r'"([^"]*)"', str(tags_value))
                
                if not tags:
                    # If no tags were found, keep the original value as a single tag
                    new_row = row.copy()
                    new_row['unique_id'] = self.generate_unique_id()
                    result_data.append(new_row)
                else:
                    # Create a new row for each tag
                    for tag in tags:
                        new_row = row.copy()
                        new_row[output_tag_column] = tag
                        new_row['unique_id'] = self.generate_unique_id()
                        result_data.append(new_row)
        else:
            # If not splitting tags, just add unique IDs
            for _, row in transformed_data.iterrows():
                new_row = row.copy()
                new_row['unique_id'] = self.generate_unique_id()
                result_data.append(new_row)
        
        # Convert the list of rows back to a DataFrame
        result_df = pd.DataFrame(result_data)
        
        # Ensure all output headers are in the DataFrame
        for header in self.output_headers:
            if header not in result_df.columns:
                result_df[header] = ''  # Add empty column if missing
        
        # Reorder columns to match the specified output headers
        result_df = result_df[self.output_headers]
        
        return result_df
    
    def save_output_file(self, data):
        """
        Save the transformed data to the output file.
        
        Args:
            data (pandas.DataFrame): The DataFrame to save
            
        Returns:
            bool: True if saving was successful, False otherwise
        """
        try:
            if self.output_format == 'csv':
                data.to_csv(self.output_file, index=False)
            else:  # excel format
                data.to_excel(self.output_file, index=False)
                
            print(f"Data successfully saved to {self.output_file}")
            print(f"Output file contains {len(data)} rows and {len(data.columns)} columns.")
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
        return self.save_output_file(transformed_data)

def main():
    """
    Main function to run the file processor from command line arguments.
    """
    parser = argparse.ArgumentParser(description='Process CSV/Excel files with specific transformations.')
    parser.add_argument('input_file', help='Path to the input CSV or Excel file')
    parser.add_argument('--output-file', help='Path to the output file')
    parser.add_argument('--output-format', choices=['csv', 'excel'], help='Format of the output file')
    args = parser.parse_args()
    
    processor = FileProcessor(args.input_file, args.output_file, args.output_format)
    
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
    success = processor.process_file('TAGS')
    
    if success:
        print("File processing completed successfully.")
    else:
        print("File processing failed.")

if __name__ == "__main__":
    main()
