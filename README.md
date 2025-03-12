# python_scripts

A selection of Python scripts for file processing, validation, and splitting.

## Requirements

- Python 3.x
- pandas
- openpyxl
- tqdm
- colorama
- tabulate

### Setting Up a Virtual Environment

It is recommended to use a virtual environment to manage dependencies. Follow these steps to create and activate a virtual environment:

1. **Create a virtual environment**:

   ```sh
   python3 -m venv venv
   ```

2. **Activate the virtual environment**:

   - On Linux and macOS:

     ```sh
     source venv/bin/activate
     ```

   - On Windows:

     ```sh
     .\venv\Scripts\activate
     ```

3. **Install the required packages**:

   ```sh
   pip install pandas openpyxl tqdm colorama tabulate
   ```

## Scripts

### 1. `file-splitter.py`

This script splits large CSV or Excel files into smaller files compatible with spreadsheet applications like Google Sheets, Excel, or LibreOffice.

#### Usage

Basic usage:

```sh
python file-splitter.py large_data_file.csv
```

Advanced usage:

```sh
python file-splitter.py large_data_file.xlsx --output-dir=split_files --target-app=google_sheets --output-format=csv --max-rows=30000 --max-size-mb=15
```

#### Command-Line Arguments

- `input_file`: Path to the large CSV or Excel file to split
- `--output-dir`: Directory to save output files (optional)
- `--target-app`: Target application (excel, google_sheets, or libreoffice)
- `--output-format`: Format for output files (csv or excel)
- `--max-rows`: Maximum number of rows per output file
- `--max-size-mb`: Maximum file size in MB per output file
- `--chunk-size`: Number of rows to process at a time (for memory efficiency)

### 2. `data-validator.py`

This script validates CSV and Excel files before database ingestion by checking for various issues like missing values, duplicate primary keys, data type consistency, and more.

#### Usage

Basic usage:

```sh
python data-validator.py input_file.csv --pk id
```

Advanced usage:

```sh
python data-validator.py input_file.xlsx --pk "id,email" --required "name,email,phone" --config data-validator-config.json
```

#### Command-Line Arguments

- `input_file`: Path to the CSV or Excel file to validate
- `--pk`: Column name(s) to use as primary key(s) for duplicate detection
- `--required`: Comma-separated list of columns that must not contain empty values
- `--config`: Path to JSON configuration file with validation rules

### 3. `csv-excel-processor.py`

This script processes input CSV or Excel files by reading input files, validating column headers, renaming specified columns, transforming data, generating unique identifiers, and outputting the processed data to a new file.

#### Usage

Basic usage:

```sh
python csv-excel-processor.py input_file.csv
```

Advanced usage:

```sh
python csv-excel-processor.py input_file.csv --output-file=processed_file.csv --output-format=csv --chunk-size=5000
```

#### Command-Line Arguments

- `input_file`: Path to the input CSV or Excel file
- `--output-file`: Path to the output file (optional)
- `--output-format`: Format of the output file (csv or excel)
- `--chunk-size`: Number of rows to process at a time

## License

This repository is licensed under the MIT License. See the [LICENSE](LICENSE) file for more information.