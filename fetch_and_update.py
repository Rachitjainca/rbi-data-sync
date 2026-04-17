"""
RBI Excel Data Fetcher and Google Sheets Updater
Fetches RBI Excel file hourly and updates a single Google Sheet with new worksheets per month
"""

import os
import json
import logging
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rbi_data_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
RBI_EXCEL_URL = "https://rbidocs.rbi.org.in/rdocs/content/docs/PSDDP04062020.xlsx"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
API_RETRY_DELAY = 2  # seconds between API calls
API_MAX_RETRIES = 3  # max retry attempts on rate limit
RATE_LIMIT_DELAY = 0.5  # delay between write operations to avoid quota hits


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame by replacing NaN, infinity, and other problematic values
    
    Args:
        df: DataFrame to clean
        
    Returns:
        Cleaned DataFrame
    """
    try:
        # Replace NaN with empty string
        df = df.fillna('')
        
        # Replace infinity with empty string
        df = df.replace([np.inf, -np.inf], '')
        
        # Convert problematic dtypes
        for col in df.columns:
            if df[col].dtype == 'object':
                # Handle remaining NaN strings
                df[col] = df[col].astype(str).replace('nan', '').replace('inf', '').replace('-inf', '')
        
        return df
    except Exception as e:
        logger.warning(f"Error cleaning dataframe: {str(e)}; returning as-is")
        return df


def retry_with_backoff(func, max_retries=API_MAX_RETRIES, delay=API_RETRY_DELAY):
    """
    Retry a function with exponential backoff on API quota errors
    
    Args:
        func: Function to call
        max_retries: Maximum retry attempts
        delay: Initial delay between retries
        
    Returns:
        Function result
    """
    for attempt in range(max_retries):
        try:
            return func()
        except APIError as e:
            if '429' in str(e) and attempt < max_retries - 1:
                wait_time = delay * (2 ** attempt)
                logger.warning(f"Rate limit hit; retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise


class RBIDataFetcher:
    def __init__(self, service_account_json: str, spreadsheet_id: str):
        """
        Initialize the fetcher with Google Sheets credentials and target spreadsheet ID
        
        Args:
            service_account_json: Path or JSON string of Google Service Account credentials
            spreadsheet_id: Google Sheet ID to update
        """
        self.service_account_json = service_account_json
        self.spreadsheet_id = spreadsheet_id
        self.gsheet_client = None
        self.spreadsheet = None
        self.authenticate()
        
    def authenticate(self) -> None:
        """Authenticate with Google Sheets API using Service Account"""
        try:
            # Parse credentials - could be JSON string or file path
            if os.path.isfile(self.service_account_json):
                creds_dict = json.load(open(self.service_account_json))
            else:
                creds_dict = json.loads(self.service_account_json)
            
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            self.gsheet_client = gspread.authorize(creds)
            self.spreadsheet = self.gsheet_client.open_by_key(self.spreadsheet_id)
            logger.info(f"✓ Successfully authenticated with Google Sheets API")
        except Exception as e:
            logger.error(f"✗ Failed to authenticate with Google Sheets: {str(e)}")
            raise
    
    @staticmethod
    def download_excel() -> Optional[str]:
        """
        Download the RBI Excel file
        
        Returns:
            Path to downloaded file, or None if failed
        """
        try:
            logger.info(f"Downloading RBI Excel file from {RBI_EXCEL_URL}")
            response = requests.get(RBI_EXCEL_URL, timeout=30)
            response.raise_for_status()
            
            # Save to local file
            filename = f"rbi_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"✓ Successfully downloaded RBI Excel file: {filename}")
            return filename
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Failed to download RBI Excel file: {str(e)}")
            return None
    
    @staticmethod
    def parse_excel_sheets(excel_file: str, sheet_names_to_parse: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Parse sheets from the Excel file (optionally filtered by sheet names)
        
        Args:
            excel_file: Path to Excel file
            sheet_names_to_parse: Optional list of sheet names to parse. If None, parse all.
            
        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        try:
            logger.info(f"Parsing sheets from {excel_file}")
            excel_file_read = pd.ExcelFile(excel_file)
            sheets_data = {}
            
            # If no specific sheets requested, use all
            sheets_to_parse = sheet_names_to_parse if sheet_names_to_parse else excel_file_read.sheet_names
            logger.info(f"Reading {len(sheets_to_parse)} sheets (out of {len(excel_file_read.sheet_names)} total)")
            
            for sheet_name in sheets_to_parse:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                # Clean the dataframe immediately after loading
                df = clean_dataframe(df)
                sheets_data[sheet_name] = df
                logger.info(f"  ✓ Parsed sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
            
            logger.info(f"✓ Successfully parsed {len(sheets_data)} sheets from Excel file")
            return sheets_data
        except Exception as e:
            logger.error(f"✗ Failed to parse Excel sheets: {str(e)}")
            return {}
    
    def check_and_create_worksheet(self, sheet_name: str) -> Optional[gspread.Worksheet]:
        """
        Check if worksheet exists in Google Sheet; create if missing
        
        Args:
            sheet_name: Name of the worksheet to check/create
            
        Returns:
            Worksheet object, or None if failed
        """
        try:
            # Try to get existing worksheet
            try:
                def get_ws():
                    return self.spreadsheet.worksheet(sheet_name)
                worksheet = retry_with_backoff(get_ws)
                logger.info(f"✓ Found existing worksheet: '{sheet_name}'")
                return worksheet
            except WorksheetNotFound:
                # Worksheet doesn't exist, create new one
                logger.info(f"Creating new worksheet: '{sheet_name}'")
                def create_ws():
                    return self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)
                worksheet = retry_with_backoff(create_ws)
                logger.info(f"✓ Successfully created new worksheet: '{sheet_name}'")
                return worksheet
        except Exception as e:
            logger.error(f"✗ Failed to check/create worksheet '{sheet_name}': {str(e)}")
            return None
    
    def find_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Find the date column in the dataframe (heuristic approach)
        
        Args:
            df: DataFrame to search
            
        Returns:
            Column name if found, None otherwise
        """
        # First priority: exact keyword match
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if any(keyword in col_lower for keyword in ['date', 'day', 'time', 'dt', 'period']):
                logger.info(f"Found date column: '{col}'")
                return col
        
        # Second priority: if only one column, use it
        if len(df.columns) == 1:
            logger.warning(f"No explicit date column found; using single column '{df.columns[0]}' as identifier")
            return df.columns[0]
        
        # Third priority: first column as fallback
        if len(df.columns) > 0:
            logger.warning(f"No explicit date column found; using first column '{df.columns[0]}' as identifier")
            return df.columns[0]
        
        logger.error("No columns found in DataFrame")
        return None
    
    @staticmethod
    def _extract_month_year(sheet_name: str) -> Optional[Tuple[int, int]]:
        """
        Extract month and year from sheet name
        Handles formats like: "Jan-2020", "January 2020", "2020-01", "01/2020"
        
        Args:
            sheet_name: Sheet name to parse
            
        Returns:
            Tuple of (year, month) or None if cannot parse
        """
        try:
            sheet_lower = sheet_name.lower()
            
            # List of month names
            months = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12
            }
            
            # Try to find month name
            month = None
            for month_name, month_num in months.items():
                if month_name in sheet_lower:
                    month = month_num
                    break
            
            if month is None:
                return None
            
            # Try to find year (4 digits)
            year_match = re.search(r'(19\d{2}|20\d{2})', sheet_name)
            if not year_match:
                return None
            
            year = int(year_match.group(1))
            return (year, month)
        except Exception:
            return None
    
    @staticmethod
    def _should_process_sheet(sheet_name: str) -> bool:
        """
        Check if sheet should be processed (current month or future only)
        Skip past months to avoid reprocessing
        
        Args:
            sheet_name: Sheet name to check
            
        Returns:
            True if should process, False if past month (skip)
        """
        month_year = RBIDataFetcher._extract_month_year(sheet_name)
        if not month_year:
            # If cannot determine month, process it (be safe)
            return True
        
        year, month = month_year
        today = date.today()
        current_year = today.year
        current_month = today.month
        
        # Skip if month is in the past
        if year < current_year:
            logger.debug(f"Skipping past year sheet: {sheet_name} ({year})")
            return False
        
        if year == current_year and month < current_month:
            logger.debug(f"Skipping past month sheet: {sheet_name} ({month}/{year})")
            return False
        
        # Process current month and future months
        return True
    
    def update_gsheet_data(self, worksheet: gspread.Worksheet, df: pd.DataFrame) -> bool:
        """
        Update Google Sheet worksheet with data from DataFrame (INCREMENTAL - new rows only)
        Only appends rows not already in the worksheet
        
        Args:
            worksheet: gspread Worksheet object
            df: DataFrame with new data (already cleaned)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning(f"DataFrame is empty; skipping update for worksheet '{worksheet.title}'")
                return True
            
            # Add rate-limiting delay before each write
            time.sleep(RATE_LIMIT_DELAY)
            
            date_col = self.find_date_column(df)
            logger.info(f"Using date column for deduplication: '{date_col}'")
            
            # Get existing data from worksheet
            def get_all_values():
                return worksheet.get_all_values()
            existing_data = retry_with_backoff(get_all_values)
            
            logger.info(f"Worksheet '{worksheet.title}' contains {len(existing_data)} total rows (including header)")
            
            if not existing_data:
                # Worksheet is empty, write header and all data
                logger.info(f"Worksheet '{worksheet.title}' is empty; writing header and {len(df)} rows")
                self._write_to_worksheet_incremental(worksheet, df)
                logger.info(f"✓ Successfully initialized worksheet '{worksheet.title}' with {len(df)} rows")
                return True
            
            # Extract header and existing rows
            header = existing_data[0]
            existing_rows = existing_data[1:] if len(existing_data) > 1 else []
            
            logger.debug(f"Header row: {header[:5]}...")  # Log first 5 columns of header
            logger.info(f"Existing data has header + {len(existing_rows)} data rows")
            
            # Filter out rows that appear to be metadata or empty
            # Skip rows where first column is just text like "Sheet April 2026" or similar
            filtered_existing_rows = []
            skipped_metadata = 0
            skipped_empty = 0
            
            for row in existing_rows:
                if not row or all(not str(cell).strip() for cell in row):
                    # Skip completely empty rows
                    skipped_empty += 1
                    logger.debug(f"Skipping empty row")
                    continue
                
                # Check if first cell looks like metadata (contains "Sheet" or similar)
                first_cell_value = str(row[0]).strip().lower() if row else ""
                if any(keyword in first_cell_value for keyword in ['sheet', 'metadata', 'total', 'summary']):
                    skipped_metadata += 1
                    logger.debug(f"Skipping potential metadata row: {row[0]}")
                    continue
                
                filtered_existing_rows.append(row)
            
            existing_rows = filtered_existing_rows
            logger.info(f"After filtering: {len(existing_rows)} actual data rows (skipped {skipped_empty} empty + {skipped_metadata} metadata)")
            
            if not existing_rows:
                logger.warning(f"⚠ No valid data rows found after filtering (worksheet might only have header!)")
            
            if existing_rows and len(existing_rows) > 0:
                logger.debug(f"First existing data row (first 5 cells): {existing_rows[0][:5]}")
                logger.debug(f"Last existing data row (first 5 cells): {existing_rows[-1][:5]}")
            
            # Check if DataFrame columns match existing header (more lenient comparison)
            df_cols = [str(col).strip() for col in df.columns]
            header_normalized = [str(h).strip() for h in header]
            
            # Check for actual schema mismatch (not just whitespace/dtype differences)
            schema_mismatch = False
            
            if len(df_cols) != len(header_normalized):
                schema_mismatch = True
                logger.warning(f"Column count mismatch: DataFrame has {len(df_cols)} columns, worksheet has {len(header_normalized)}")
            elif df_cols != header_normalized:
                # Check if it's just a whitespace or format issue
                if any(col.lower() != hdr.lower() for col, hdr in zip(df_cols, header_normalized)):
                    schema_mismatch = True
                    logger.warning(f"Column names mismatch")
            
            if schema_mismatch:
                logger.warning(f"Column mismatch in '{worksheet.title}'")
                logger.warning(f"  DataFrame columns ({len(df_cols)}): {df_cols[:5]}...")
                logger.warning(f"  Worksheet columns ({len(header_normalized)}): {header_normalized[:5]}...")
                # Column structure changed - need to rebuild
                logger.info(f"Rebuilding worksheet '{worksheet.title}' with new schema")
                def clear_ws():
                    worksheet.clear()
                retry_with_backoff(clear_ws)
                self._write_to_worksheet_incremental(worksheet, df)
                logger.info(f"✓ Rebuilt worksheet '{worksheet.title}' with new schema")
                return True
            
            logger.info(f"Column structure matches; proceeding with OVERWRITE update")
            
            # CRITICAL: Reorder DataFrame columns to match worksheet column order
            # This ensures row-hash comparison works even if columns are in different order
            try:
                df_reordered = df[[col.strip() for col in header_normalized]].copy()
                logger.debug(f"DataFrame reordered to match worksheet column order")
            except KeyError as e:
                logger.error(f"Failed to reorder DataFrame columns to match header: {e}")
                logger.warning(f"DataFrame columns: {list(df.columns)}")
                logger.warning(f"Header columns: {header_normalized[:5]}...")
                # If reordering fails, use original DataFrame
                df_reordered = df.copy()
                logger.info(f"Proceeding with original column order")
            
            # Clear the entire worksheet and write fresh data (OVERWRITE mode)
            logger.info(f"Clearing existing data for worksheet '{worksheet.title}' to replace with updated month data")
            def clear_ws():
                worksheet.clear()
            retry_with_backoff(clear_ws)
            logger.info(f"✓ Cleared worksheet '{worksheet.title}'")
            
            # Write all data from scratch
            self._write_to_worksheet_incremental(worksheet, df_reordered)
            
            logger.info(f"✓ Successfully replaced data in worksheet '{worksheet.title}' with {len(df_reordered)} rows")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to update worksheet '{worksheet.title}': {str(e)}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _write_to_worksheet_incremental(self, worksheet: gspread.Worksheet, df: pd.DataFrame) -> None:
        """Helper function to write data to worksheet (initial write with header) with rate limiting"""
        try:
            # Prepare header
            header = [str(col) for col in df.columns]
            
            # Add rate-limiting delay before append
            time.sleep(RATE_LIMIT_DELAY)
            
            def append_header():
                worksheet.append_row(header)
            retry_with_backoff(append_header)
            
            # Prepare data rows
            data_rows = df.values.tolist()
            
            # Add rate-limiting delay before batch append
            time.sleep(RATE_LIMIT_DELAY)
            
            # Batch write for performance (limit rows per batch to avoid quota)
            batch_size = 100
            if data_rows:
                for i in range(0, len(data_rows), batch_size):
                    batch = data_rows[i:i+batch_size]
                    
                    def append_batch():
                        worksheet.append_rows(batch, value_input_option='USER_ENTERED')
                    
                    retry_with_backoff(append_batch)
                    
                    # Rate limiting between batches
                    if i + batch_size < len(data_rows):
                        time.sleep(RATE_LIMIT_DELAY * 2)
        except Exception as e:
            logger.error(f"Error initializing worksheet: {str(e)}")
            raise
    
    @staticmethod
    def _normalize_date_string(date_val: str) -> str:
        """
        Normalize a date string to a consistent format for comparison
        Handles formats like "2026-04-01 0:00", "2026-04-01 00:00:00", etc.
        
        Args:
            date_val: Date string to normalize
            
        Returns:
            Normalized date string (YYYY-MM-DD format)
        """
        try:
            # Remove extra whitespace
            date_val = str(date_val).strip()
            
            # Try to parse as datetime first
            try:
                # Try parsing with pandas (handles multiple formats)
                parsed_date = pd.to_datetime(date_val)
                # Return in normalized format: YYYY-MM-DD (just date, no time)
                return parsed_date.strftime('%Y-%m-%d')
            except:
                # If parsing fails, try to extract just the date part
                # Match pattern like "2026-04-01" at the start
                import re
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', date_val)
                if date_match:
                    return date_match.group(1)
                
                # If that fails too, return normalized string
                return date_val
        except Exception as e:
            logger.debug(f"Error normalizing date '{date_val}': {e}; returning as-is")
            return str(date_val).strip()
    
    @staticmethod
    def _get_row_hash(row: List) -> str:
        """
        Create a hash of a row for deduplication (more robust than date-only)
        
        Args:
            row: Row data as list
            
        Returns:
            Hash string of the row
        """
        import hashlib
        # Convert row to string and hash it
        row_str = '|'.join(str(cell).strip() for cell in row)
        return hashlib.md5(row_str.encode()).hexdigest()
    
    def _filter_new_rows(self, df: pd.DataFrame, existing_rows: List, header: List, date_col: str) -> pd.DataFrame:
        """
        Filter DataFrame to only include rows NOT already in the worksheet
        Uses first column as unique identifier (most stable approach)
        
        Args:
            df: New DataFrame (columns already reordered to match header)
            existing_rows: Existing rows from worksheet (filtered for metadata)
            header: Worksheet header
            date_col: Date column name (unused - keeping for compatibility)
            
        Returns:
            Filtered DataFrame with only new rows
        """
        if not existing_rows:
            logger.info("No existing rows in worksheet; all rows are new")
            return df
        
        try:
            # Use FIRST column as unique key (most reliable - doesn't depend on date formatting)
            first_col_name = df.columns[0] if len(df.columns) > 0 else None
            
            if not first_col_name:
                logger.warning("DataFrame has no columns; returning all rows")
                return df
            
            logger.info(f"Using '{first_col_name}' (first column) as unique identifier for deduplication")
            
            # Extract first column values from existing worksheet rows
            existing_first_col_values = set()
            sample_existing = []
            
            for row in existing_rows:
                if row and len(row) > 0:
                    first_cell = str(row[0]).strip()
                    if first_cell:
                        existing_first_col_values.add(first_cell)
                        if len(sample_existing) < 5:
                            sample_existing.append(first_cell)
            
            logger.info(f"✓ Found {len(existing_first_col_values)} unique values in first column of worksheet")
            logger.info(f"Sample existing first-column values: {sample_existing}")
            
            # Extract first column values from DataFrame
            df_first_col = df[first_col_name].astype(str).str.strip()
            sample_df = df_first_col.head(5).tolist()
            logger.info(f"Sample DataFrame first-column values: {sample_df}")
            
            # Compare - mark rows as new if first column value not in existing set
            new_rows_mask = ~df_first_col.isin(existing_first_col_values)
            new_count = new_rows_mask.sum()
            duplicate_count = len(df) - new_count
            
            logger.info(f"Comparison result: {new_count} new rows, {duplicate_count} duplicates out of {len(df)} total")
            
            # Safety check
            if new_count == len(df) and len(existing_first_col_values) > 0:
                logger.warning(f"⚠ All {len(df)} rows marked as NEW despite {len(existing_first_col_values)} existing identifiers!")
                logger.warning(f"Possible mismatch between DataFrame and worksheet first column")
                logger.warning(f"Sample existing: {sorted(list(existing_first_col_values))[:3]}")
                logger.warning(f"Sample DataFrame: {df_first_col.head(3).tolist()}")
            elif new_count == 0 and len(existing_first_col_values) > 0:
                logger.info(f"✓ All {len(df)} rows already exist in worksheet (no new data to add)")
            
            return df[new_rows_mask].copy()
            
        except Exception as e:
            logger.error(f"Error filtering new rows: {str(e)}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            logger.warning(f"Filtering failed - RETURNING ALL ROWS AS SAFETY FALLBACK")
            return df
    
    def _append_new_rows(self, worksheet: gspread.Worksheet, df: pd.DataFrame) -> None:
        """
        Append new rows to worksheet (only data rows, no header)
        
        Args:
            worksheet: gspread Worksheet object
            df: DataFrame with new rows to append
        """
        try:
            # Prepare data rows
            data_rows = df.values.tolist()
            
            # Add rate-limiting delay before append
            time.sleep(RATE_LIMIT_DELAY)
            
            logger.info(f"About to append {len(data_rows)} rows; sample row 1: {data_rows[0][:5] if data_rows else 'N/A'}...")
            
            # Batch write for performance
            batch_size = 100
            if data_rows:
                for i in range(0, len(data_rows), batch_size):
                    batch = data_rows[i:i+batch_size]
                    
                    def append_batch():
                        worksheet.append_rows(batch, value_input_option='USER_ENTERED')
                    
                    retry_with_backoff(append_batch)
                    
                    # Rate limiting between batches
                    if i + batch_size < len(data_rows):
                        time.sleep(RATE_LIMIT_DELAY * 2)
                        
                    logger.info(f"Appended batch {i//batch_size + 1} ({len(batch)} rows)")
            
            # DIAGNOSTIC: Get fresh data from worksheet to confirm what was written
            logger.info(f"Retrieving worksheet data to verify what was written...")
            time.sleep(1)  # Wait a moment for Google Sheets to update
            
            def get_all_values():
                return worksheet.get_all_values()
            
            fresh_data = retry_with_backoff(get_all_values)
            num_rows_after = len(fresh_data) - 1 if fresh_data else 0  # Exclude header
            logger.info(f"Worksheet now has {num_rows_after} data rows after append")
            
            if fresh_data and len(fresh_data) > 1:
                logger.debug(f"Sample of just-written data:")
                logger.debug(f"  Row from DataFrame: {data_rows[0][:5] if data_rows else 'N/A'}")
                logger.debug(f"  Row from worksheet: {fresh_data[-1][:5] if len(fresh_data) > 1 else 'N/A'}")
                
        except Exception as e:
            logger.error(f"Error appending new rows: {str(e)}")
            raise
    
    def _write_to_worksheet(self, worksheet: gspread.Worksheet, df: pd.DataFrame, existing_rows: List = None) -> None:
        """Helper function to write data to worksheet with rate limiting"""
        try:
            # Prepare header
            header = [str(col) for col in df.columns]
            
            # Add rate-limiting delay before append
            time.sleep(RATE_LIMIT_DELAY)
            
            def append_header():
                worksheet.append_row(header)
            retry_with_backoff(append_header)
            
            # Prepare data rows
            data_rows = df.values.tolist()
            
            # Add rate-limiting delay before batch append
            time.sleep(RATE_LIMIT_DELAY)
            
            # Batch write for performance (limit rows per batch to avoid quota)
            batch_size = 100
            if data_rows:
                for i in range(0, len(data_rows), batch_size):
                    batch = data_rows[i:i+batch_size]
                    
                    def append_batch():
                        worksheet.append_rows(batch, value_input_option='USER_ENTERED')
                    
                    retry_with_backoff(append_batch)
                    
                    # Rate limiting between batches
                    if i + batch_size < len(data_rows):
                        time.sleep(RATE_LIMIT_DELAY * 2)
        except Exception as e:
            logger.error(f"Error writing to worksheet: {str(e)}")
            raise
    
    def sync_data(self) -> bool:
        """
        Main sync function: download, parse, and update Google Sheet
        Only processes current and future month sheets; skips past months
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("=" * 60)
            logger.info(f"Starting RBI data sync at {datetime.now().isoformat()}")
            logger.info("=" * 60)
            
            # Step 1: Download Excel file
            excel_file = self.download_excel()
            if not excel_file:
                return False
            
            # Step 2: Get sheet names and filter to current/future months BEFORE parsing
            try:
                excel_file_read = pd.ExcelFile(excel_file)
                all_sheet_names = excel_file_read.sheet_names
                logger.info(f"Found {len(all_sheet_names)} total sheets in Excel file")
            except Exception as e:
                logger.error(f"✗ Failed to read Excel sheet names: {str(e)}")
                return False
            
            # Filter sheets: only current and future months
            sheets_to_process = []
            skipped_count = 0
            for sheet_name in all_sheet_names:
                if self._should_process_sheet(sheet_name):
                    sheets_to_process.append(sheet_name)
                else:
                    skipped_count += 1
                    logger.debug(f"⊘ Skipping past month sheet: '{sheet_name}'")
            
            logger.info(f"Processing {len(sheets_to_process)} sheets (skipped {skipped_count} past months)")
            
            if not sheets_to_process:
                logger.warning("No sheets to process (all are past months)")
                return True
            
            # Step 3: Parse only the filtered sheets
            sheets_data = self.parse_excel_sheets(excel_file, sheet_names_to_parse=sheets_to_process)
            if not sheets_data:
                return False
            
            # Step 4: Update Google Sheet with new worksheets and data
            success_count = 0
            total_sheets = len(sheets_data)
            
            for idx, (sheet_name, df) in enumerate(sheets_data.items(), 1):
                logger.info(f"Processing sheet {idx}/{total_sheets}: '{sheet_name}'")
                
                # Sanitize sheet name for Google Sheets (max 100 chars, no special chars)
                gsheet_name = self._sanitize_sheet_name(sheet_name)
                
                worksheet = self.check_and_create_worksheet(gsheet_name)
                if worksheet and self.update_gsheet_data(worksheet, df):
                    success_count += 1
                
                # Rate limit between sheets
                time.sleep(RATE_LIMIT_DELAY)
            
            logger.info(f"✓ Successfully updated {success_count}/{total_sheets} worksheets")
            
            # Cleanup: Remove downloaded file
            try:
                os.remove(excel_file)
                logger.info(f"Cleaned up downloaded file: {excel_file}")
            except Exception as e:
                logger.warning(f"Failed to cleanup file {excel_file}: {str(e)}")
            
            logger.info("=" * 60)
            logger.info(f"RBI data sync completed successfully at {datetime.now().isoformat()}")
            logger.info("=" * 60)
            return True
        except Exception as e:
            logger.error(f"✗ Sync failed: {str(e)}")
            return False
    
    @staticmethod
    def _sanitize_sheet_name(name: str) -> str:
        """
        Sanitize sheet name for Google Sheets compliance
        Max 100 characters, no leading/trailing spaces
        """
        # Remove leading/trailing spaces
        name = name.strip()
        
        # Truncate to 100 characters
        name = name[:100]
        
        # Replace problematic characters (Google Sheets doesn't allow some)
        # Keep alphanumeric, spaces, hyphens, underscores
        import re
        name = re.sub(r'[^\w\s\-]', '', name)
        
        return name if name else "Sheet1"


def main():
    """Main entry point"""
    try:
        # Get credentials and spreadsheet ID from environment variables
        service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')
        
        if not service_account_json or not spreadsheet_id:
            logger.error("Missing required environment variables:")
            logger.error("  - GOOGLE_SERVICE_ACCOUNT_JSON: Google Service Account credentials (JSON)")
            logger.error("  - GOOGLE_SPREADSHEET_ID: Target Google Sheet ID")
            return False
        
        # Create fetcher and sync
        fetcher = RBIDataFetcher(service_account_json, spreadsheet_id)
        success = fetcher.sync_data()
        
        return success
    except Exception as e:
        logger.error(f"✗ Fatal error: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
