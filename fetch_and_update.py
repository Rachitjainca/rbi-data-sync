"""
RBI Excel Data Fetcher and Google Sheets Updater
Fetches RBI Excel file hourly and updates a single Google Sheet with new worksheets per month
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

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
    def parse_excel_sheets(excel_file: str) -> Dict[str, pd.DataFrame]:
        """
        Parse all sheets from the Excel file
        
        Args:
            excel_file: Path to Excel file
            
        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        try:
            logger.info(f"Parsing all sheets from {excel_file}")
            excel_file_read = pd.ExcelFile(excel_file)
            sheets_data = {}
            
            for sheet_name in excel_file_read.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
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
                worksheet = self.spreadsheet.worksheet(sheet_name)
                logger.info(f"✓ Found existing worksheet: '{sheet_name}'")
                return worksheet
            except WorksheetNotFound:
                # Worksheet doesn't exist, create new one
                logger.info(f"Creating new worksheet: '{sheet_name}'")
                worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=26)
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
        for col in df.columns:
            # Check if column name contains date-related keywords
            if any(keyword in str(col).lower() for keyword in ['date', 'day', 'time', 'dt', 'period']):
                return col
        
        # If no date column found, use first column as identifier
        if len(df.columns) > 0:
            logger.warning(f"No explicit date column found; using first column as identifier")
            return df.columns[0]
        
        return None
    
    def update_gsheet_data(self, worksheet: gspread.Worksheet, df: pd.DataFrame) -> bool:
        """
        Update Google Sheet worksheet with data from DataFrame
        Performs upsert based on date identifier (update if exists, insert if new)
        
        Args:
            worksheet: gspread Worksheet object
            df: DataFrame with new data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning(f"DataFrame is empty; skipping update for worksheet '{worksheet.title}'")
                return True
            
            date_col = self.find_date_column(df)
            
            # Get existing data from worksheet
            existing_data = worksheet.get_all_values()
            
            if not existing_data:
                # Worksheet is empty, write header and all data
                logger.info(f"Worksheet '{worksheet.title}' is empty; writing header and {len(df)} rows")
                self._write_to_worksheet(worksheet, df)
                return True
            
            # Extract header and existing rows
            header = existing_data[0]
            existing_rows = existing_data[1:] if len(existing_data) > 1 else []
            
            # Check if DataFrame columns match existing header
            df_cols = [str(col) for col in df.columns]
            if df_cols != header:
                logger.warning(f"Column mismatch in'{worksheet.title}'; updating header")
                # Clear and rewrite with new data
                worksheet.clear()
                self._write_to_worksheet(worksheet, df)
                return True
            
            # Perform upsert based on date identifier
            logger.info(f"Performing upsert for worksheet '{worksheet.title}' on column '{date_col}'")
            updated_rows = self._upsert_rows(df, existing_rows, header, date_col)
            
            # Clear and write all data back
            worksheet.clear()
            self._write_to_worksheet(worksheet, df, existing_rows=updated_rows)
            
            logger.info(f"✓ Successfully updated worksheet '{worksheet.title}'")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to update worksheet '{worksheet.title}': {str(e)}")
            return False
    
    @staticmethod
    def _write_to_worksheet(worksheet: gspread.Worksheet, df: pd.DataFrame, existing_rows: List = None) -> None:
        """Helper function to write data to worksheet"""
        # Prepare header
        header = [str(col) for col in df.columns]
        worksheet.append_row(header)
        
        # Prepare data rows
        data_rows = df.values.tolist()
        
        # Batch write for performance
        if data_rows:
            worksheet.append_rows(data_rows, value_input_option='USER_ENTERED')
    
    @staticmethod
    def _upsert_rows(df: pd.DataFrame, existing_rows: List, header: List, date_col: str) -> List:
        """
        Perform upsert: merge new data with existing rows based on date identifier
        
        Returns:
            List of updated rows
        """
        if not existing_rows or date_col not in header:
            return df.values.tolist()
        
        try:
            date_col_idx = header.index(date_col)
            existing_dict = {row[date_col_idx]: row for row in existing_rows if len(row) > date_col_idx}
            
            # Update with new data
            for idx, row in enumerate(df.values.tolist()):
                date_val = str(row[df.columns.tolist().index(date_col)]) if date_col in df.columns else None
                if date_val in existing_dict:
                    existing_dict[date_val] = row
                else:
                    existing_dict[date_val] = row
            
            return list(existing_dict.values())
        except Exception as e:
            logger.warning(f"Upsert failed: {str(e)}; returning new data only")
            return df.values.tolist()
    
    def sync_data(self) -> bool:
        """
        Main sync function: download, parse, and update Google Sheet
        
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
            
            # Step 2: Parse all sheets
            sheets_data = self.parse_excel_sheets(excel_file)
            if not sheets_data:
                return False
            
            # Step 3: Update Google Sheet with new worksheets and data
            success_count = 0
            for sheet_name, df in sheets_data.items():
                # Sanitize sheet name for Google Sheets (max 100 chars, no special chars)
                gsheet_name = self._sanitize_sheet_name(sheet_name)
                
                worksheet = self.check_and_create_worksheet(gsheet_name)
                if worksheet and self.update_gsheet_data(worksheet, df):
                    success_count += 1
            
            logger.info(f"✓ Successfully updated {success_count}/{len(sheets_data)} worksheets")
            
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
