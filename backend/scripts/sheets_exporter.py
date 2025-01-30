import os
import logging
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and ranges of your spreadsheet
SPREADSHEET_ID = os.getenv('GOOGLE_SHEET_ID')
DATA_RANGE = 'data!A1:Z100'  # Range for both metrics and targets

def get_credentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    return creds

def get_sheet_data(service, spreadsheet_id, range_name):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        return result.get('values', [])
    except Exception as e:
        logger.error(f"Error reading sheet data: {str(e)}")
        return []

def process_sheet_data(data):
    """Process the raw sheet data into the required format."""
    # Extract headers
    headers = data[0] if data else []
    
    # Process data
    result = {}
    time_periods = ['All Time', 'Last 30 Days', 'Last 7 Days', 'Last 3 Days', 'Last 24 Hours']
    
    for period in time_periods:
        result[period] = {}
        period_row = None
        
        # Find the row for this time period
        for row in data[1:]:  # Skip header row
            if row and row[0] == period:
                period_row = row
                break
        
        if period_row:
            # Process metrics (column A) and targets (column B)
            for i, header in enumerate(headers):
                if i < len(period_row):
                    try:
                        # Get metric value from column A
                        value = int(period_row[i]) if period_row[i].strip() else 0
                        result[period][header] = value
                        
                        # Get target value from column B if it exists
                        if i + 1 < len(period_row):
                            try:
                                target_value = float(period_row[i + 1]) if period_row[i + 1].strip() else None
                                if target_value is not None:
                                    result[period][f"{header} Target"] = target_value
                            except (ValueError, IndexError):
                                pass
                    except (ValueError, AttributeError):
                        value = 0
                        result[period][header] = value

    return result

def export_to_sheets():
    """Export analytics data to Google Sheets."""
    try:
        logger.info("Starting analytics export")
        logger.info("Starting export to Google Sheets")

        creds = get_credentials()
        service = build('sheets', 'v4', credentials=creds)

        # Get data from the sheet
        data = get_sheet_data(service, SPREADSHEET_ID, DATA_RANGE)

        # Process the data
        processed_data = process_sheet_data(data)
        return processed_data

    except Exception as e:
        logger.error(f"Error in export_to_sheets: {str(e)}")
        raise