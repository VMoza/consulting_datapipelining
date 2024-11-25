import os
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = None
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'client_secret_file.json', SCOPES)
        creds = flow.run_local_server(port=8080)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)

EXCLUDED_STATUS = ['BOUNCED', 'ERROR', '0', 'RESPONDI', 'NO_RECIPIENT', 'UNSUBSCRIBED', 'UNINTERESTED']

# Define mappings for potential variations in header names
HEADER_MAP = {
    "name": "NAME",
    "first": "FIRST",
    "position": "POSITION",
    "role" : "POSITION",
    "email address": "EMAIL",
    "email": "EMAIL",
    "e-mail": "EMAIL",
    "company" : "COMPANY",
    "company name": "COMPANY",
    "merge status" : "Merge status"
}
def fetch_data_from_sheet(sheet_id, range_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])
    
    if not values:
        return []
    
    headers = [header.strip().lower() for header in values[0]]

    return [dict(zip(headers, row)) for row in values[1:]]

def merge_and_filter(data_list):
    merged_data = []
    excluded_data = []  # For data with excluded merge status
    emails = set()

    for data in data_list:
        email = data.get('EMAIL')
        merge_status = data.get('Merge status')

        if email not in emails:
            if merge_status in EXCLUDED_STATUS:
                excluded_data.append(data)
                continue
            emails.add(email)
            merged_data.append(data)

    return sorted(merged_data, key=lambda x: (x.get('Company') is None, x.get('Company'))), excluded_data

def standardize_headers(data):
    """Standardize headers based on mapping and handle missing fields."""
    for record in data:
        standardized_record = {}
        for old_key, new_key in HEADER_MAP.items():
            if old_key in record:
                # Check if we're about to overwrite an existing field.
                # If so, merge the values.
                if new_key in standardized_record:
                    standardized_record[new_key] = f"{standardized_record[new_key]} {record[old_key]}"
                else:
                    standardized_record[new_key] = record[old_key]

        # Fill missing keys with default values
        for key in HEADER_MAP.values():
            standardized_record.setdefault(key, None)

        # Replace the original record with the standardized one
        record.clear()
        record.update(standardized_record)

    return data

def fetch_data_from_all_sheets(sheet_id):
    sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_titles = [sheet['properties']['title'] for sheet in sheet_metadata['sheets']]
    
    all_data = []
    for title in sheet_titles[2:]:  # Skip the first two sheets
        range_name = f"{title}!A:Z"
        sheet_data = fetch_data_from_sheet(sheet_id, range_name)
        
        # Standardize headers and handle missing ones
        standardized_data = standardize_headers(sheet_data)
        all_data.extend(standardized_data)
        
    return all_data

def filter_and_reorder_columns(data):
    """Filter out unwanted columns and reorder them."""
    desired_columns = ["NAME", "FIRST", "EMAIL", "COMPANY", "POSITION"]
    reordered_data = []
    
    for record in data:
        new_record = {col: record.get(col, "") for col in desired_columns}
        reordered_data.append(new_record)

    return reordered_data

def write_to_sheet(data, sheet_id, range_name):
    if not data:
        print("Warning: Trying to write an empty dataset. Skipping...")
        return
    headers = list(data[0].keys())
    rows = [[record.get(key, "") for key in headers] for record in data]
    sheet = service.spreadsheets()
    sheet.values().update(spreadsheetId=sheet_id, range=range_name, valueInputOption='RAW', body={"values": [headers] + rows}).execute()

def get_emails_from_data(data):
    """Extract emails from the provided data."""
    return {record['EMAIL'] for record in data if 'EMAIL' in record}

if __name__ == "__main__":
    # Fetching data from the source sheets
    data_from_source_sheet = fetch_data_from_all_sheets("1sOOXMQQd8denQQ7lrKv8tdBhT_adsBmXW4uCgQXghAI")
    
    print(f"Initial data size from source: {len(data_from_source_sheet)}")  # Debugging line

    final_data_to_write = []
    excluded_data = []

    for record in data_from_source_sheet:
        email = record.get('EMAIL')
        merge_status = record.get('Merge status')

        if not email:
            continue

        # If email has a bad status, exclude it
        if merge_status in EXCLUDED_STATUS:
            excluded_data.append(record)
        else:
            final_data_to_write.append(record)

    # Fetch existing data from 'Excluded' Sheet
    # Fetch data and standardize headers using HEADER_MAP
    excluded_data_from_sheet = fetch_data_from_sheet("1sWAtVHbSHA-BBfVywbPuPNQWbmPTT-JcikzMhkeBjrg", "Excluded!A:Z")
    standardized_excluded_data = [{HEADER_MAP.get(k.strip().lower(), k): v for k, v in record.items()} for record in excluded_data_from_sheet]
    
    print(f"Excluded data size from sheet: {len(standardized_excluded_data)}")  # Debugging line

    # Create set of existing excluded emails, assuming standardized header is 'EMAIL'
    existing_excluded_emails = {record.get('EMAIL') for record in standardized_excluded_data if 'EMAIL' in record}

    # Extracting the new excluded emails
    new_excluded_emails = {record['EMAIL'] for record in excluded_data}
    
    # Combine and deduplicate the new excluded emails with the existing ones
    all_excluded_emails = existing_excluded_emails.union(new_excluded_emails)

    # Capture the count before removal for comparison
    initial_count = len(final_data_to_write)

    # Ensure 'Master' does not contain excluded emails
    final_data_to_write = [data for data in final_data_to_write if data['EMAIL'] not in all_excluded_emails]

    # Calculate the difference in count and print it
    removed_count = initial_count - len(final_data_to_write)
    print(f"Number of emails retroactively removed from Master: {removed_count}")

    print(f"Data to be written to Master Sheet: {len(final_data_to_write)} records")  # To show the total number of records

    # Writing to Master Sheet
    write_to_sheet(final_data_to_write, "1ZOKirGGwzL1ku8VzcF7JZmyxtw5TvBzFkIbuk8ahBSs", "Master!A:Z")

    # Writing combined excluded emails to the 'Excluded' sheet (convert set back to the desired list format)
    combined_excluded_data = [{"EMAIL": email} for email in all_excluded_emails]
    write_to_sheet(combined_excluded_data, "1sWAtVHbSHA-BBfVywbPuPNQWbmPTT-JcikzMhkeBjrg", "Excluded!A:A")