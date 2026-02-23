import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
import ssl


ssl._create_default_https_context = ssl._create_unverified_context

def get_all_sheets_data(sheet_identifier, credentials_path):
    
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import google.auth.transport.requests
    import io
    import concurrent.futures

    if not credentials_path:
        raise ValueError("credentials_path is required to access private Google Sheets. Please provide the path to your service account JSON file.")

    sheet_id = sheet_identifier
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_identifier)
    if match:
        sheet_id = match.group(1)

    dfs = {}
    
    print("Authenticating with Google service account...")
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    
    # Authorized HTTP Session
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20))
    
    request = google.auth.transport.requests.Request(session)
    credentials.refresh(request)
    session.headers.update({'Authorization': f'Bearer {credentials.token}'})

    client = gspread.Client(auth=credentials, session=session)
    
    print(f"Fetching remote private sheet (ID: {sheet_id}) - Extracting ALL tabs...")
    try:
        sh = client.open_by_key(sheet_id)
    except Exception as e:
        raise RuntimeError(f"Could not open sheet. Ensure the Service Account Email is shared as a 'Viewer' on the Google Sheet. Error: {e}")
    
    worksheets = sh.worksheets()
    
    def fetch_worksheet(worksheet):
        print(f" Extracting tab: {worksheet.title}")
        # Direct CSV export is massively faster than get_all_values API calls
        export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={worksheet.id}"
        response = session.get(export_url)
        if response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.text), header=None, keep_default_na=False)
            return worksheet.title, df
        return worksheet.title, pd.DataFrame()

    # Rip through the tabs concurrently rather than waiting for each to finish
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_worksheet, worksheets)
        for title, df in results:
            dfs[title] = df
            
    return dfs

def clean_for_bq(df):
    import numpy as np
    
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(how='all')
    
    df.columns = [str(c) if isinstance(c, str) and not str(c).isdigit() else f'col_{c}' for c in df.columns]
    
    if 'col_1' in df.columns:
        df = df[df['col_1'].astype(str).str.lower() != 'int']
        
    for col in df.columns:
        if col != 'col_0': 
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df = df.reset_index(drop=True)
    return df

if __name__ == "__main__":
    SHEET_IDENTIFIER = "https://docs.google.com/spreadsheets/d/1p6pRyr0FJkzSd52oHMjCwMTJiXP1CHxc5xrBY88_3DY/edit?gid=0#gid=0"
    CREDENTIALS_JSON_PATH = "path/to/your/service_account.json"
    
    try:
        raw_dfs = get_all_sheets_data(
            sheet_identifier=SHEET_IDENTIFIER,
            credentials_path=CREDENTIALS_JSON_PATH
        )
        
        print("\n--- Raw Data Extracted Successfully ---")
        for tab_name, df in raw_dfs.items():
            print(f"  Tab '{tab_name}' shape: {df.shape}")
        
        cleaned_dfs = {}
        for tab_name, df in raw_dfs.items():
            if not df.empty:
                cleaned_dfs[tab_name] = clean_for_bq(df.copy())
            else:
                cleaned_dfs[tab_name] = pd.DataFrame()
        
        # Next Steps Example: Inserting into BigQuery
        # from google.cloud import bigquery
        # client = bigquery.Client()
        # dataset_id = "your-project.your_dataset"
        # job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # or WRITE_APPEND
        # 
        # for tab_name, c_df in cleaned_dfs.items():
        #     if not c_df.empty:
        #         table_name = re.sub(r'[^a-zA-Z0-9_]', '_', tab_name) # Sanitize table name
        #         table_id = f"{dataset_id}.{table_name}"
        #         print(f"Loading '{tab_name}' into '{table_id}'...")
        #         job = client.load_table_from_dataframe(c_df, table_id, job_config=job_config)
        #         job.result()  # Wait for the job to complete
        
    except Exception as e:
        print(f"\nError occurred: {e}")
