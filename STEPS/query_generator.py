import pandas as pd
import os
import re

# --- Configuration ---
# NOTE: UPDATE THIS TO MATCH YOUR DAILY DOWNLOADED FILE NAME
EXCEL_FILENAME = "zambia sms template.xlsx" 
OUTPUT_SQL_FILE = "daily_news_updates.sql"

# Sheets to process (main sheet first, then backup)
TARGET_SHEETS = ["21_DEC_SMS1", "21_DEC_SMS2"] 

# Template for the final update query (used for concatenation)
# The Service ID comes from Column B, the Content comes from Column C
SQL_TEMPLATE = "update sms_content set date=now(),content='{}' where service_id='{}';"

# --- Cleaning Rules (Based on Your Steps 2, 5, 6) ---
def clean_and_format_content(text):
    """
    Applies all required cleaning and sanitation rules to the content string.
    """
    if pd.isna(text):
        return ""
        
    text = str(text).strip()
    
    # Step 2: find and replace ' with null (i.e., escape or remove single quotes)
    # The safest approach for SQL is to ESCAPE it: ' becomes ''
    text = text.replace("'", "''") 
    
    # Step 5: find and replace " with null (i.e., remove double quotes)
    text = text.replace('"', '')
    
    # Step 6: find and replace the special characters – and - :
    # We will remove or replace these common long hyphens and non-breaking spaces
    text = text.replace('–', ' - ')  # Replace long hyphen with regular hyphen (with spaces)
    text = text.replace('\u2013', ' - ') # Another common dash
    text = text.replace('\xa0', ' ') # Non-breaking space
    text = text.replace(':', '') # Remove colons if they cause issues, though often they are safe
    
    # Remove newlines and extra spaces from copy-paste (critical for single line)
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def generate_sql_for_sheet(excel_file, sheet_name):
    """Reads a single sheet, applies cleaning, and generates single-line queries."""
    queries = []
    
    try:
        # Read the sheet, assuming header is on the second row (index 1)
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=1)
        
        # --- Handle Column Names ---
        # The Excel screenshot shows data in columns B (Service ID) and C (Content).
        # We assume the column at index 1 is Service ID and index 2 is Content.
        
        # Determine the column names dynamically, assuming the first few rows are consistent
        SERVICE_ID_COL = df.columns[1]  # Column B (index 1)
        CONTENT_COL = df.columns[2]     # Column C (index 2)
        
        # Step 1: Delete F column (The script skips reading/using it, effectively deleting it)
        
        # Filter out rows where essential data is missing
        df.dropna(subset=[SERVICE_ID_COL, CONTENT_COL], inplace=True)
        
        for index, row in df.iterrows():
            service_id = str(row[SERVICE_ID_COL]).strip()
            raw_content = row[CONTENT_COL]
            
            # Perform all cleaning (Steps 2, 5, 6)
            safe_content = clean_and_format_content(raw_content)
            
            # Step 3 & 7: Construct the final SQL UPDATE query in a single line
            query = SQL_TEMPLATE.format(safe_content, service_id)
            queries.append(query)
            
    except Exception as e:
        print(f"ERROR processing sheet '{sheet_name}': {e}")
        # If the sheet name is wrong or the file is corrupted, return empty list
        return []

    return queries

if __name__ == "__main__":
    
    print(f"\n--- ZAMBIA SMS QUERY GENERATOR ---")
    print(f"Reading file: {EXCEL_FILENAME} from {os.getcwd()}")
    
    all_queries = []
    
    # Process each sheet
    for i, sheet in enumerate(TARGET_SHEETS):
        print(f"Processing sheet: {sheet}...")
        current_queries = generate_sql_for_sheet(EXCEL_FILENAME, sheet)
        
        if current_queries:
            # Add a separator and sheet title for clarity in the output file
            if i > 0:
                all_queries.append(f"\n\n-- QUERIES FROM BACKUP SHEET: {sheet} --\n")
            
            all_queries.extend(current_queries)
            print(f"   Successfully generated {len(current_queries)} single-line queries.")
        else:
            print(f"   No queries generated for sheet {sheet}.")

    if all_queries:
        # Write all generated queries to the output file
        try:
            with open(OUTPUT_SQL_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(all_queries))
            
            print(f"\nSUCCESS! Total {len(all_queries)} queries saved to:")
            print(f"   {os.path.abspath(OUTPUT_SQL_FILE)}")
            print("\nNEXT STEP: Review, transfer, and execute 'daily_news_updates.sql' on the AWS server.")
        except Exception as e:
            print(f"FATAL ERROR writing output file: {e}")
    else:
        print("\nProcess finished: No valid SQL queries were generated.")
