import pandas as pd
import os
import re
from datetime import datetime

# --- Configuration ---
OUTPUT_SQL_FILE = "daily_news_updates.sql"
SQL_TEMPLATE = "update sms_content set date=now(),content='{}' where service_id='{}';"
SCRIPT_DIR = "/Users/adityakrishnav/Desktop/ZAMBIA_SMS" # Your fixed working directory

# --- Dynamic Filename and Sheet Name Calculation (Locale-Proof) ---
def get_dynamic_names():
    """Calculates the expected filename and sheet names based on the current date."""
    
    today = datetime.now()
    
    # 1. Filename: DD-MM-YYYY.xlsx (e.g., '23-10-2025.xlsx')
    EXCEL_FILENAME = today.strftime('%d-%m-%Y.xlsx')
    
    # 2. Sheet Names: DD_MMM_SMS1, DD_MMM_SMS2 (e.g., '23_OCT_SMS1')
    MONTH_ABBREV = {
        1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
        7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'
    }
    
    day_number = today.strftime('%d')
    month_abbr = MONTH_ABBREV[today.month]
    
    day_month_str = f"{day_number}_{month_abbr}"
    
    TARGET_SHEETS = [f"{day_month_str}_SMS1", f"{day_month_str}_SMS2"]
    
    return EXCEL_FILENAME, TARGET_SHEETS

# --- Cleaning Rules (All Manual Steps 2, 5, 6, 7) ---
def clean_and_format_content(text):
    """Applies all required cleaning and sanitation rules."""
    if pd.isna(text): return ""
    text = str(text).strip()
    
    # Step 2: Escape single quotes (' to '')
    text = text.replace("'", "''") 
    
    # Step 5: Remove double quotes (")
    text = text.replace('"', '')
    
    # Step 6: Handle special characters (long dash, regular dash, non-breaking space, colon)
    text = text.replace('–', ' ').replace('\u2013', ' ')  # em-dash / en-dash → space
    text = text.replace('\u2014', ' ')                     # long em-dash → space
    text = text.replace('-', ' ')                          # regular dash/hyphen → space
    text = text.replace('\xa0', ' ')                       # non-breaking space → regular space
    text = text.replace(':', '')                           # remove colons
    
    # Step 7: Collapse newlines and excess spaces into single line
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def generate_sql_for_sheet(excel_file, sheet_name):
    """Reads a single sheet, applies cleaning, and generates single-line queries."""
    queries = []
    
    try:
        # FIX for Two-Row Header: Use header=None to read all rows
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        
        # Select data starting at row 2 (index 1). Columns: B (index 1), C (index 2)
        df_data = df.iloc[1:][[1, 2]].copy() 
        df_data.columns = ['Service_ID', 'Content']
        
        df_data.dropna(subset=['Service_ID', 'Content'], inplace=True)
        
        for index, row in df_data.iterrows():
            service_id = str(row['Service_ID']).strip()
            raw_content = row['Content']
            
            safe_content = clean_and_format_content(raw_content)
            query = SQL_TEMPLATE.format(safe_content, service_id)
            queries.append(query)
            
    except ValueError as e:
        if "Worksheet named" in str(e):
             raise KeyError(f"Worksheet named '{sheet_name}' not found.")
        raise e 
        
    except Exception as e:
        print(f"ERROR reading or processing data in sheet '{sheet_name}': {e}")
        return []

    return queries

if __name__ == "__main__":
    
    EXCEL_FILENAME, TARGET_SHEETS = get_dynamic_names()
    
    print(f"\n--- ZAMBIA SMS QUERY GENERATOR ---")
    print(f"Current Date: {datetime.now().strftime('%Y-%m-%d')}")
    print(f"Expecting file: {EXCEL_FILENAME}")
    print(f"Dynamically calculated sheets: {TARGET_SHEETS}")
    
    # Change to the working directory for file access
    os.chdir(SCRIPT_DIR)
    
    if not os.path.exists(EXCEL_FILENAME):
        print(f"🔴 FATAL ERROR: Expected file '{EXCEL_FILENAME}' not found in {SCRIPT_DIR}.")
        exit(1)
    
    all_queries = []
    
    for i, sheet in enumerate(TARGET_SHEETS):
        try:
            current_queries = generate_sql_for_sheet(EXCEL_FILENAME, sheet)
            
            if current_queries:
                if i > 0:
                    all_queries.append(f"\n\n-- QUERIES FROM BACKUP SHEET: {sheet} --\n")
                
                all_queries.extend(current_queries)
            
        except KeyError:
             print(f"   🔴 FATAL ERROR: Worksheet named '{sheet}' not found in the Excel file.")
             exit(1)
        except Exception as e:
            print(f"   🔴 FATAL ERROR during processing sheet {sheet}: {e}")
            exit(1)

    if all_queries:
        with open(OUTPUT_SQL_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(all_queries))
        
        print(f"\n✅ SUCCESS! Total {len(all_queries)} queries saved to {os.path.abspath(OUTPUT_SQL_FILE)}")
    else:
        print("\nProcess finished: No valid SQL queries were generated.")
