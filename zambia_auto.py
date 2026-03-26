#!/usr/bin/env python3
"""
🇿🇲 ZAMBIA SMS DAILY AUTOMATION
================================
Full end-to-end automation of the daily Zambia SMS content update workflow.

Workflow:
  1. Download today's .xlsx from Dropbox shared folder
  2. Process Excel → Generate clean SQL queries
  3. SSH into remote server → Execute SQL on MySQL
  4. Update backup smsContentUpdate.sql on server
  5. Hit reload endpoint to refresh content cache

Usage:
  python3 zambia_auto.py              # Run full automation
  python3 zambia_auto.py --dry-run    # Preview queries without executing
  python3 zambia_auto.py --skip-download  # Skip Dropbox download (use existing file)
  python3 zambia_auto.py --date 05-03-2026  # Process a specific date's file

Author: Automated by Antigravity
"""

import os
import sys
import re
import json
import logging
import argparse
import requests
import pandas as pd
import paramiko
import smtplib
import ssl
import time
from datetime import datetime, time as dt_time
from pathlib import Path
from io import StringIO
from email.message import EmailMessage

# Try to load .env file
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
except ImportError:
    pass  # dotenv not installed, rely on system env vars

# ============================================================
# CONFIGURATION (from environment variables)
# ============================================================
class Config:
    """Centralized configuration loaded from .env file."""
    
    # Dropbox
    DROPBOX_SHARED_URL = os.getenv('DROPBOX_SHARED_URL', '')
    DROPBOX_ACCESS_TOKEN = os.getenv('DROPBOX_ACCESS_TOKEN', '')
    
    # SSH
    SSH_HOST = os.getenv('SSH_HOST', '41.1.4.4')
    SSH_PORT = int(os.getenv('SSH_PORT', '22'))
    SSH_USERNAME = os.getenv('SSH_USERNAME', 'muthu_ops')
    SSH_PASSWORD = os.getenv('SSH_PASSWORD', '')
    SSH_KEY_PATH = os.getenv('SSH_KEY_PATH', '')
    
    # MySQL
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'mobibattle.clgcbjwtmtg2.ap-south-1.rds.amazonaws.com')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
    MYSQL_USER = os.getenv('MYSQL_USER', 'mb')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'global')
    
    # Remote paths
    REMOTE_BACKUP_DIR = os.getenv('REMOTE_BACKUP_DIR', '/home/sshmobi/crontab/backup/sysScripts')
    REMOTE_BACKUP_FILE = os.getenv('REMOTE_BACKUP_FILE', 'smsContentUpdate.sql')
    
    # Reload
    RELOAD_URL = os.getenv('RELOAD_URL', 'http://41.1.2.7:9091/ContentServiceWrapper/reloadContentMap')
    
    # Local
    LOCAL_WORK_DIR = os.getenv('LOCAL_WORK_DIR', '/Users/adityakrishnav/Desktop/ZAMBIA_SMS')
    
    # Email Notifications
    EMAIL_SENDER = os.getenv('EMAIL_SENDER', '')
    EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')
    EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER', '')
    EMAIL_SMTP_SERVER = os.getenv('EMAIL_SMTP_SERVER', 'smtp.gmail.com')
    EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', '465'))
    
    # Automation Window
    RETRY_START_HOUR = 14
    RETRY_START_MIN = 30
    RETRY_END_HOUR = 16
    RETRY_END_MIN = 30
    RETRY_INTERVAL_SECONDS = 900 # 15 minutes
    
    # SQL Template
    SQL_TEMPLATE = "update sms_content set date=now(),content='{}' where service_id='{}';"
    
    # Month abbreviations (locale-proof)
    MONTH_ABBREV = {
        1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
        7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'
    }
    @classmethod
    def validate(cls, logger):
        """Check for critical missing configuration."""
        missing = []
        if not cls.DROPBOX_SHARED_URL and not cls.DROPBOX_ACCESS_TOKEN:
            missing.append("Dropbox (DROPBOX_SHARED_URL or DROPBOX_ACCESS_TOKEN)")
        if not cls.SSH_PASSWORD and not cls.SSH_KEY_PATH:
            missing.append("SSH (SSH_PASSWORD or SSH_KEY_PATH)")
        if not cls.MYSQL_PASSWORD:
            missing.append("MySQL (MYSQL_PASSWORD)")
            
        if missing:
            logger.error(f"❌ [CONFIG ERROR] Missing credentials for: {', '.join(missing)}")
            logger.error("   Please update your .env file with the required secrets.")
            return False
            
        # Optional: Email check
        if not cls.EMAIL_SENDER or not cls.EMAIL_PASSWORD or not cls.EMAIL_RECEIVER:
            logger.warning("⚠️  [CONFIG WARNING] Email credentials not fully set. Notifications will be disabled.")
            
        return True


# ============================================================
# NOTIFICATION SYSTEM
# ============================================================
class EmailNotifier:
    """Handles sending email alerts for success/failure."""
    
    @staticmethod
    def send(subject, body, logger, attachment_path=None):
        """Send an email alert to one or more recipients."""
        if not Config.EMAIL_SENDER or not Config.EMAIL_PASSWORD or not Config.EMAIL_RECEIVER:
            return False
            
        try:
            # Handle multiple recipients
            receivers = [r.strip() for r in Config.EMAIL_RECEIVER.split(',') if r.strip()]
            
            msg = EmailMessage()
            msg['Subject'] = f"🇿🇲 Zambia Automation: {subject}"
            msg['From'] = Config.EMAIL_SENDER
            msg['To'] = ", ".join(receivers)
            msg.set_content(body)
            
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, 'rb') as f:
                    file_data = f.read()
                    file_name = os.path.basename(attachment_path)
                msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)
            
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(Config.EMAIL_SMTP_SERVER, Config.EMAIL_SMTP_PORT, context=context) as server:
                server.login(Config.EMAIL_SENDER, Config.EMAIL_PASSWORD)
                server.send_message(msg)
            
            logger.info(f"📧 Email notification sent successfully to {len(receivers)} recipient(s)")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")
            return False


# ============================================================
# LOGGING SETUP
# ============================================================
def setup_logging(work_dir):
    """Configure logging to both console and file."""
    log_dir = os.path.join(work_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"zambia_auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Custom formatter with emojis for console
    class EmojiFormatter(logging.Formatter):
        FORMATS = {
            logging.DEBUG: "  🔍 %(message)s",
            logging.INFO: "  ✅ %(message)s",
            logging.WARNING: "  ⚠️  %(message)s",
            logging.ERROR: "  🔴 %(message)s",
            logging.CRITICAL: "  💀 %(message)s",
        }
        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno, "  %(message)s")
            formatter = logging.Formatter(log_fmt)
            return formatter.format(record)
    
    logger = logging.getLogger('zambia_auto')
    logger.setLevel(logging.DEBUG)
    
    # Console handler (emoji formatted)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(EmojiFormatter())
    logger.addHandler(ch)
    
    # File handler (detailed)
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(fh)
    
    return logger, log_file


# ============================================================
# STEP 1: DOWNLOAD FROM DROPBOX
# ============================================================
def download_from_dropbox(target_filename, work_dir, logger):
    """Download today's .xlsx file from Dropbox shared folder."""
    
    target_path = os.path.join(work_dir, target_filename)
    
    # Check if file already exists
    if os.path.exists(target_path):
        logger.info(f"File already exists: {target_filename} ({os.path.getsize(target_path)} bytes)")
        return target_path
    
    logger.info(f"Downloading {target_filename} from Dropbox...")
    
    # Method 1: Using Dropbox API (if token available)
    if Config.DROPBOX_ACCESS_TOKEN:
        return _download_via_api(target_filename, target_path, logger)
    
    # Method 2: Using shared folder link (direct download)
    if Config.DROPBOX_SHARED_URL:
        return _download_via_shared_link(target_filename, target_path, logger)
    
    logger.error(f"❌ [DROPBOX ERROR] File not found: '{target_filename}'")
    logger.error("   The daily Excel file is missing from the shared folder.")
    logger.error("   Please ensure it is uploaded with the correct date format.")
    raise FileNotFoundError(f"Dropbox file '{target_filename}' is missing")


def _download_via_shared_link(target_filename, target_path, logger):
    """Download file from Dropbox shared folder using the shared link."""
    
    # Dropbox shared folder links can be converted to direct download
    # by changing ?dl=0 to ?dl=1 or using the API
    shared_url = Config.DROPBOX_SHARED_URL
    
    # If the shared URL is a folder link, we need to construct the file URL
    # Dropbox shared folder: https://www.dropbox.com/scl/fo/FOLDER_ID/FOLDER_NAME
    # File in folder: append the filename to the shared link
    
    # Try the Dropbox API endpoint for shared links
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    }
    
    # For shared folder links, construct the file download URL
    # The Dropbox shared link API: https://content.dropboxapi.com/2/sharing/get_shared_link_file
    # But without an API token, we use the web download trick
    
    # Construct direct download URL
    # If the shared URL ends with dl=0, change to dl=1
    if 'dl=0' in shared_url:
        download_url = shared_url.replace('dl=0', 'dl=1')
    elif 'dl=1' not in shared_url:
        # Append dl=1 parameter
        separator = '&' if '?' in shared_url else '?'
        download_url = f"{shared_url}{separator}dl=1"
    else:
        download_url = shared_url
    
    # For folder links, we need to specify the file path
    # Using Dropbox's /sharing/get_shared_link_file API alternative
    # We'll try to download the specific file from the folder
    
    try:
        # Attempt 1: Direct file download from shared folder (concatenation)
        if '/fo/' in shared_url:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            parsed = urlparse(shared_url)
            params = parse_qs(parsed.query)
            params['dl'] = ['1']
            
            # Try appending filename to path
            path_parts = parsed.path.rstrip('/').split('/')
            path_parts.append(target_filename)
            new_path = '/'.join(path_parts)
            
            file_url = urlunparse((parsed.scheme, parsed.netloc, new_path, parsed.params, urlencode(params, doseq=True), parsed.fragment))
            logger.debug(f"Attempting direct file URL: {file_url}")
            response = requests.get(file_url, headers=headers, stream=True, timeout=60, allow_redirects=True)
            
            # If Attempt 1 failed (returned HTML), try Attempt 2: preview parameter
            if response.status_code != 200 or ('html' in response.headers.get('Content-Type', '').lower()):
                logger.debug("Direct file URL failed or returned HTML, trying 'preview' parameter...")
                params['preview'] = [target_filename]
                # Reconstruct original folder URL but with preview + dl=1
                file_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(params, doseq=True), parsed.fragment))
                response = requests.get(file_url, headers=headers, stream=True, timeout=60, allow_redirects=True)
        else:
            response = requests.get(download_url, headers=headers, stream=True, timeout=60, allow_redirects=True)
        
        # Check if we got the file or if we should try the ZIP Fallback
        content_type = response.headers.get('Content-Type', '').lower()
        is_html = 'html' in content_type and 'spreadsheet' not in content_type
        
        if response.status_code == 200 and not is_html:
            return _save_response_to_file(response, target_path, target_filename, logger)
        
        # ── ZIP FALLBACK ──
        # If we reach here, direct file download failed. Try downloading the whole folder as ZIP.
        logger.warning("Individual file download failed. Attempting to download folder as ZIP as fallback...")
        
        # Ensure dl=1 on the folder URL to force ZIP download
        if 'dl=1' not in download_url:
            separator = '&' if '?' in download_url else '?'
            zip_url = f"{download_url.replace('dl=0', '')}{separator}dl=1"
        else:
            zip_url = download_url
            
        zip_response = requests.get(zip_url, headers=headers, stream=True, timeout=120, allow_redirects=True)
        
        if zip_response.status_code == 200:
            import zipfile
            import tempfile
            from io import BytesIO
            
            logger.info("Folder ZIP downloaded. Searching for target file...")
            with zipfile.ZipFile(BytesIO(zip_response.content)) as z:
                # Find file in ZIP (might be in a subfolder)
                file_in_zip = None
                for name in z.namelist():
                    if name.endswith(target_filename):
                        file_in_zip = name
                        break
                
                if file_in_zip:
                    with open(target_path, 'wb') as f:
                        f.write(z.read(file_in_zip))
                    file_size = os.path.getsize(target_path)
                    logger.info(f"Successfully extracted {target_filename} from ZIP ({file_size:,} bytes)")
                    return target_path
                else:
                    available = [n for n in z.namelist() if n.endswith('.xlsx')]
                    logger.error(f"File '{target_filename}' not found inside the Dropbox ZIP.")
                    logger.error(f"Available .xlsx files in ZIP: {available}")
                    raise FileNotFoundError(f"'{target_filename}' not in ZIP")
        else:
            logger.error(f"❌ [DROPBOX ERROR] Failed to download ZIP (HTTP {zip_response.status_code})")
            raise RuntimeError(f"ZIP fallback failed (HTTP {zip_response.status_code})")
            
    except Exception as e:
        logger.error(f"❌ [DROPBOX ERROR] Process failed: {e}")
        raise

def _save_response_to_file(response, target_path, target_filename, logger):
    """Helper to save a successful requests response to disk."""
    with open(target_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    file_size = os.path.getsize(target_path)
    logger.info(f"Downloaded {target_filename} ({file_size:,} bytes)")
    return target_path


def _download_via_api(target_filename, target_path, logger):
    """Download file using Dropbox API with access token."""
    
    # List files in the shared folder to find today's file
    headers = {
        'Authorization': f'Bearer {Config.DROPBOX_ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    
    # If we have a shared link, use it to list folder contents
    if Config.DROPBOX_SHARED_URL:
        list_url = 'https://api.dropboxapi.com/2/files/list_folder'
        
        # First, get the shared folder metadata
        data = {
            'shared_link': {'url': Config.DROPBOX_SHARED_URL},
            'path': '',
        }
        
        response = requests.post(list_url, headers=headers, json=data, timeout=30)
        
        if response.status_code == 200:
            entries = response.json().get('entries', [])
            target_entry = None
            
            for entry in entries:
                if entry.get('name') == target_filename:
                    target_entry = entry
                    break
            
            if not target_entry:
                available = [e.get('name') for e in entries]
                logger.error(f"File '{target_filename}' not found in Dropbox folder.")
                logger.error(f"Available files: {available}")
                raise FileNotFoundError(f"'{target_filename}' not in Dropbox folder")
            
            # Download the specific file
            download_headers = {
                'Authorization': f'Bearer {Config.DROPBOX_ACCESS_TOKEN}',
                'Dropbox-API-Arg': json.dumps({
                    'url': Config.DROPBOX_SHARED_URL,
                    'path': f'/{target_filename}',
                }),
            }
            
            dl_response = requests.post(
                'https://content.dropboxapi.com/2/sharing/get_shared_link_file',
                headers=download_headers,
                timeout=60,
            )
            
            if dl_response.status_code == 200:
                with open(target_path, 'wb') as f:
                    f.write(dl_response.content)
                
                file_size = os.path.getsize(target_path)
                logger.info(f"Downloaded via API: {target_filename} ({file_size:,} bytes)")
                return target_path
            else:
                raise RuntimeError(f"API download failed: {dl_response.status_code} - {dl_response.text}")
        else:
            logger.error(f"❌ [DROPBOX ERROR] API listing failed (HTTP {response.status_code})")
            raise RuntimeError(f"API list_folder failed: {response.status_code} - {response.text}")
    else:
        raise RuntimeError("Need DROPBOX_SHARED_URL for API-based download")


# ============================================================
# STEP 2: PROCESS EXCEL → SQL QUERIES
# ============================================================
def get_dynamic_names(target_date=None):
    """Calculate expected filename and sheet names based on date."""
    
    if target_date:
        # Parse DD-MM-YYYY format
        today = datetime.strptime(target_date, '%d-%m-%Y')
    else:
        today = datetime.now()
    
    # Filename: DD-MM-YYYY.xlsx
    excel_filename = today.strftime('%d-%m-%Y.xlsx')
    
    # Sheet Names: DD_MMM_SMS1, DD_MMM_SMS2
    day_number = today.strftime('%d')
    month_abbr = Config.MONTH_ABBREV[today.month]
    day_month_str = f"{day_number}_{month_abbr}"
    
    target_sheets = [f"{day_month_str}_SMS1", f"{day_month_str}_SMS2"]
    
    return excel_filename, target_sheets, today


def clean_and_format_content(text):
    """Apply all required cleaning and sanitation rules."""
    if pd.isna(text):
        return ""
    text = str(text).strip()
    
    # Step 2: Escape single quotes (' → '')
    text = text.replace("'", "''")
    
    # Step 5: Remove double quotes
    text = text.replace('"', '')
    
    # Step 6: Handle special characters
    text = text.replace('–', ' ').replace('\u2013', ' ')   # en-dash → space
    text = text.replace('\u2014', ' ')                      # em-dash → space
    text = text.replace('-', ' ')                           # regular dash/hyphen → space
    text = text.replace('\xa0', ' ')                        # non-breaking space → regular space
    text = text.replace(':', '')                            # remove colons
    
    # Step 7: Collapse all whitespace into single spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def generate_sql_for_sheet(excel_file, sheet_name, logger):
    """Read a single sheet, apply cleaning, generate SQL queries."""
    queries = []
    
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        
        # Data starts at row 2 (index 1). Columns: B (index 1), C (index 2)
        df_data = df.iloc[1:][[1, 2]].copy()
        df_data.columns = ['Service_ID', 'Content']
        df_data.dropna(subset=['Service_ID', 'Content'], inplace=True)
        
        for _, row in df_data.iterrows():
            service_id = str(row['Service_ID']).strip()
            raw_content = row['Content']
            safe_content = clean_and_format_content(raw_content)
            query = Config.SQL_TEMPLATE.format(safe_content, service_id)
            queries.append(query)
        
        logger.info(f"Sheet '{sheet_name}': Generated {len(queries)} queries")
        
    except ValueError as e:
        if "Worksheet named" in str(e):
            raise KeyError(f"Worksheet named '{sheet_name}' not found.")
        raise e
    except Exception as e:
        logger.error(f"Error processing sheet '{sheet_name}': {e}")
        return []
    
    return queries


def process_excel(excel_path, target_sheets, logger):
    """Process Excel file and generate all SQL queries."""
    
    logger.info(f"Processing: {os.path.basename(excel_path)}")
    logger.info(f"Expected sheets: {target_sheets}")
    
    all_queries_sheet1 = []
    all_queries_sheet2 = []
    
    for i, sheet in enumerate(target_sheets):
        try:
            current_queries = generate_sql_for_sheet(excel_path, sheet, logger)
            
            if i == 0:
                all_queries_sheet1 = current_queries
            else:
                all_queries_sheet2 = current_queries
                
        except KeyError:
            logger.error(f"❌ [EXCEL ERROR] Worksheet '{sheet}' not found!")
            logger.error(f"   The Excel file was found, but it doesn't contain a sheet for today's date.")
            logger.error(f"   Expected: {sheet}")
            raise
        except Exception as e:
            logger.error(f"❌ [EXCEL ERROR] Failed to read Excel file.")
            logger.error(f"   Reason: {e}")
            raise
    
    # Save combined output for reference
    output_path = os.path.join(os.path.dirname(excel_path), 'daily_news_updates.sql')
    combined = list(all_queries_sheet1)
    if all_queries_sheet2:
        combined.append(f"\n\n-- QUERIES FROM BACKUP SHEET: {target_sheets[1]} --\n")
        combined.extend(all_queries_sheet2)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(combined))
    
    logger.info(f"Saved {len(combined)} total queries to {output_path}")
    
    return all_queries_sheet1, all_queries_sheet2


# ============================================================
# STEP 3: SSH + MYSQL EXECUTION
# ============================================================
def get_ssh_client(logger):
    """Establish SSH connection to remote server."""
    
    logger.info(f"Connecting to {Config.SSH_USERNAME}@{Config.SSH_HOST}:{Config.SSH_PORT}...")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    connect_kwargs = {
        'hostname': Config.SSH_HOST,
        'port': Config.SSH_PORT,
        'username': Config.SSH_USERNAME,
        'timeout': 30,
    }
    
    if Config.SSH_KEY_PATH and os.path.exists(Config.SSH_KEY_PATH):
        connect_kwargs['key_filename'] = Config.SSH_KEY_PATH
        logger.debug(f"Using SSH key: {Config.SSH_KEY_PATH}")
    elif Config.SSH_PASSWORD:
        connect_kwargs['password'] = Config.SSH_PASSWORD
        logger.debug("Using password authentication")
    else:
        # Try default SSH keys
        logger.debug("Trying default SSH key authentication")
    
    try:
        ssh.connect(**connect_kwargs)
        logger.info(f"SSH connection established to {Config.SSH_HOST}")
        return ssh
    except Exception as e:
        logger.error(f"❌ [SSH ERROR] Failed to connect to {Config.SSH_HOST}")
        logger.error(f"   Reason: {e}")
        logger.error("   Check if the server is up, the IP is correct, or if credentials changed.")
        raise


def execute_sql_queries(ssh, queries, logger):
    """Execute SQL queries on remote MySQL via SSH."""
    
    if not queries:
        logger.warning("No queries to execute!")
        return True
    
    logger.info(f"Executing {len(queries)} SQL queries on MySQL...")
    
    # Build the MySQL command with all queries
    # Combine all queries into a single MySQL session
    sql_block = f"USE {Config.MYSQL_DATABASE};\n"
    sql_block += '\n'.join(queries)
    
    # Construct MySQL command
    mysql_cmd = (
        f"mysql -u{Config.MYSQL_USER} "
        f"-p'{Config.MYSQL_PASSWORD}' "
        f"-h{Config.MYSQL_HOST} "
        f"--port={Config.MYSQL_PORT} "
        f"-e \"{sql_block.replace(chr(34), chr(92)+chr(34))}\""
    )
    
    # Alternative: pipe SQL via stdin (safer for special characters)
    # Use heredoc approach
    safe_sql = sql_block.replace('\\', '\\\\').replace('$', '\\$').replace('`', '\\`')
    
    pipe_cmd = (
        f"echo '{safe_sql}' | "
        f"mysql -u{Config.MYSQL_USER} "
        f"-p'{Config.MYSQL_PASSWORD}' "
        f"-h{Config.MYSQL_HOST} "
        f"--port={Config.MYSQL_PORT} "
        f"{Config.MYSQL_DATABASE}"
    )
    
    # Use a temp file approach (most reliable for complex SQL)
    temp_sql_cmd = f"cat << 'ZAMBIA_EOF' > /tmp/zambia_daily.sql\n{sql_block}\nZAMBIA_EOF"
    
    # Write temp SQL file
    stdin, stdout, stderr = ssh.exec_command(temp_sql_cmd, timeout=30)
    exit_status = stdout.channel.recv_exit_status()
    
    if exit_status != 0:
        err = stderr.read().decode('utf-8', errors='replace')
        logger.error(f"Failed to write temp SQL file: {err}")
        return False
    
    # Execute the temp SQL file
    exec_cmd = (
        f"mysql -u{Config.MYSQL_USER} "
        f"-p'{Config.MYSQL_PASSWORD}' "
        f"-h{Config.MYSQL_HOST} "
        f"--port={Config.MYSQL_PORT} "
        f"{Config.MYSQL_DATABASE} < /tmp/zambia_daily.sql"
    )
    
    stdin, stdout, stderr = ssh.exec_command(exec_cmd, timeout=60)
    exit_status = stdout.channel.recv_exit_status()
    
    stdout_text = stdout.read().decode('utf-8', errors='replace')
    stderr_text = stderr.read().decode('utf-8', errors='replace')
    
    # MySQL password warning is expected, filter it out
    stderr_lines = [
        line for line in stderr_text.split('\n')
        if line.strip() and 'Using a password on the command line' not in line
    ]
    
    if exit_status != 0 and stderr_lines:
        logger.error("❌ [MYSQL ERROR] Query execution failed on the remote server.")
        for line in stderr_lines:
            logger.error(f"   MySQL Error: {line}")
        logger.error("   Check database permissions, table names, or SQL syntax.")
        return False
    
    if stdout_text.strip():
        logger.debug(f"MySQL output: {stdout_text.strip()}")
    
    # Clean up temp file
    ssh.exec_command("rm -f /tmp/zambia_daily.sql")
    
    logger.info(f"Successfully executed {len(queries)} SQL queries ✓")
    return True


# ============================================================
# STEP 4: UPDATE BACKUP SQL FILE ON SERVER
# ============================================================
def update_backup_file(ssh, backup_queries, logger):
    """Update the smsContentUpdate.sql backup file on the remote server."""
    
    if not backup_queries:
        logger.warning("No backup queries (sheet 2) to write")
        return True
    
    backup_path = f"{Config.REMOTE_BACKUP_DIR}/{Config.REMOTE_BACKUP_FILE}"
    logger.info(f"Updating backup file: {backup_path}")
    
    # Build the backup SQL content
    backup_content = '\n'.join(backup_queries)
    
    # Write to the backup file using heredoc (preserves special chars)
    write_cmd = f"cat << 'ZAMBIA_BACKUP_EOF' > {backup_path}\n{backup_content}\nZAMBIA_BACKUP_EOF"
    
    stdin, stdout, stderr = ssh.exec_command(write_cmd, timeout=30)
    exit_status = stdout.channel.recv_exit_status()
    
    if exit_status != 0:
        err = stderr.read().decode('utf-8', errors='replace')
        logger.error(f"Failed to update backup file: {err}")
        return False
    
    # Verify the file was written
    stdin, stdout, stderr = ssh.exec_command(f"wc -l {backup_path}", timeout=10)
    line_count = stdout.read().decode('utf-8', errors='replace').strip()
    logger.info(f"Backup file updated: {line_count}")
    
    return True


# ============================================================
# STEP 5: RELOAD CONTENT MAP
# ============================================================
def reload_content_map(ssh, logger):
    """Hit the reload endpoint from the remote server."""
    
    logger.info(f"Reloading content map: {Config.RELOAD_URL}")
    
    curl_cmd = f'curl -s -o /dev/null -w "%{{http_code}}" "{Config.RELOAD_URL}"'
    
    stdin, stdout, stderr = ssh.exec_command(curl_cmd, timeout=30)
    exit_status = stdout.channel.recv_exit_status()
    
    http_code = stdout.read().decode('utf-8', errors='replace').strip()
    
    if http_code == '200':
        logger.info(f"Content map reloaded successfully (HTTP {http_code}) ✓")
        return True
    else:
        err = stderr.read().decode('utf-8', errors='replace')
        logger.error(f"Reload failed - HTTP {http_code}: {err}")
        return False


# ============================================================
# SAFETY TEST: CONNECTION VALIDATION
# ============================================================
def run_connection_test(ssh, logger):
    """Safely test all remote connections and paths without changing data."""
    logger.info("🧪 STARTING CONNECTION & PATH VALIDATION TEST")
    
    # 1. Test MySQL Connection
    mysql_test_cmd = (
        f"mysql -h{Config.MYSQL_HOST} -P{Config.MYSQL_PORT} "
        f"-u{Config.MYSQL_USER} -p'{Config.MYSQL_PASSWORD}' "
        f"-e 'SELECT 1 AS connection_test;' {Config.MYSQL_DATABASE}"
    )
    
    stdin, stdout, stderr = ssh.exec_command(mysql_test_cmd)
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().decode().strip()
    error = stderr.read().decode().strip()
    
    # Filter out the password warning from stderr
    error_lines = [
        line for line in error.split('\n')
        if line.strip() and 'Using a password on the command line' not in line
    ]

    if exit_status == 0:
        logger.info("  ✅ MySQL connection: SUCCESS")
    else:
        logger.error(f"  🔴 MySQL connection: FAILED (exit code {exit_status})")
        for line in error_lines:
            logger.error(f"    {line}")
        return False
        
    # 2. Test Remote Backup Directory
    logger.info(f"  🔍 Checking remote path: {Config.REMOTE_BACKUP_DIR}")
    stdin, stdout, stderr = ssh.exec_command(f"ls -ld {Config.REMOTE_BACKUP_DIR}")
    if stdout.channel.recv_exit_status() == 0:
        logger.info("  ✅ Remote backup directory: EXISTS")
    else:
        err = stderr.read().decode('utf-8', errors='replace')
        logger.error(f"  🔴 Remote backup directory: NOT FOUND or ACCESS DENIED\n    {err.strip()}")
        return False
        
    # 3. Test curl availability
    logger.info("  🔍 Checking remote 'curl' command availability")
    stdin, stdout, stderr = ssh.exec_command("which curl")
    if stdout.channel.recv_exit_status() == 0:
        logger.info("  ✅ Remote 'curl' command: AVAILABLE")
    else:
        err = stderr.read().decode('utf-8', errors='replace')
        logger.error(f"  🔴 Remote 'curl' command: NOT FOUND\n    {err.strip()}")
        return False
        
    logger.info("🎉 CONNECTION TEST PASSED! All credentials and paths are valid.")
    return True


# ============================================================
# MAIN ORCHESTRATION
# ============================================================
def main():
    """Main automation orchestrator."""
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='🇿🇲 Zambia SMS Daily Automation')
    parser.add_argument('--dry-run', action='store_true', help='Preview queries without executing on server')
    parser.add_argument('--skip-download', action='store_true', help='Skip Dropbox download, use existing file')
    parser.add_argument('--date', type=str, help='Process a specific date (DD-MM-YYYY format)')
    parser.add_argument('--skip-reload', action='store_true', help='Skip the content reload curl step')
    parser.add_argument('--test-connection', action='store_true', help='Test SSH/MySQL/Paths without making changes')
    args = parser.parse_args()
    
    # Setup
    work_dir = Config.LOCAL_WORK_DIR
    os.makedirs(work_dir, exist_ok=True)
    logger, log_file = setup_logging(work_dir)
    
    # Banner
    print("\n" + "=" * 60)
    print("  🇿🇲  ZAMBIA SMS DAILY AUTOMATION")
    print("=" * 60)
    
    target_date = args.date if args.date else None
    excel_filename, target_sheets, process_date = get_dynamic_names(target_date)
    
    logger.info(f"Date: {process_date.strftime('%Y-%m-%d (%A)')}")
    logger.info(f"Expected file: {excel_filename}")
    logger.info(f"Expected sheets: {target_sheets}")
    
    if args.dry_run:
        logger.warning("DRY RUN MODE - No changes will be made to the server")
    
    print("-" * 60)
    
    success = True
    ssh = None
    
    try:
        # Validate config first
        if not Config.validate(logger):
            return 1

        # ── STEP 1: Download from Dropbox (with Retry Loop) ──
        print("\n📥 STEP 1: Download from Dropbox")
        excel_path = os.path.join(work_dir, excel_filename)
        
        if args.skip_download:
            if os.path.exists(excel_path):
                logger.info(f"Skipping download, using existing: {excel_filename}")
            else:
                logger.error(f"File not found: {excel_path}")
                EmailNotifier.send("FAILED - Missing File", f"File not found locally: {excel_path}", logger)
                return 1
        else:
            # RETRY LOOP LOGIC
            max_retries = 10 
            retry_count = 0
            file_found = False
            
            while not file_found:
                try:
                    excel_path = download_from_dropbox(excel_filename, work_dir, logger)
                    file_found = True
                except Exception as e:
                    # Check if we are still within the 2:30 - 4:30 window
                    now = datetime.now()
                    window_start = now.replace(hour=Config.RETRY_START_HOUR, minute=Config.RETRY_START_MIN, second=0, microsecond=0)
                    window_end = now.replace(hour=Config.RETRY_END_HOUR, minute=Config.RETRY_END_MIN, second=0, microsecond=0)
                    
                    # If this is a specific date run or manual run outside window, don't retry
                    is_scheduled_run = window_start <= now <= window_end
                    
                    if is_scheduled_run and retry_count < max_retries:
                        retry_count += 1
                        logger.warning(f"File missing. Retrying in {Config.RETRY_INTERVAL_SECONDS/60} minutes... (Attempt {retry_count})")
                        time.sleep(Config.RETRY_INTERVAL_SECONDS)
                    else:
                        error_msg = f"❌ Download failed after {retry_count} retries or window closed: {e}"
                        logger.error(error_msg)
                        EmailNotifier.send("CRITICAL - Dropbox File Missing", 
                                         f"The automation failed because the file '{excel_filename}' was not found in Dropbox during the allowed window (2:30 PM - 4:30 PM).\n\nDetails: {e}", 
                                         logger)
                        return 1
        
        # ── Establish SSH connection early if needed for test or execution ──
        if args.test_connection or not args.dry_run:
            print("\n🔐 Establishing SSH connection...")
            try:
                ssh = get_ssh_client(logger)
            except Exception as e:
                logger.error(f"SSH connection failed: {e}")
                logger.error("Check SSH_HOST, SSH_USERNAME, and SSH_PASSWORD/SSH_KEY_PATH in .env")
                return 1

        # ── CONNECTION TEST ──
        if args.test_connection:
            if run_connection_test(ssh, logger):
                print("\n🎉 ALL TESTS PASSED! Connection is secure and paths are valid.")
                print("You can now safely run the live update.")
            else:
                print("\n🔴 CONNECTION TEST FAILED. Please check errors in log.")
            # SSH connection will be closed in finally block
            return 0 if success else 1 # Return based on test result
            
        # ── STEP 2: Process Excel → SQL ──
        print("\n🧹 STEP 2: Process Excel → Generate SQL")
        try:
            sheet1_queries, sheet2_queries = process_excel(excel_path, target_sheets, logger)
        except Exception as e:
            logger.error(f"Excel processing failed: {e}")
            return 1
        
        if not sheet1_queries:
            logger.error("No queries generated from Sheet 1!")
            return 1
        
        # Show preview
        print("\n  📋 Query Preview (Sheet 1):")
        for i, q in enumerate(sheet1_queries[:3], 1):
            preview = q[:120] + "..." if len(q) > 120 else q
            print(f"     {i}. {preview}")
        if len(sheet1_queries) > 3:
            print(f"     ... and {len(sheet1_queries) - 3} more")
        
        if sheet2_queries:
            print(f"\n  📋 Backup Sheet 2: {len(sheet2_queries)} queries")
        
        if args.dry_run:
            logger.info("Dry run mode: content processed but server updates skipped.")
        else:
            # ── STEP 3: SSH + Execute SQL ──
            print("\n🔐 STEP 3: Connect to Server & Execute SQL")
            try:
                ssh = get_ssh_client(logger)
            except Exception as e:
                logger.error(f"SSH connection failed: {e}")
                logger.error("Check SSH_HOST, SSH_USERNAME, and SSH_PASSWORD/SSH_KEY_PATH in .env")
                return 1
            
            # Execute Sheet 1 queries (main update)
            print("\n🗄️  STEP 3a: Execute Sheet 1 queries (main update)")
            if not execute_sql_queries(ssh, sheet1_queries, logger):
                logger.error("Sheet 1 SQL execution failed!")
                success = False
            
            # ── STEP 4: Update backup file on server ──
            # Note: Following the user's requirement, Sheet 2 is for backup ONLY.
            print("\n📝 STEP 4: Update backup SQL file on server")
            if sheet2_queries:
                if not update_backup_file(ssh, sheet2_queries, logger):
                    logger.warning("Backup file update failed (non-critical)")
            else:
                logger.info("No sheet 2 queries - using sheet 1 for backup")
                if not update_backup_file(ssh, sheet1_queries, logger):
                    logger.warning("Backup file update failed (non-critical)")
            
            # ── STEP 5: Reload content cache ──
            if not args.skip_reload:
                print("\n🔄 STEP 5: Reload content cache")
                if not reload_content_map(ssh, logger):
                    logger.error("❌ [RELOAD ERROR] The content map reload failed.")
                    logger.error(f"   The endpoint {Config.RELOAD_URL} did not return 200 OK.")
                    success = False
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        success = False
    except FileNotFoundError as e:
        logger.error(f"❌ Automation Halted: {e}")
        success = False
    except Exception as e:
        logger.error(f"❌ [CRITICAL FAILURE] {e}")
        import traceback
        logger.debug(traceback.format_exc())
        success = False
    finally:
        if ssh:
            ssh.close()
            logger.debug("SSH connection closed")
    
    # ── Summary & Notifications ──
    print("\n" + "=" * 60)
    if success:
        print("  🎉 ALL STEPS COMPLETED SUCCESSFULLY!")
        EmailNotifier.send("SUCCESS - Automation Complete", 
                          f"The Zambia SMS automation for {process_date.strftime('%Y-%m-%d')} completed successfully.\n\nLog file: {log_file}", 
                          logger)
    else:
        print("  ⚠️  COMPLETED WITH ERRORS - Check log for details")
        EmailNotifier.send("FAILED - Automation Errors", 
                          f"The Zambia SMS automation for {process_date.strftime('%Y-%m-%d')} failed or completed with errors.\n\nPlease check the attached log file for details.", 
                          logger, 
                          attachment_path=log_file)
    
    print(f"  📄 Log file: {log_file}")
    print(f"  📄 SQL output: {work_dir}/daily_news_updates.sql")
    print("=" * 60 + "\n")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
