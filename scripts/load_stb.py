# --- scripts/load_stb.py ---
import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
import traceback
from dotenv import load_dotenv

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
RAW_DIR = Path(__file__).resolve().parent.parent / "data/raw/stb_railcarloads_reports"
TABLE = "core_energy.fact_series_value"
META = "core_energy.fact_series_meta"

# Enable debug logging
DEBUG = True

def log(message, level="INFO"):
    """Log a message with a timestamp."""
    if DEBUG or level != "DEBUG":
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")

# ────────────────────── Helpers ──────────────────────────── #
def extract_metadata(path: Path):
    """Extract railroad and observation date from filename."""
    fname = path.stem
    parts = fname.split("_")
    if len(parts) < 2:
        log(f"Warning: Filename format unexpected for {path.name}", "WARNING")
        railroad = fname.lower()
        # Default to current date if no date in filename
        obs_date = datetime.now(timezone.utc)
    else:
        railroad = parts[0].lower()
        try:
            obs_date = datetime.strptime(parts[1], "%Y%m%d")
        except ValueError:
            log(f"Warning: Could not parse date from {parts[1]} in {path.name}", "WARNING")
            obs_date = datetime.now(timezone.utc)
    
    return railroad, obs_date

def safe_read_excel(path, **kwargs):
    """Safely read Excel files, handling errors."""
    try:
        return pd.read_excel(path, **kwargs)
    except Exception as e:
        log(f"Error reading Excel file: {e}", "ERROR")
        # Check if the error is about out-of-bounds indices
        if "out-of-bounds indices" in str(e):
            # Try to determine actual column range
            try:
                # Read first row to detect available columns
                preview = pd.read_excel(path, nrows=1)
                log(f"Available columns: {list(preview.columns)}", "DEBUG")
                
                # Adjust usecols if specified
                if 'usecols' in kwargs:
                    if isinstance(kwargs['usecols'], str):
                        # For ranges like 'A:E', try a more conservative range
                        start_col = kwargs['usecols'][0]
                        kwargs['usecols'] = f"{start_col}:{chr(ord(start_col) + min(preview.shape[1]-1, 4))}"
                    else:
                        # For numeric indices, limit to available columns
                        kwargs['usecols'] = [i for i in kwargs['usecols'] if i < preview.shape[1]]
                
                log(f"Retrying with adjusted usecols: {kwargs.get('usecols')}", "DEBUG")
                return pd.read_excel(path, **kwargs)
            except Exception as e2:
                log(f"Failed to retry with adjusted columns: {e2}", "ERROR")
                return pd.DataFrame()  # Return empty DataFrame
        else:
            return pd.DataFrame()  # Return empty DataFrame

def clean_value(value):
    """Clean and convert a value to float if possible."""
    if pd.isna(value):
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    try:
        val_str = str(value).replace(",", "").replace(" ", "").strip()
        if val_str in {"-", "", "N/A", "na", "NA"}:
            return None
        if not val_str.replace(".", "").replace("-", "").isdigit():
            return None
        return float(val_str)
    except (ValueError, TypeError):
        return None

# ────────────────────── Table Parsers ────────────────────── #
def parse_table_1(path, railroad, obs_date):
    """Parse table 1 (speed) data."""
    try:
        log(f"Parsing table 1 (speed) from {path.name}")
        df = safe_read_excel(path, skiprows=4, nrows=8, usecols="A:B", header=None)
        if df.empty:
            log("Table 1 empty or could not be parsed", "WARNING")
            return []
        
        records = []
        for _, row in df.iterrows():
            if pd.isna(row[0]): 
                continue
            
            value = clean_value(row[1])
            if value is None:
                continue
                
            commodity = str(row[0]).strip().lower().replace(" ", "_")
            sid = f"stb.speed.{railroad}.{commodity}"
            records.append((sid, obs_date, value))
        
        log(f"Extracted {len(records)} records from table 1")
        return records
    except Exception as e:
        log(f"Error parsing table 1: {e}", "ERROR")
        traceback.print_exc()
        return []

def parse_table_2(path, railroad, obs_date):
    """Parse table 2 (dwell) data."""
    try:
        log(f"Parsing table 2 (dwell) from {path.name}")
        df = safe_read_excel(path, skiprows=14, nrows=11, usecols="A:B", header=None)
        if df.empty:
            log("Table 2 empty or could not be parsed", "WARNING")
            return []
        
        records = []
        for _, row in df.iterrows():
            if pd.isna(row[0]):
                continue
                
            # Skip header rows
            if isinstance(row[0], str) and "terminal" in row[0].lower():
                continue
                
            value = clean_value(row[1])
            if value is None:
                continue
                
            terminal = str(row[0]).strip().lower().replace(",", "").replace(" ", "_")
            sid = f"stb.dwell.{railroad}.{terminal}"
            records.append((sid, obs_date, value))
        
        log(f"Extracted {len(records)} records from table 2")
        return records
    except Exception as e:
        log(f"Error parsing table 2: {e}", "ERROR")
        traceback.print_exc()
        return []

def parse_table_3(path, railroad, obs_date):
    """Parse table 3 (cars on line) data."""
    try:
        log(f"Parsing table 3 (cars on line) from {path.name}")
        df = safe_read_excel(path, skiprows=28, nrows=9, usecols="A:B", header=None)
        if df.empty:
            log("Table 3 empty or could not be parsed", "WARNING")
            return []
        
        records = []
        for _, row in df.iterrows():
            if pd.isna(row[0]):
                continue
                
            value = clean_value(row[1])
            if value is None:
                continue
                
            car_type = str(row[0]).strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
            sid = f"stb.carsonline.{railroad}.{car_type}"
            records.append((sid, obs_date, value))
        
        log(f"Extracted {len(records)} records from table 3")
        return records
    except Exception as e:
        log(f"Error parsing table 3: {e}", "ERROR")
        traceback.print_exc()
        return []

def parse_table_5(path, railroad, obs_date):
    """Parse table 5 (holding) data."""
    try:
        log(f"Parsing table 5 (holding) from {path.name}")
        
        # First, check how many columns are available
        sample = safe_read_excel(path, skiprows=48, nrows=1)
        if sample.empty:
            log("Could not read sample for table 5", "WARNING")
            return []
            
        num_cols = sample.shape[1]
        log(f"Found {num_cols} columns in table 5", "DEBUG")
        
        # Adjust usecols based on available columns
        if num_cols >= 5:
            usecols = "A:E"
        elif num_cols >= 3:
            usecols = "A:C"
        else:
            usecols = "A:B"
        
        df = safe_read_excel(path, skiprows=48, nrows=9, usecols=usecols, header=None)
        if df.empty:
            log("Table 5 empty or could not be parsed", "WARNING")
            return []
        
        records = []
        for _, row in df.iterrows():
            if pd.isna(row[0]):
                continue
                
            # Skip header rows
            if isinstance(row[0], str) and "train" in row[0].lower():
                continue
                
            train_type = str(row[0]).strip().lower().replace(" ", "_")
            
            # Process each cause column, if available
            causes = ["crew", "power", "other", "total"]
            for i, cause in enumerate(causes, start=1):
                if i >= len(row):
                    break  # Don't try to access columns that don't exist
                    
                value = clean_value(row[i])
                if value is None:
                    continue
                    
                if isinstance(row[i], str) and row[i].strip().lower() in causes + ["cause"]:
                    continue
                    
                sid = f"stb.holding.{railroad}.{train_type}.{cause}"
                records.append((sid, obs_date, value))
        
        log(f"Extracted {len(records)} records from table 5")
        return records
    except Exception as e:
        log(f"Error parsing table 5: {e}", "ERROR")
        traceback.print_exc()
        return []

def parse_table_6(path, railroad, obs_date):
    """Parse table 6 (stalled) data."""
    try:
        log(f"Parsing table 6 (stalled) from {path.name}")
        
        # First, check how many columns are available
        sample = safe_read_excel(path, skiprows=61, nrows=1)
        if sample.empty:
            log("Could not read sample for table 6", "WARNING")
            return []
            
        num_cols = sample.shape[1]
        log(f"Found {num_cols} columns in table 6", "DEBUG")
        
        # Adjust usecols based on available columns
        if num_cols >= 3:
            usecols = "A:C"
        else:
            usecols = "A:B"
        
        df = safe_read_excel(path, skiprows=61, nrows=9, usecols=usecols, header=None)
        if df.empty:
            log("Table 6 empty or could not be parsed", "WARNING")
            return []
        
        records = []
        for _, row in df.iterrows():
            if pd.isna(row[0]):
                continue
                
            group = str(row[0]).strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
            
            # Process loaded and empty columns if available
            tags = ["loaded", "empty"]
            for col, tag in enumerate(tags, start=1):
                if col >= len(row):
                    break  # Don't try to access columns that don't exist
                    
                value = clean_value(row[col])
                if value is None:
                    continue
                    
                sid = f"stb.stalled.{railroad}.{group}.{tag}"
                records.append((sid, obs_date, value))
        
        log(f"Extracted {len(records)} records from table 6")
        return records
    except Exception as e:
        log(f"Error parsing table 6: {e}", "ERROR")
        traceback.print_exc()
        return []

# ────────────────────── Upsert Logic ────────────────────── #
def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    """Insert or update a time series value in the database."""
    log(f"▶️ {series_code} | {obs_date} | {value}")
    
    try:
        cur.execute(f"""
            INSERT INTO {META} (series_code, source_id, description)
            VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='STB'), %s)
            ON CONFLICT (series_code) DO NOTHING;
        """, (series_code, series_code))
    except Exception as e:
        log(f"Error inserting metadata for {series_code}: {e}", "ERROR")
        raise

    try:
        cur.execute(f"SELECT series_id FROM {META} WHERE series_code=%s", (series_code,))
        result = cur.fetchone()
        if not result:
            log(f"Failed to find series_id for {series_code}", "ERROR")
            return
        series_id = result[0]
    except Exception as e:
        log(f"Error querying series_id for {series_code}: {e}", "ERROR")
        raise

    try:
        # Use timezone.utc instead of utcnow() (fixes deprecation warning)
        cur.execute(f"""
            INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
        """, (series_id, obs_date, value, datetime.now(timezone.utc)))
    except Exception as e:
        log(f"Error inserting value for {series_code}: {e}", "ERROR")
        raise

# ────────────────────── Main Entry ──────────────────────── #
def main():
    """Main entry point for STB data loading."""
    log("🚀 Starting STB data loading process")
    
    # Check if directory exists
    if not RAW_DIR.exists():
        log(f"⚠️ RAW_DIR does not exist: {RAW_DIR}", "ERROR")
        return
    
    files = sorted(RAW_DIR.glob("*.xlsx"))
    log(f"📥 Found {len(files)} STB Excel files")
    
    if not files:
        log("⚠️ No Excel files found in RAW_DIR", "WARNING")
        return
    
    # Process files
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        for f in files:
            try:
                railroad, obs_date = extract_metadata(f)
                log(f"→ Processing {f.name} → {railroad.upper()}, {obs_date.date()}")
                
                # Track success/failure for each table
                results = {}
                
                # Define parsers with names for better error reporting
                parsers = [
                    ("Table 1 (Speed)", parse_table_1),
                    ("Table 2 (Dwell)", parse_table_2),
                    ("Table 3 (Cars on Line)", parse_table_3),
                    ("Table 5 (Holding)", parse_table_5),
                    ("Table 6 (Stalled)", parse_table_6)
                ]
                
                # Process each table
                for table_name, parse_fn in parsers:
                    try:
                        records = parse_fn(f, railroad, obs_date)
                        
                        # Insert records with transaction
                        record_count = 0
                        for sid, ts, val in records:
                            try:
                                upsert_series(cur, sid, ts, val)
                                record_count += 1
                            except Exception as e:
                                log(f"Failed to insert record {sid}: {e}", "ERROR")
                                conn.rollback()
                                break
                        else:
                            # Only commit if all records were inserted successfully
                            conn.commit()
                        
                        results[table_name] = f"✓ {record_count} records"
                    except Exception as e:
                        log(f"Failed to process {table_name} in {f.name}: {e}", "ERROR")
                        traceback.print_exc()
                        results[table_name] = f"✗ {str(e)}"
                        conn.rollback()
                
                # Print summary for this file
                log(f"Summary for {f.name}:")
                for table, result in results.items():
                    log(f"  - {table}: {result}")
                
            except Exception as e:
                log(f"⚠️ Failed to process {f.name}: {e}", "ERROR")
                traceback.print_exc()
                conn.rollback()
    
    log("✓ STB loading complete")

if __name__ == "__main__":
    main()