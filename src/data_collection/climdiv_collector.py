# # --- src/data_collection/climdiv_collector.py ---
# import datetime as dt
# import requests
# import pandas as pd
# from pathlib import Path

# from ..utils import send_email

# USER_EMAIL = "jarviswilliamd@gmail.com"
# OUT_DIR = Path("data/raw/climdiv_hddcdd_reports")
# OUT_DIR.mkdir(parents=True, exist_ok=True)

# BASE_URL = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv"
# TS = dt.datetime.now().strftime("%Y%m%d")

# # Latest official versioned files (static until NOAA updates)
# FILES = {
#     "hdd": "climdiv-hddcdv-v1.0.0-20250404",
#     "cdd": "climdiv-cddcdv-v1.0.0-20250404",
# }

# MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
#           "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

# def parse_climdiv_file(content: str, variable: str) -> pd.DataFrame:
#     """Parse CLIMDIV fixed-width formatted files into a pandas DataFrame.
    
#     The CLIMDIV files use a fixed-width format where:
#     - First 6 characters: Division ID (state code + division number)
#     - Characters 7-10: Year
#     - Characters 11-80: 12 monthly values, each in a 6-character field
#     """
#     rows = []
#     for line in content.strip().splitlines():
#         if not line.strip() or len(line) < 80:
#             continue  # Skip empty or malformed lines
            
#         try:
#             # Fixed-width format parsing
#             division = line[0:6].strip()
#             year = int(line[6:10].strip())
            
#             # Extract the 12 monthly values (each in a 6-character field)
#             values = []
#             for i in range(12):
#                 start_pos = 10 + (i * 6)
#                 end_pos = start_pos + 6
#                 val_str = line[start_pos:end_pos].strip()
#                 # NOAA uses -9999 for missing values
#                 try:
#                     val = float(val_str) if val_str and val_str != '-9999' else float('nan')
#                     values.append(val)
#                 except ValueError:
#                     values.append(float('nan'))
            
#             row = {
#                 "division": division,
#                 "year": year,
#                 "variable": variable.upper()
#             }
            
#             for month, val in zip(MONTHS, values):
#                 row[month] = val
                
#             rows.append(row)
            
#         except Exception as e:
#             print(f"Warning: Could not parse line: '{line[:20]}...' Error: {e}")
#             continue
            
#     if not rows:
#         raise ValueError(f"No valid data rows found for {variable}")
        
#     return pd.DataFrame(rows)

# def fallback_parse_climdiv(file_path, variable):
#     """
#     Fallback method using pandas read_fwf for fixed-width files.
#     """
#     # Define column specifications
#     colspecs = [
#         (0, 6),    # division
#         (6, 10),   # year
#     ]
    
#     # Add specifications for the 12 monthly values
#     for i in range(12):
#         start_pos = 10 + (i * 6)
#         end_pos = start_pos + 6
#         colspecs.append((start_pos, end_pos))
    
#     # Column names
#     col_names = ["division", "year"] + MONTHS
    
#     # Read the file
#     df = pd.read_fwf(
#         file_path, 
#         colspecs=colspecs, 
#         header=None, 
#         names=col_names,
#         na_values=["-9999"]
#     )
    
#     # Add variable column
#     df["variable"] = variable.upper()
    
#     return df

# def collect():
#     print("📦 Collecting CLIMDIV HDD/CDD fixed-width files...")
#     successes = []
#     failures = []
#     tidy_dfs = []

#     for label, fname in FILES.items():
#         url = f"{BASE_URL}/{fname}"
#         raw_path = OUT_DIR / f"{label}_{TS}.txt"
        
#         try:
#             print(f"Downloading {url}...")
#             r = requests.get(url, timeout=30)
#             r.raise_for_status()
#             content = r.text

#             # Save raw file
#             raw_path.write_text(content)
#             print(f"✓ Saved raw {label.upper()} to {raw_path.name}")
#             successes.append(f"{label.upper()} download")
            
#             # Preview the first few lines to help with debugging
#             print(f"File preview (first 2 lines):")
#             preview_lines = content.strip().splitlines()[:2]
#             for i, line in enumerate(preview_lines):
#                 print(f"  Line {i+1} ({len(line)} chars): '{line[:80]}...'")
            
#             # Parse and normalize
#             try:
#                 df = parse_climdiv_file(content, label)
#                 print(f"✓ Parsed {label.upper()} data - {len(df)} rows")
                
#                 try:
#                     df_melted = df.melt(
#                         id_vars=["division", "year", "variable"],
#                         value_vars=MONTHS,
#                         var_name="month",
#                         value_name="value"
#                     )
#                     tidy_dfs.append(df_melted)
#                     print(f"✓ Melted {label.upper()} data - {len(df_melted)} rows")
#                     successes.append(f"{label.upper()} processing")
#                 except Exception as e:
#                     print(f"✗ Failed to melt {label.upper()} data: {e}")
#                     failures.append(f"{label.upper()} melting")
#             except Exception as e:
#                 print(f"✗ Failed to parse {label.upper()} data with primary method: {e}")
#                 print(f"Trying fallback parser...")
#                 try:
#                     df = fallback_parse_climdiv(raw_path, label)
#                     print(f"✓ Parsed {label.upper()} data with fallback method - {len(df)} rows")
                    
#                     try:
#                         df_melted = df.melt(
#                             id_vars=["division", "year", "variable"],
#                             value_vars=MONTHS,
#                             var_name="month",
#                             value_name="value"
#                         )
#                         tidy_dfs.append(df_melted)
#                         print(f"✓ Melted {label.upper()} data with fallback method - {len(df_melted)} rows")
#                         successes.append(f"{label.upper()} processing (fallback)")
#                     except Exception as e:
#                         print(f"✗ Failed to melt {label.upper()} data with fallback method: {e}")
#                         failures.append(f"{label.upper()} melting (fallback)")
#                 except Exception as e2:
#                     print(f"✗ Fallback parser also failed: {e2}")
#                     failures.append(f"{label.upper()} parsing")

#         except Exception as e:
#             print(f"✗ Failed to download {label.upper()}: {e}")
#             failures.append(f"{label.upper()} download")

#     if not tidy_dfs:
#         print("No data was successfully processed.")
#         send_email(
#             subject="CLIMDIV collector: Failed",
#             body="All parsing or processing failed. Raw files may be downloaded.",
#             to=USER_EMAIL
#         )
#         print("✓ email sent")
#         return

#     # Save combined data
#     combined_df = pd.concat(tidy_dfs, ignore_index=True)
#     tidy_path = OUT_DIR / f"climdiv_hddcdd_normalized_{TS}.csv"
#     combined_df.to_csv(tidy_path, index=False)
#     print(f"✓ Saved normalized file: {tidy_path.name}")

#     send_email(
#         subject="CLIMDIV collector: Success",
#         body=(
#             f"Downloaded: {', '.join(successes)}\n"
#             f"Failures: {', '.join(failures) if failures else 'None'}\n"
#             f"Saved normalized file: {tidy_path.name}"
#         ),
#         to=USER_EMAIL
#     )
#     print("✓ email sent")

# if __name__ == "__main__":
#     collect()