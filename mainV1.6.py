import pandas as pd
import json
import re
import requests
from datetime import datetime
import time
import os
from tqdm import tqdm
import sys

# === Config ===
# Use relative paths instead of absolute paths for better portability
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_EXCEL = os.path.join(SCRIPT_DIR, "content-folder/target-excel-file-copy.xlsx")
TEMP_CSV = os.path.join(SCRIPT_DIR, "content-folder/converted_temp9.csv")
OUTPUT_JSON = os.path.join(SCRIPT_DIR, "content-folder/large_output9.json")
API_URL = "http://localhost:3001/api/v1/bar/addBar"
CHUNK_SIZE = 100  # reduced for testing

# === Mappings ===
DAY_MAP = {
    "Mon": "Monday", "Tue": "Tuesday", "Wed": "Wednesday",
    "Thu": "Thursday", "Fri": "Friday", "Sat": "Saturday", "Sun": "Sunday"
}
COUNTRY_CODE_MAP = {"CA": "+1", "CAN": "+1", "Canada": "+1", "US": "+1", "USA": "+1", "United States": "+1"}
WEEK_DAYS = list(DAY_MAP.values())

# === Utility Functions ===

def normalize_time(raw_time):
    if not raw_time or not isinstance(raw_time, str):
        return None
    raw_time = raw_time.strip().lower()
    if raw_time == "midnight":
        return "00:00"
    if raw_time == "noon":
        return "12:00"
    if ':' not in raw_time and ('am' in raw_time or 'pm' in raw_time):
        raw_time = raw_time.replace('am', ':00am').replace('pm', ':00pm')
    try:
        dt = datetime.strptime(raw_time, "%I:%M%p")
        return dt.strftime("%H:%M")
    except Exception as e:
        print(f"Warning: Could not parse time '{raw_time}': {str(e)}")
        return None

def expand_days_range(start, end):
    keys = list(DAY_MAP.keys())
    try:
        start_idx = keys.index(start)
        end_idx = keys.index(end)
    except Exception as e:
        print(f"Warning: Invalid day range {start}-{end}: {str(e)}")
        return []
    if start_idx <= end_idx:
        return [DAY_MAP[k] for k in keys[start_idx:end_idx+1]]
    return [DAY_MAP[k] for k in (keys[start_idx:] + keys[:end_idx+1])]

def convert_opening_hours_to_business_hours(hours_str):
    if not isinstance(hours_str, str):
        return [{"day": day, "is_closed": True} for day in WEEK_DAYS]
    business_hours = {day: {"day": day, "is_closed": True} for day in WEEK_DAYS}
    for segment in re.split(r",\s*", hours_str.strip()):
        match = re.match(r"([A-Za-z]{3})(?:-([A-Za-z]{3}))?\s+([^\s]+)-([^\s]+)", segment)
        if match:
            start_day, end_day, open_time, close_time = match.groups()
            days = expand_days_range(start_day, end_day) if end_day else [DAY_MAP.get(start_day)]
            open_norm = normalize_time(open_time)
            close_norm = normalize_time(close_time)
            if open_norm and close_norm:
                for day in days:
                    if day:  # Add check to ensure day is not None
                        business_hours[day] = {
                            "day": day, "is_closed": False,
                            "start_time": open_norm, "end_time": close_norm
                        }
    return list(business_hours.values())

def transform_row(row_dict):
    name = row_dict.get("Name", "")
    if not name:
        print("Warning: Row missing name, skipping")
        return None
        
    email = row_dict.get("Most Common Email") or row_dict.get("Direct Emails") or None
    if email and isinstance(email, str) and "," in email:
        email = email.split(",")[0].strip()
    slug = name.lower().replace(" ", "-").replace("'", "")
    
    # Check required coordinates
    lon = row_dict.get("Establishment Longitude")
    lat = row_dict.get("Establishment Latitude")
    if not lon or not lat:
        print(f"Warning: Missing coordinates for {name}, using null")
        location = None
    else:
        try:
            location = {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            }
        except (ValueError, TypeError) as e:
            print(f"Warning: Invalid coordinates for {name}: {e}")
            location = None
    
    return {
        "sic_code": row_dict.get("SIC Code") or None,
        "name": name,
        "google_registerd_bar_name": name,
        "description": row_dict.get("Description") or None,
        "slug": slug,
        "address": row_dict.get("Full Address", ""),
        "location": location,
        "images": [],
        "country_code": COUNTRY_CODE_MAP.get(str(row_dict.get("Country Code", "")).strip(), "+1"),
        "phone": row_dict.get("Phone", ""),
        "email": email,
        "website": row_dict.get("URL", ""),
        "business_hours": convert_opening_hours_to_business_hours(row_dict.get("Opening Hours", "")),
        "google_place_id": None,
        "google_reference": None,
        "available_in_angel_shot": False,
        "owner_id": None,
        "is_active": str(row_dict.get("Status", "")).lower() == "open",
        "is_deleted": False
    }

def post_bar(bar_object):
    try:
        print(f"Posting bar: {bar_object['name']}")
        response = requests.post(API_URL, json=bar_object, timeout=10)
        if response.status_code == 201:
            print(f"Success: {bar_object['name']} posted")
            return True
        else:
            print(f"⚠️ API Error: {response.status_code} - {response.text}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"⚠️ Connection error: Could not connect to API at {API_URL}")
        return False
    except Exception as e:
        print(f"⚠️ Request failed: {str(e)}")
        return False

# === Steps ===

def convert_excel_to_csv(input_path, output_csv_path):
    # Ensure content folder exists
    output_dir = os.path.dirname(output_csv_path)
    if not os.path.exists(output_dir):
        print(f"Creating directory: {output_dir}")
        os.makedirs(output_dir)
        
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Input Excel file not found: {input_path}")
        
    print(f"Reading Excel file: {input_path}")
    try:
        df = pd.read_excel(input_path, engine='openpyxl')
    except Exception as e:
        raise Exception(f"Failed to read Excel file: {str(e)}")
        
    print(f"✅ Excel file loaded: {len(df)} rows.")
    print(f"Columns: {df.columns.tolist()}")

    # Check if necessary columns exist
    required_columns = ['Name', 'Full Address']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if 'Country Code' in df.columns:
        before_count = len(df)
        df = df[~df['Country Code'].astype(str).str.strip().str.upper().eq('CA')]
        after_count = len(df)
        print(f"✅ {before_count - after_count} Canada rows removed. Remaining: {after_count} rows.")

    print(f"Writing CSV to: {output_csv_path}")
    df.to_csv(output_csv_path, index=False)
    
    # Verify the CSV was created
    if os.path.exists(output_csv_path):
        print(f"✅ CSV created at: {output_csv_path}")
    else:
        raise FileNotFoundError(f"❌ Failed to create CSV at: {output_csv_path}")

def process_csv_and_post(csv_path, output_json_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    # Count rows for progress reporting
    with open(csv_path, 'r') as f:
        row_count = sum(1 for _ in f) - 1  # Subtract header row
    print(f"ℹ️ Total CSV rows: {row_count}")

    successful_bars = []
    failed_bars = []
    
    api_accessible = False
    try:
        # Test the API connection before we start
        response = requests.get(API_URL.rsplit('/', 1)[0], timeout=5)
        if response.status_code < 500:  # Any response that's not a server error
            api_accessible = True
            print("✅ API appears to be accessible")
        else:
            print(f"⚠️ API returned status code {response.status_code}")
    except requests.exceptions.RequestException:
        print(f"⚠️ Could not connect to API at {API_URL}")
    
    if not api_accessible:
        print("⚠️ Continuing without API (will save to JSON only)")
    
    chunk_count = 0
    total_chunks = (row_count + CHUNK_SIZE - 1) // CHUNK_SIZE  # Ceiling division
    
    try:
        for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE):
            chunk_count += 1
            print(f"\nProcessing chunk {chunk_count}/{total_chunks}")
            
            for _, row in chunk.iterrows():
                row_dict = row.where(pd.notnull(row), None).to_dict()
                bar_object = transform_row(row_dict)
                
                if bar_object is None:
                    continue
                
                if api_accessible:
                    posted = post_bar(bar_object)
                    if posted:
                        successful_bars.append(bar_object)
                    else:
                        failed_bars.append(bar_object)
                else:
                    # Skip API posting but collect all objects
                    successful_bars.append(bar_object)
                    
            # Save progress after each chunk
            intermediate_json = output_json_path.replace(".json", f"_partial_{chunk_count}.json")
            with open(intermediate_json, 'w', encoding='utf-8') as f:
                json.dump(successful_bars, f, ensure_ascii=False, indent=2)
            print(f"✅ Intermediate JSON saved: {intermediate_json}")
    
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        # Save what we have so far
        if successful_bars:
            recovery_json = output_json_path.replace(".json", "_recovery.json")
            with open(recovery_json, 'w', encoding='utf-8') as f:
                json.dump(successful_bars, f, ensure_ascii=False, indent=2)
            print(f"✅ Recovery JSON saved: {recovery_json}")
        raise

    # Save final JSONs
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(successful_bars, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON saved: {output_json_path} ({len(successful_bars)} bars)")

    if failed_bars:
        failed_json_path = output_json_path.replace(".json", "_failed.json")
        with open(failed_json_path, 'w', encoding='utf-8') as f:
            json.dump(failed_bars, f, ensure_ascii=False, indent=2)
        print(f"⚠️ Failed bars saved: {failed_json_path} ({len(failed_bars)} bars)")

# === Main execution ===

if __name__ == "__main__":
    try:
        print("\n=== Step 1: Convert Excel to CSV ===")
        convert_excel_to_csv(INPUT_EXCEL, TEMP_CSV)

        print("\n=== Step 2: Process CSV and Post to API ===")
        process_csv_and_post(TEMP_CSV, OUTPUT_JSON)

        print("\n🏁 All Done Successfully!")
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)