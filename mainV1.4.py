import pandas as pd
import json
import re
from datetime import datetime
import time
import math
from tqdm import tqdm
import os

DAY_MAP = {
    "Mon": "Monday",
    "Tue": "Tuesday",
    "Wed": "Wednesday",
    "Thu": "Thursday",
    "Fri": "Friday",
    "Sat": "Saturday",
    "Sun": "Sunday"
}
COUNTRY_CODE_MAP = {
    "CA": "+1",
    "CAN": "+1",
    "Canada": "+1",
    "US": "+1",
    "USA": "+1",
    "United States": "+1"
}

WEEK_DAYS = list(DAY_MAP.values())

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
    except ValueError:
        return None

def expand_days_range(start, end):
    keys = list(DAY_MAP.keys())
    try:
        start_idx = keys.index(start)
        end_idx = keys.index(end)
    except ValueError:
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
                    if day:
                        business_hours[day] = {
                            "day": day,
                            "is_closed": False,
                            "start_time": open_norm,
                            "end_time": close_norm
                        }
    
    return list(business_hours.values())

def transform_row(row_dict):
    name = row_dict.get("Name", "")
        # Get the most appropriate email
    email = row_dict.get("Most Common Email") or row_dict.get("Direct Emails") or None
    if email and isinstance(email, str) and "," in email:
        email = email.split(",")[0].strip()
    slug = name.lower().replace(" ", "-").replace("'", "")    
    
    return {
        "sic_code": row_dict.get("SIC Code") or None,
        "name": name,
        "google_registerd_bar_name": name,
        "description": row_dict.get("Description") or None,
        "slug": slug,
        "address": row_dict.get("Full Address", ""),
        "location": {
            "type": "Point",
            "coordinates": [
                float(row_dict["Establishment Longitude"]),
                float(row_dict["Establishment Latitude"])
            ] if row_dict.get("Establishment Longitude") and row_dict.get("Establishment Latitude") else None
        },
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


def convert_excel_to_csv(excel_path, csv_path):
    print(f"📄 Converting Excel file to CSV for efficient chunking...")
    df = pd.read_excel(excel_path, engine='openpyxl')
    df.to_csv(csv_path, index=False)
    print(f"✅ Conversion complete: {csv_path}")

def process_large_csv(csv_path, output_path, chunk_size=10000):
    print(f"🚀 Starting processing of CSV file (chunk_size={chunk_size})")
    start_time = time.time()

    row_count = sum(1 for _ in open(csv_path)) - 1  # subtract header
    print(f"ℹ️ Total rows to process: {row_count}")

    processed_rows = 0
    first_chunk = True
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('[')
        
        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size),
                         total=math.ceil(row_count / chunk_size),
                         desc="Processing chunks"):
            
            chunk_start = time.time()
            records = []

            for _, row in chunk.iterrows():
                row_dict = row.where(pd.notnull(row), None).to_dict()
                records.append(transform_row(row_dict))
                processed_rows += 1
            
            if not first_chunk:
                f.write(',\n')
            json.dump(records, f, ensure_ascii=False)
            first_chunk = False
            
        f.write(']')

    total_time = time.time() - start_time
    print(f"\n🏁 Processing complete!")
    print(f"📊 Processed {processed_rows} rows in {total_time:.2f} seconds — {processed_rows/total_time:.1f} rows/sec")

    return processed_rows

if __name__ == "__main__":
    input_excel = "/home/lp-55/Documents/Playground/angel-shot-python-data-migrate-script/target-excel-file-copy.xlsx"
    temp_csv = "converted_tempv6.csv"
    output_json = "large_outputv6.json"

    # 1. Convert Excel to CSV
    convert_excel_to_csv(input_excel, temp_csv)

    # 2. Process CSV in chunks and export to JSON
    process_large_csv(temp_csv, output_json, chunk_size=10000)
