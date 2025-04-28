import pandas as pd
import json
import re
from datetime import datetime
import time  # Added for timing measurement

DAY_MAP = {
    "Mon": "Monday",
    "Tue": "Tuesday",
    "Wed": "Wednesday",
    "Thu": "Thursday",
    "Fri": "Friday",
    "Sat": "Saturday",
    "Sun": "Sunday"
}
WEEK_DAYS = list(DAY_MAP.values())

def normalize_time(raw_time):
    """Converts strings like '9pm', 'Midnight', '2:30am' to HH:MM (24h)"""
    raw_time = raw_time.strip().lower()
    if raw_time in ["midnight"]:
        return "00:00"
    if raw_time in ["noon"]:
        return "12:00"
    if re.match(r"^\d{1,2}(am|pm)$", raw_time):
        raw_time = raw_time[:-2] + ":00" + raw_time[-2:]
    try:
        return datetime.strptime(raw_time, "%I:%M%p").strftime("%H:%M")
    except Exception:
        return None

def expand_days_range(start, end):
    keys = list(DAY_MAP.keys())
    start_idx = keys.index(start)
    end_idx = keys.index(end)
    if start_idx <= end_idx:
        return [DAY_MAP[keys[i]] for i in range(start_idx, end_idx + 1)]
    else:
        return [DAY_MAP[keys[i % 7]] for i in range(start_idx, start_idx + 7) if i % 7 <= end_idx]

def convert_opening_hours_to_business_hours(hours_str):
    business_hours = {
        day: {"day": day, "is_closed": True, "start_time": None, "end_time": None}
        for day in WEEK_DAYS
    }

    if not isinstance(hours_str, str) or not hours_str.strip():
        return list(business_hours.values())

    segments = re.split(r",\s*", hours_str.strip())

    for segment in segments:
        # Handle "Daily"
        match_daily = re.match(r"Daily\s+([^\s]+)-([^\s]+)", segment, re.I)
        if match_daily:
            start_time = normalize_time(match_daily.group(1))
            end_time = normalize_time(match_daily.group(2))
            for day in WEEK_DAYS:
                business_hours[day] = {
                    "day": day,
                    "is_closed": False,
                    "start_time": start_time,
                    "end_time": end_time
                }
            continue

        # Handle "Mon-Thu 11am-9pm"
        match_range = re.match(r"([A-Za-z]{3})(?:-([A-Za-z]{3}))?\s+([^\s]+)-([^\s]+)", segment)
        if match_range:
            start_abbr = match_range.group(1)
            end_abbr = match_range.group(2)
            start_time = normalize_time(match_range.group(3))
            end_time = normalize_time(match_range.group(4))

            days = []
            if end_abbr:
                days = expand_days_range(start_abbr, end_abbr)
            else:
                days = [DAY_MAP.get(start_abbr)]

            for day in days:
                business_hours[day] = {
                    "day": day,
                    "is_closed": False,
                    "start_time": start_time,
                    "end_time": end_time
                }

    return list(business_hours.values())

def transform_to_object2_format(row_dict):
    """Transforms a row from Object 1 format to Object 2 format"""
    # Handle country code conversion
    country_code = "+1" if row_dict.get("Country Code") == "CA" else None
    
    # Handle status conversion
    is_active = row_dict.get("Status", "").lower() == "open"
    
    # Get the most appropriate email
    email = row_dict.get("Most Common Email") or row_dict.get("Direct Emails") or None
    if email and isinstance(email, str) and "," in email:
        email = email.split(",")[0].strip()
    
    # Prepare the location coordinates
    longitude = row_dict.get("Establishment Longitude")
    latitude = row_dict.get("Establishment Latitude")
    location = {
        "type": "Point",
        "coordinates": [longitude, latitude] if longitude and latitude else None
    }
    
    # Create slug from name
    name = row_dict.get("Name", "")
    slug = name.lower().replace(" ", "-").replace("'", "")
    
    # Create the Object 2 structure
    transformed = {
        "bar_id": "SYSTEM_GENERATED",  # You'll replace this with your system
        "sic_code": None,
        "name": name,
        "google_registerd_bar_name": name,  # Assuming same as name
        "description": None,
        "slug": slug,
        "address": row_dict.get("Full Address"),
        "location": location,
        "images": [],  # Can be populated from other sources if available
        "country_code": country_code,
        "phone": row_dict.get("Phone"),
        "email": email,
        "website": row_dict.get("URL"),
        "business_hours": convert_opening_hours_to_business_hours(row_dict.get("Opening Hours", "")),
        "google_place_id": None,
        "google_reference": None,
        "available_in_angel_shot": False,
        "owner_id": None,
        "is_active": is_active,
        "is_deleted": False,
    }
    
    return transformed

def write_excel_as_object2_json(file_path, output_path, sheet_name=0):
    # Start timing
    start_time = time.time()
    
    print("⏳ Reading Excel file...")
    read_start = time.time()
    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
    read_end = time.time()
    print(f"✅ Excel read completed in {read_end - read_start:.2f} seconds")
    
    print("⏳ Processing data...")
    process_start = time.time()
    json_rows = []
    for _, row in df.iterrows():
        row_dict = row.where(pd.notnull(row), "").to_dict()
        transformed = transform_to_object2_format(row_dict)
        json_rows.append(transformed)
    process_end = time.time()
    print(f"✅ Data processing completed in {process_end - process_start:.2f} seconds")
    print(f"ℹ️ Processed {len(json_rows)} rows")
    
    print("⏳ Writing JSON file...")
    write_start = time.time()
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_rows, f, indent=4, ensure_ascii=False)
    write_end = time.time()
    print(f"✅ JSON write completed in {write_end - write_start:.2f} seconds")
    
    total_time = time.time() - start_time
    print(f"\n🏁 Total execution time: {total_time:.2f} seconds")
    print(f"📊 Performance metrics:")
    print(f"- Excel reading: {(read_end - read_start)/total_time:.1%} of total time")
    print(f"- Data processing: {(process_end - process_start)/total_time:.1%} of total time")
    print(f"- JSON writing: {(write_end - write_start)/total_time:.1%} of total time")

if __name__ == "__main__":
    excel_file = "/home/lp-55/Documents/Playground/angel-shot-python-data-migrate-script/target-excel-file-copy.xlsx"  # change this
    output_file = "output_object2_format01.json"
    write_excel_as_object2_json(excel_file, output_file)