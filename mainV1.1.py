# Code version V1.1 reads the excel file and writes the rows as JSON objects to a file, where headers are keys. and change the opening hours to business hours
import pandas as pd
import json
import re
from datetime import datetime

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

def write_excel_as_json(file_path, output_path, sheet_name=0):
    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')

    json_rows = []
    for _, row in df.iterrows():
        row_dict = row.where(pd.notnull(row), "").to_dict()
        opening_hours = row_dict.pop("Opening Hours", "")

        serializable_dict = {
            k: str(v) if isinstance(v, (pd.Timestamp, pd.Timedelta)) else v
            for k, v in row_dict.items()
        }

        serializable_dict["business_hours"] = convert_opening_hours_to_business_hours(opening_hours)
        json_rows.append(serializable_dict)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_rows, f, indent=4, ensure_ascii=False)

    print(f"✅ JSON written to: {output_path}")

if __name__ == "__main__":
    excel_file = "/home/lp-55/Documents/Playground/angel-shot-python-data-migrate-script/target-excel-file-copy.xlsx"  # change this
    output_file = "output1.json"
    write_excel_as_json(excel_file, output_file)
