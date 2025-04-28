# CODE version V1.0 reads the excel file and writes the rows as JSON objects to a file, where headers are keys.

import pandas as pd
import json


def write_excel_as_json(file_path, output_path, sheet_name=0):
    """
    Reads the specified Excel file (and sheet) and writes its rows as JSON objects
    to a file, where headers are keys.

    :param file_path: Path to the Excel file.
    :param output_path: Path where the output JSON file will be saved.
    :param sheet_name: Sheet name or index (default: first sheet).
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')

    json_rows = []
    for _, row in df.iterrows():
        row_dict = row.where(pd.notnull(row), "").to_dict()
        serializable_dict = {
            k: str(v) if isinstance(v, (pd.Timestamp, pd.Timedelta)) else v
            for k, v in row_dict.items()
        }
        json_rows.append(serializable_dict)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_rows, f, indent=4, ensure_ascii=False)

    print(f"JSON data written to {output_path}")


if __name__ == "__main__":
    excel_file = "/home/lp-55/Documents/Playground/angel-shot-python-data-migrate-script/target-excel-file-copy.xlsx"
    output_json = "./output.json"
    write_excel_as_json(excel_file, output_json)



