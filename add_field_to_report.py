#!/usr/bin/env python3
"""
Add a field to a report JSON entry safely.
"""

import sys
import json
import os
import argparse
import time
import logging


# Set up logging
script_name = os.path.basename(__file__)
log_filename = f"c:\\temp\\{script_name}_{int(time.time() * 1000)}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Log to stdout
        logging.FileHandler(log_filename)   # Log to file
    ]
)
logging.info(f"Logging to file: {log_filename}")

# Resolve custom libs folder if using --target
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIBS_DIR = os.path.join(BASE_DIR, "libs")
sys.path.insert(0, LIBS_DIR)

def is_json_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            json.load(f)
        return True
    except Exception:
        return False

def load_file_value(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    try:
        return json.loads(content)
    except Exception:
        return content





def main():
    parser = argparse.ArgumentParser(description="Add a field to a report JSON entry safely.")
    parser.add_argument("--report_json", required=True, help="Path to the report JSON file.")
    parser.add_argument("--match_value", required=False, help="Value to identify the target entry (matches any field value).")
    parser.add_argument("--value_to_add", required=True, help="String or path to a file (file contents added; JSON parsed if possible).")
    parser.add_argument("--value_from_file", required=False, action='store_true', help="attach contents of --value_to_add")
    parser.add_argument("--new_field_name", required=True, help="Name of the field to add to the matched entry.")
    parser.add_argument("--create_report", required=False, action='store_true', help="Create a new report file if it doesn't exist.")
    parser.add_argument("--lock_timeout", type=int, default=30, help="Timeout in seconds to acquire the file lock.")
    args = parser.parse_args()

    report_path = args.report_json
    match_value = args.match_value
    value_to_add = args.value_to_add
    new_field_name = args.new_field_name
    lock_timeout = args.lock_timeout #lock timeout deprecated
    create_report = args.create_report

    added_value = value_to_add
    if (args.value_from_file):
            added_value = load_file_value(value_to_add)

    try:
        # Load or create report
        if create_report and not os.path.exists(report_path):
            # Create directories recursively if needed
            report_dir = os.path.dirname(os.path.abspath(report_path))
            if report_dir:
                os.makedirs(report_dir, exist_ok=True)
            # Create new report with initial entry

            report = [{new_field_name: match_value, new_field_name: added_value}]
            logging.info(f"Created new report: {report_path}")
        else:
            # Load existing report
            with open(report_path, 'r', encoding='utf-8-sig') as f:
                report = json.load(f)

        #print(f"------------------ Original report: {report_path} ------------------")
        #print(json.dumps(report, indent=2))

        # If we just created the report, skip the find-and-update logic
        if not (create_report and not os.path.exists(report_path)):
            found = False
            for entry in report:
                for k, v in entry.items():
                    v_str = str(v)
                    if os.path.exists(match_value):
                        if os.path.abspath(v_str) == os.path.abspath(match_value):
                            entry[new_field_name] = added_value
                            found = True
                            break
                    elif v_str == match_value:
                        entry[new_field_name] = added_value
                        found = True
                        break
                if found:
                    break

            if not found:
                logging.error(f"No entry found matching value: {match_value}")
                sys.exit(2)

        # Write updated report
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
            logging.info(f"[OK] Report written: {report_path}")
        
        #print(f"------------------ Updated report: {report_path} ------------------")
        #print(json.dumps(report, indent=2))
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
