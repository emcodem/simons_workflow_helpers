#!/usr/bin/env python3
"""
Add a field to a report JSON entry safely.
"""

import logging
import shlex
import sys
import json
import os
import argparse
import pprint
import time


# 1. Check the environment variable directly
bytecode_env = os.environ.get("PYTHONDONTWRITEBYTECODE")

# 2. Check Python's internal flag (derived from the env var)
internal_flag = sys.dont_write_bytecode

print(f"Environment Variable: {bytecode_env}")
print(f"Python Internal Flag: {internal_flag}")

# if internal_flag:
#     print("No bytecode (__pycache__) will be written, as expected.")
# else:
#     print("ERROR: Python is still set to write bytecode. This can prevent python scripts from running correctly on busy NAS.")


# Resolve custom libs folder if using --target
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# LIBS_DIR = os.path.join(BASE_DIR, "libs")
# sys.path.insert(0, LIBS_DIR)

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

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    global ffas_py_args
    ffas_py_args = ffas_py_args.decode('utf-8') if 'ffas_py_args' in globals() else False
    if ffas_py_args:
        # Parse from environment variable using shlex to handle quoted strings
        args_list = shlex.split(ffas_py_args)
        logging.info(f"Parsing arguments from ffas_py_args: {ffas_py_args}")
        logging.info(f"Arguments list: {args_list}")
    else:
        # Use command line arguments
        args_list = sys.argv[1:]
        logging.info(f"Parsing arguments from sys.argv: {args_list}")

    args = parser.parse_args(args_list)
    pprint.pprint(vars(args))

    
    report_path = args.report_json
    match_value = args.match_value
    value_to_add = args.value_to_add
    new_field_name = args.new_field_name
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

            report = [{new_field_name: added_value}]
            print(f"Created new report: {report_path}")
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
                print(f"No entry found matching value: {match_value}", file=sys.stderr)
                sys.exit(2)

        # Write updated report
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2,ensure_ascii=False)
            print(f"[OK] Report written: {report_path}")
        
        #print(f"------------------ Updated report: {report_path} ------------------")
        #print(json.dumps(report, indent=2, ensure_ascii=False))
    except Exception as e:
        logging.error(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
