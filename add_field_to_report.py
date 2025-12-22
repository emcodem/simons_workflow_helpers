#!/usr/bin/env python3
"""
Add a field to a report JSON entry safely using SoftFileLock,
with stale lock detection (force remove if older than 10 seconds).
"""

import sys
import json
import os
import argparse
import time


# 1. Check the environment variable directly
bytecode_env = os.environ.get("PYTHONDONTWRITEBYTECODE")

# 2. Check Python's internal flag (derived from the env var)
internal_flag = sys.dont_write_bytecode

print(f"Environment Variable: {bytecode_env}")
print(f"Python Internal Flag: {internal_flag}")

if internal_flag:
    print("No bytecode (__pycache__) will be written, as expected.")
else:
    print("ERROR: Python is still set to write bytecode. This can prevent python scripts from running correctly on busy NAS.")


# Resolve custom libs folder if using --target
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIBS_DIR = os.path.join(BASE_DIR, "libs")
sys.path.insert(0, LIBS_DIR)

from filelock import SoftFileLock, Timeout

STALE_LOCK_AGE = 1000  # seconds

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

import time

def acquire_lock_with_stale_handling(lock_path, timeout):
    """
    Attempt to acquire a SoftFileLock.
    If the lock file is older than STALE_LOCK_AGE seconds, force-remove it and retry.
    Raises Timeout if total wait exceeds `timeout`.
    """
    start_time = time.time()
    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
    print(f"Attempting to get filelock on {lock_path}")
    print(f"Start time: {start_time_str}")

    while True:
        lock = SoftFileLock(lock_path, timeout=5)
        try:
            lock.acquire(timeout=5)
            print ("Lock acquired: " +lock_path)
            return lock  # acquired successfully
        except Timeout:
            # Check for stale lock
            if os.path.exists(lock_path):
                age = time.time() - os.path.getmtime(lock_path)
                if age > STALE_LOCK_AGE:
                    try:
                        os.remove(lock_path)
                        print(f"Removed stale lock: {lock_path}")
                    except Exception as e:
                        print(f"Failed to remove stale lock: {e}", file=sys.stderr)
            # Check overall timeout
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                raise Timeout(f"Could not get lock to {lock_path} after {timeout} seconds.")
        time.sleep(1)



def main():
    parser = argparse.ArgumentParser(description="Add a field to a report JSON entry safely.")
    parser.add_argument("--report_json", required=True, help="Path to the report JSON file.")
    parser.add_argument("--match_value", required=True, help="Value to identify the target entry (matches any field value).")
    parser.add_argument("--value_to_add", required=True, help="String or path to a file (file contents added; JSON parsed if possible).")
    parser.add_argument("--value_from_file", required=False, action='store_true', help="attach contents of --value_to_add")
    parser.add_argument("--new_field_name", required=True, help="Name of the field to add to the matched entry.")
    parser.add_argument("--lock_timeout", type=int, default=30, help="Timeout in seconds to acquire the file lock.")
    args = parser.parse_args()

    report_path = args.report_json
    match_value = args.match_value
    value_to_add = args.value_to_add
    new_field_name = args.new_field_name
    lock_timeout = args.lock_timeout

    added_value = value_to_add
    if (args.value_from_file):
            added_value = load_file_value(value_to_add)

    # Lock path
    lock_path = report_path + ".lock"

    try:
        # Acquire lock with stale detection
        lock = acquire_lock_with_stale_handling(lock_path, timeout=lock_timeout)
        with lock:
            # Load report and add entry
            with open(report_path, 'r', encoding='utf-8-sig') as f:
                report = json.load(f)

            #print(f"------------------ Original report: {report_path} ------------------")
            #print(json.dumps(report, indent=2))

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
                json.dump(report, f, indent=2)
                print(f"[OK] Report written: {report_path}")
            
            #print(f"------------------ Updated report: {report_path} ------------------")
            #print(json.dumps(report, indent=2))

    except Timeout as e:
        print(str(e), file=sys.stderr)
        sys.exit(5)

if __name__ == "__main__":
    main()
