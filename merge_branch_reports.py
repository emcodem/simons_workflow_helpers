#!/usr/bin/env python3
"""
Merge branch reports into a full report.
Full report is created by files find and resides unmodified until the branches finished. 
Branch reports are created by each branch at start. 
At the end of the workflow, this script merges the full report with branch reports in order to feed createaaf with complete data.
Finds entries by original_file or remaster_file and replaces them in the full report.
"""

import sys
import json
import os
import argparse


def main():
    parser = argparse.ArgumentParser(description="Merge branch reports into a full report.")
    parser.add_argument("--full_report", required=True, help="Path to the full report JSON file.")
    parser.add_argument("--branch_report_dir", required=True, help="Directory containing branch report JSON files.")
    args = parser.parse_args()

    full_report_path = args.full_report
    branch_report_dir = args.branch_report_dir

    # Load full report
    with open(full_report_path, 'r', encoding='utf-8-sig') as f:
        full_report = json.load(f)

    # List all files in branch report directory, sorted by name
    if not os.path.exists(branch_report_dir):
        print(f"Error: branch_report_dir does not exist: {branch_report_dir}", file=sys.stderr)
        sys.exit(1)

    branch_files = sorted([f for f in os.listdir(branch_report_dir) if f.endswith('.json')])

    # Process each branch report
    for branch_file in branch_files:
        branch_file_path = os.path.join(branch_report_dir, branch_file)
        
        # Read branch report
        with open(branch_file_path, 'r', encoding='utf-8-sig') as f:
            branch_report = json.load(f)

        # Each branch report has exactly one entry at top level
        if not isinstance(branch_report, list) or len(branch_report) != 1:
            print(f"Error: {branch_file} does not contain exactly one entry at top level", file=sys.stderr)
            sys.exit(1)

        branch_entry = branch_report[0]
        
        # Try to get matching key (original_file or remaster_file)

        if 'original_file' in branch_entry:
            match_value = branch_entry['original_file']
        elif 'remaster_file' in branch_entry:
            match_value = branch_entry['remaster_file']
        
        # Find and replace the entry in full_report by matching the value in any key
        found = False
        for i, entry in enumerate(full_report):
            if match_value in entry.values():
                branch_entry['found_branch_report'] = True
                full_report[i] = branch_entry
                found = True
                break

        if not found:
            print(f"Error: value '{match_value}' from {branch_file} not found in full_report", file=sys.stderr)
            sys.exit(1)

    # Write updated full report
    with open(full_report_path, 'w', encoding='utf-8') as f:
        json.dump(full_report, f, indent=2)
        print(f"[OK] Full report updated: {full_report_path}")


if __name__ == "__main__":
    main()
