#!/usr/bin/env python3
import os
import sys
import json
import fnmatch
import argparse
import logging
import time

import os
import platform
import subprocess
import sys

def normalize_patterns(patterns):
    return [p.lower() for p in patterns] if patterns else []

def folder_matches(path, folder_patterns):
    if not folder_patterns:
        return False
    path_l = os.path.abspath(path).lower()
    basename = os.path.basename(path_l)
    for pat in folder_patterns:
        if fnmatch.fnmatch(path_l, pat) or fnmatch.fnmatch(basename, pat):
            return True
    return False


def file_matches(name_or_path, file_patterns):
    if not file_patterns:
        return False
    name = os.path.basename(name_or_path).lower()
    full = os.path.abspath(name_or_path).lower()
    for pat in file_patterns:
        if fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(full, pat):
            return True
    return False


def list_files(base_path,
               include_files=None,
               exclude_files=None,
               include_folders=None,
               exclude_folders=None):
    include_files = normalize_patterns(include_files)
    exclude_files = normalize_patterns(exclude_files)
    include_folders = normalize_patterns(include_folders)
    exclude_folders = normalize_patterns(exclude_folders)

    if not os.path.exists(base_path):
        #if base_path is part of a filename, add it to include_files, enables search for all files c:\temp\fileprefix*
        parent = os.path.dirname(os.path.abspath(base_path))
        basename = os.path.basename(base_path)
        if os.path.exists(parent):
            # Treat basename as an additional include pattern with wildcard
            pattern = basename + "*"
            # Add to include_files if not already present
            if pattern.lower() not in include_files:
                include_files = include_files + [pattern.lower()]
            base_path = parent
        else:
            raise FileNotFoundError(f"Path not found: {base_path}")

    files = []

    # Single file
    if os.path.isfile(base_path):
        parent = os.path.dirname(os.path.abspath(base_path))
        if exclude_folders and folder_matches(parent, exclude_folders):
            return []
        if include_folders and not folder_matches(parent, include_folders):
            return []
        if exclude_files and file_matches(base_path, exclude_files):
            return []
        if include_files and not file_matches(base_path, include_files):
            return []
        return [os.path.abspath(base_path)]

    # Walk directories
    for root, dirs, filenames in os.walk(base_path):
        if exclude_folders:
            dirs[:] = [d for d in dirs if not folder_matches(os.path.join(root, d), exclude_folders)]
        for fname in filenames:
            full_path = os.path.abspath(os.path.join(root, fname))
            if exclude_folders and folder_matches(root, exclude_folders):
                continue
            if include_folders and not folder_matches(root, include_folders):
                continue
            if exclude_files and file_matches(full_path, exclude_files):
                continue
            if include_files and not file_matches(full_path, include_files):
                continue
            files.append(full_path)

    import re
    def natural_key(s):
        # Split string into list of strings and integers for natural sort
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]
    if len(files) == 0:
        print("Did not find any files in subfolders of: " + str(base_path))
        sys.exit(1)
    return sorted(files, key=natural_key)


if __name__ == "__main__":
    #log only to stderr
    
        # Set up logging
    script_name = os.path.basename(__file__)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stderr),  # Log to stdout
        ]
    )
    logging.info(f"startup")

    parser = argparse.ArgumentParser(
        description="Recursively list files with separate include/exclude filters for files and folders."
    )
    parser.add_argument("path", help="Path to folder or file.")
    parser.add_argument("--include-files", help="Comma-separated list of file name patterns to include (e.g. '*.mp4,*cam1*').")
    parser.add_argument("--exclude-files", help="Comma-separated list of file name patterns to exclude (e.g. '*proxy*').")
    parser.add_argument("--include-folders", help="Comma-separated list of folder patterns to include.")
    parser.add_argument("--exclude-folders", help="Comma-separated list of folder patterns to exclude.")
    parser.add_argument("--report", help="Write results to JSON report file (e.g. 'c:\\temp\\report.json').")
    parser.add_argument("--output-json", help="Optional: path to output JSON file containing found files.")

    args = parser.parse_args()

    include_files = args.include_files.split(",") if args.include_files else []
    exclude_files = args.exclude_files.split(",") if args.exclude_files else []
    include_folders = args.include_folders.split(",") if args.include_folders else []
    exclude_folders = args.exclude_folders.split(",") if args.exclude_folders else []

    result = list_files(
        args.path,
        include_files=include_files,
        exclude_files=exclude_files,
        include_folders=include_folders,
        exclude_folders=exclude_folders,
    )

    print(json.dumps(result, indent=2))

    # Format results based on output type
    if args.report:
        # Create JSON objects with original_file key for each file
        report_data = [{"original_file": filepath} for filepath in result]
        # Write to report file
        try:
            report_dir = os.path.dirname(args.report)
            if report_dir:
                os.makedirs(report_dir, exist_ok=True)
            with open(args.report, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2)
            logging.debug(f"Report written to: {args.report}")
        except Exception as e:
            logging.debug(f"Error writing report file: {e}", file=sys.stderr)
            sys.exit(1)

    if args.output_json:
        try:
            out_dir = os.path.dirname(args.output_json)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            with open(args.output_json, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            logging.debug(f"Output JSON written to: {args.output_json}")
        except Exception as e:
            logging.debug(f"Error writing output JSON file: {e}", file=sys.stderr)
            sys.exit(1)
