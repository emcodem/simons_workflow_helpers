import argparse
import json
import shutil
import time
import sys
import os
from pathlib import Path

def move_file_with_retry(src: Path, dst: Path, retries=100, delay=1):
    for attempt in range(retries + 1):
        try:
            shutil.move(str(src), str(dst))
            return True
        except Exception as e:
            print(f"Failed to move {src} -> {dst}: {e}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Giving up.")
                return False

def write_json_with_retry(data, json_path: Path, retries=100, delay=1):
    for attempt in range(retries + 1):
        try:
            json_path.parent.mkdir(parents=True, exist_ok=True)
            with json_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            print(f"Failed to write JSON to {json_path}: {e}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Giving up.")
                return False


def move_mxf_files(input_dir: str, output_dir: str, json_out: str, include_files: str = "*.mxf"):
    
    output_dir.mkdir(parents=True, exist_ok=True)

    moved_files = []

    patterns = [p.strip() for p in include_files.split(",")]

    for pattern in patterns:
        _files = list(input_dir.glob(pattern))
        print(f"Found File Count: {len(_files)} for pattern: {pattern} in folder: {input_dir}")
        for file in _files:
            target = output_dir / file.name
            success = move_file_with_retry(file, target, retries=1, delay=5)
            if success:
                moved_files.append(str(target.resolve()))
            else:
                print(f"Failed to move {file} to {target}")

    if len(moved_files) == 0:
        print("No files were moved.")
        sys.exit(1)

    if write_json_with_retry(moved_files, json_out, retries=1, delay=5):
        print(json.dumps(moved_files, indent=2))
        print("")
        print(f"Moved {len(moved_files)} files")
        print(f"JSON written to: {json_out}")
    else:
        print("JSON write failed, moved files list not saved.")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move MXF files and write moved paths to JSON")

    parser.add_argument("--input-dir", required=True, help="BMX work directory containing .mxf files")
    parser.add_argument("--output-dir", required=True, help="Output directory to move MXF files to")
    parser.add_argument("--json-out", required=True, help="JSON file path to write moved file list")
    parser.add_argument("--include-files", default="*.mxf", help="Comma-separated list of file patterns to include (default: *.mxf)")
    #log args
    
    args = parser.parse_args()
    print(f"Arguments: {args}")

    move_mxf_files(Path(args.input_dir), Path(args.output_dir), Path(args.json_out), args.include_files)
