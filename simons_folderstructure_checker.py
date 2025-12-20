import argparse
import os
import sys
import re
import logging
import traceback
import io

# Global variable for command line arguments
args = None

def extract_date_cardname(in_file, recursed):
    global args
    in_file = os.path.normpath(in_file)
    recursed = os.path.normpath(recursed)
    
    # Check if recursed has exactly the specified number of folders
    recursed_parts = [p for p in recursed.split(os.sep) if p]  # Filter out empty strings
    if len(recursed_parts) != args.depth:
        logging.error(f"Recursed path must have exactly {args.depth} folder(s), got {len(recursed_parts)}: {recursed}")
        raise ValueError(f"Recursed path must have exactly {args.depth} folder(s), got {len(recursed_parts)}: {recursed}")
    
    # Check if the pre-last part of in_file has a date pattern YYYY_MM_DD
    path_parts = in_file.split(os.sep)
    if len(path_parts) < 2:
        raise ValueError(f"Path does not have enough parts to check pre-last folder: {in_file}")
    pre_last_part = path_parts[-2]  # Second to last part
    date_pattern = re.compile(r'^\d{4}_\d{2}_\d{2}$')
    if not date_pattern.match(pre_last_part):
        raise ValueError(f"Pre-last part of the path does not match YYYY_MM_DD pattern: {pre_last_part}")

    logging.debug(f"Found Date Pattern in pre-last folder: {pre_last_part}")
    device, date, cardname = path_parts[-3], path_parts[-2], path_parts[-1]
    
    return device, date, cardname

def run(in_file, out_root, recursed):
    try:
        device, date, cardname = extract_date_cardname(in_file, recursed)
    except Exception as e:
        logging.debug(f"No Date and Cardname in: [{in_file}]")
        return 1, f"Error extracting DEVICE, DATE and CARDNAME: {e}"

    # Reformat date: YYYYMMDD -> YYYY_MM_DD
    if len(date) == 8 and date.isdigit():
        date_formatted = f"{date[:4]}_{date[4:6]}_{date[6:]}"
    else:
        date_formatted = date

    # calculates AAF OUTPUT DIR
    target_dir = os.path.join(out_root, date_formatted)
    logging.debug(f"Target directory: {target_dir}")
    logging.debug(f"Target exists: {os.path.exists(target_dir)}")
    if os.path.exists(target_dir):
        logging.debug(f"Target is file: {os.path.isfile(target_dir)}, is dir: {os.path.isdir(target_dir)}")
    try:
        os.makedirs(target_dir, exist_ok=True)
        logging.debug(f"Created directory: {target_dir}")
        return 0, target_dir
    except Exception as e:
        logging.error(f"Exception in directory creation:\n{traceback.format_exc()}")
        return 3, f"Unexpected error: {e}"

def main():
    global args
    parser = argparse.ArgumentParser(description="Create a folder based on input path, error if it exists.")
    parser.add_argument("--log", required=True, help="Path to log file")
    parser.add_argument("--input", required=True, help="Input file path")
    parser.add_argument("--out_root", required=True, help="Root output directory")
    parser.add_argument("--recursed", required=True, help="Watch folder path")
    parser.add_argument("--depth", required=False, help="Recursed path must consist of exactly this cound of folders", default=2, type=int)
    parser.add_argument("--debug", default=True, action="store_true", help="Enable debug mode")
    args = parser.parse_args()
    
    # Set up logging buffer
    log_buffer = io.StringIO()
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = logging.StreamHandler(log_buffer)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    code = 1
    message = "Unexpected error"
    
    try:
        logging.info(">>>>>>>>>>>>>> Script started:" + args.input)
        code, message = run(args.input, args.out_root, args.recursed)
        logging.info("<<<<<<<<<<<<< Script end: " + args.input + " -> " + message + "\n\n")
    except Exception as e:
        logging.error(f"Error in main: {e}")
        print(f"Error in main: {e}", file=sys.stderr)
        code = 1
        message = f"Unexpected error: {e}"
    finally:
        # Write all logs to file at once, always
        try:
            log_size = os.path.getsize(args.log) if os.path.exists(args.log) else 0
            mode = 'w' if log_size > 10 * 1024 * 1024 else 'a'
            with open(args.log, mode) as f:
                f.write(log_buffer.getvalue())
        except Exception as e:
            print(f"Failed to write log: {e}", file=sys.stderr)
    
    # defines aaf_output_dir for workflow (by printing it to stdout)
    print(message)
    sys.exit(code)

if __name__ == "__main__":
    main()