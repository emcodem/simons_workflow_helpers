from html import parser
import sys
import re
import subprocess
from pathlib import Path
import difflib
import argparse
import os
import time
import logging


# Set up logging
script_name = os.path.basename(__file__)

# Create logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)


# Define filters
class AllExceptErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno != logging.ERROR

class OnlyErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.ERROR

# Add filters to handlers
stdout_handler.addFilter(AllExceptErrorFilter())
stderr_handler.addFilter(OnlyErrorFilter())

# Add handlers to logger
logger.addHandler(stdout_handler)
logger.addHandler(stderr_handler)

logging.info(f"Startup")


def apply_rules(command_line: str, args) -> str:
    """
    Apply transformation rules to the FFmpeg command line.
    Each rule is a regex substitution.
    """
    additional_options = args.additional_options
    if  (not additional_options):
        additional_options = ""

    bmx_cmd = args.bmx_cmd
    replace_output = args.replace_output
    assume_source_fps = args.assume_source_fps
    
    insert_filter = "," + args.insert_filter if args.insert_filter != "" else ""
    hwupload_cuda_insertion = ",hwupload_cuda" if args.insert_hwupload_cuda else ""

    insert_filter += hwupload_cuda_insertion

    rules = []
    
    # Add search and replace rules from --search-replace arguments
    if hasattr(args, 'search_replace') and args.search_replace:
        for search_replace_pair in args.search_replace:
            search_value, replace_value = search_replace_pair
            # Use literal string replacement (not regex) to handle commas safely
            rules.append(('literal', search_value, replace_value))
    #if additional_options contains -cq, remove "-b:v .+? "
    if (additional_options.find("-cq") != -1):
        rules.append((r" -b:v .+? ", " "))

    if (additional_options.find("-preset") != -1):
        rules.append((r" -preset .+? ", " "))

    if (additional_options.find(" -g .+? ") != -1):
        rules.append((r" -g .+? ", " "))

    if (insert_filter != ""):
        # Insert the specified filters AND hwupload_cuda filter as last video filter before [vstr1]
        rules.append((r"setsar=r=1:max=1\[vstr1\]","setsar=r=1:max=1" + insert_filter + "[vstr1]"))

    if args.prepend_audio_filter != "":
        # Prepend audio filter before each [astrX] where X is any number
        rules.append((r"\[astr(\d+)\]", f",{args.prepend_audio_filter}[astr\\1]"))

    if args.remove_shortest:
        # Remove -shortest flag from command
        rules.append((r" -shortest ", " "))

    #as a last thing, replace libx264 with h264_nvenc plus additional options
    rules.append((r"-c:v libx264", " -c:v h264_nvenc " + additional_options + " "))

    modified = command_line
    for rule in rules:
        if rule[0] == 'literal':
            # Literal string replacement (not regex)
            _, search_value, replace_value = rule
            modified = modified.replace(search_value, replace_value)
            logging.debug(f"Applied literal replacement: '{search_value}' -> '{replace_value}'")
        else:
            # Regex replacement (original behavior)
            pattern, replacement = rule[0], rule[1]
            modified = re.sub(pattern, replacement, modified, count=1)

    # If assume_source_fps is provided, insert -r <fps> before -i
    if assume_source_fps:
        modified = re.sub(r' -i "', f' -r {assume_source_fps} -i "', modified, count=1)

    # If bmx_cmd is provided, replace the part after the last pipe with bmx_cmd
    if bmx_cmd:
        # Escape backslashes in bmx_cmd for safe use in replacement string
        bmx_cmd_escaped = bmx_cmd.replace("\\", "\\\\")
        # Match the last pipe (not followed by another pipe) and everything after it
        modified = re.sub(r"\|(?!.*\|).*$", f"| {bmx_cmd_escaped}", modified, flags=re.DOTALL)

    # If replace_output is provided, replace the output file in the command
    if replace_output:
        # Replace the output file (last token) with replace_output
        # Escape backslashes in replace_output for safe use in replacement string
        replace_output_escaped = replace_output.replace("\\", "\\\\")
        modified = re.sub(r"\"[^\"]*\"$", f'"{replace_output_escaped}"', modified)

    return modified.strip()


def print_diff(original: str, modified: str):
    """
    Print a human-readable word-level diff between two strings.
    """
    diff = difflib.Differ().compare(original.split(), modified.split())
    logging.info("==== COMMAND DIFFERENCES ====")
    for line in diff:
        # Highlight additions and deletions only
        if line.startswith("+ "):
            logging.info(f"\033[92m{line}\033[0m")  # green = added
        elif line.startswith("- "):
            logging.info(f"\033[91m{line}\033[0m")  # red = removed
    logging.info(modified)
        # Uncomment to show unchanged tokens:
        # elif line.startswith("  "):
        #     logging.info(line)
    logging.info("=============================\n")


def get_duration_ffprobe(file_path: str, ffprobe_path: str) -> float:
    """
    Get the duration of a media file using ffprobe.
    Returns duration in seconds as a float.
    """
    try:
        cmd = [
            ffprobe_path,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1:nokey=1",
            file_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return float(result.stdout.strip())
        else:
            logging.error(f"ffprobe error for {file_path}: {result.stderr}")
            return None
    except Exception as e:
        logging.error(f"Error getting duration for {file_path}: {e}")
        return None


def check_duration(input_file: str, output_file: str, ffprobe_path: str, tolerance_sec: float = 2.0) -> bool:
    """
    Check if input and output file durations match within tolerance (default 1 second).
    Returns True if durations match within tolerance_sec, False otherwise.
    """
    logging.info("==== DURATION CHECK ====")
    
    # Check input file exists
    input_path = Path(input_file)
    if not input_path.exists():
        logging.error(f"Input file not found: {input_file}")
        return False
    
    # Check output file exists (try with _v1.mxf appended if original doesn't exist)
    output_path = Path(output_file)
    if not output_path.exists():
        output_path_v1 = Path(str(output_file) + '_v1.mxf')
        if output_path_v1.exists():
            output_path = output_path_v1
            logging.info(f"Output file not found as provided, using: {output_path}")
        else:
            logging.error(f"Output file not found: {output_file} or {output_path_v1}")
            return False
    
    # Check ffprobe exists
    ffprobe_path_obj = Path(ffprobe_path)
    if not ffprobe_path_obj.exists():
        logging.error(f"ffprobe not found: {ffprobe_path}")
        return False
    
    # Get durations
    input_duration = get_duration_ffprobe(str(input_path), ffprobe_path)
    if input_duration is None:
        logging.error(f"Failed to get duration of input file: {input_file}")
        return False
    
    output_duration = get_duration_ffprobe(str(output_path), ffprobe_path)
    if output_duration is None:
        logging.error(f"Failed to get duration of output file: {output_path}")
        return False
    
    # Compare durations with tolerance
    duration_diff = abs(input_duration - output_duration)
    logging.info(f"Input duration: {input_duration:.2f}s")
    logging.info(f"Output duration: {output_duration:.2f}s")
    logging.info(f"Difference: {duration_diff:.2f}s (tolerance: {tolerance_sec}s)")
    
    if duration_diff <= tolerance_sec:
        logging.info("Duration check PASSED")
        logging.info("=======================\n")
        return True
    else:
        logging.error(f"Duration check FAILED - difference exceeds tolerance")
        logging.error(f"Difference: {duration_diff:.2f}s (tolerance: {tolerance_sec}s)")
        logging.error(f"Input file '{input_file}': {input_duration:.2f}s")
        logging.error(f"Output file '{output_path}': {output_duration:.2f}s")
        logging.info("=======================\n")
        return False

def credential_exists(target):
    # We list the specific target and look for the "Target:" string in the output
    cmd = f'cmdkey /list:{target} | findstr "Target:"'
    result = subprocess.run(cmd, shell=True, capture_output=True)
    
    return result.returncode == 0

def main():
    parser = argparse.ArgumentParser(description="Apply transformation rules to FFmpeg command and execute it.")
    parser.add_argument("command_file", help="Path to the command file to read")
    parser.add_argument("--additional-options", help="Additional options to pass (optional, example -preset p4 -g 50)")
    parser.add_argument("--bmx_cmd_file", help="Path to a file containing a full bmx cmd, prepared to read from pipe. In this case, the ffastrans cmd must end with a bmx cmd already")
    parser.add_argument("--replace_output", help="Path to output file, only works when no bmx is used in ffastrans cmd")
    parser.add_argument("--insert_hwupload_cuda", help="Inserts hwupload_cuda filter as last video filter (before [vstr1] in filter_complex)", action='store_true')
    parser.add_argument("--assume_source_fps", help="inserts -r xx before -i to assume source fps")
    parser.add_argument("--insert_filter", default="", help="inserts the specified line into filters, e.g. format=yuv422p")
    parser.add_argument("--prepend_audio_filter", default="", help="prepends the specified audio filter to the filter chain (inserts before each [astrX] in filter_complex)")
    parser.add_argument("--remove_shortest", help="Removes -shortest flag from the FFmpeg command", action='store_true')
    parser.add_argument("--search-replace", nargs=2, action='append', metavar=('SEARCH', 'REPLACE'), 
                        help="Search and replace string in command (can be used multiple times). Values can contain commas.")
    
    parser.add_argument("--output_root", help="Output root folder to create recursively if it doesn't exist")
    parser.add_argument("--test", help="Test mode: print modified command without executing it", action='store_true')

    #duration check
    parser.add_argument("--input_file", help="Path to the input media file for duration checking")
    parser.add_argument("--output_file", help="Path to the output file for encoding results (used with duration check)")
    parser.add_argument("--ffprobe", help="Path to ffprobe executable for duration validation (used with duration check)")
    parser.add_argument("--duration_check_tolerance", type=float, default=2.0, help="Tolerance in seconds for duration check (default: 2.0)")
    parser.add_argument("--check_duration", type=bool, default=False, help="Enable duration check between input and output files (accept True/False, 1/0)")
    
    #local storage credentials (optional)
    parser.add_argument("--storage_account", help="if output_root is set, attpemts to store credentials in windows credentials manager (optional)")
    parser.add_argument("--storage_pass", help="Storage account password for local storage access (optional)")

    args = parser.parse_args()
    cmd_file_path = Path(args.command_file)
    additional_options = args.additional_options
    replace_output = args.replace_output
    
    if (args.storage_account):
        #check if output_root and storage_pass are set
        if (not args.output_root) or (not args.storage_pass):
            logging.error("Error: --storage_account requires --output_root and --storage_pass to be set.")
            sys.exit(1)
        
        #parse server name/op from output_root
        _server_name = ""
        output_root_path = Path(args.output_root)
        if (output_root_path.drive.startswith("\\\\")):
            #network path
            parts = output_root_path.parts
            if len(parts) >= 2:
                _server_name = parts[1]
        else:
            logging.error("Error: --output_root must be a network path (UNC) when using --storage_account.")
            sys.exit(1)
        logging.info(f"Checking Credentials for server: {_server_name}")
        if (not credential_exists(args.storage_account)):
            #store credentials
            logging.info(f"Storing Credentials for account: {args.storage_account}, server: {_server_name}")
            cmd = f'cmdkey /add:{args.storage_account} /user:{args.storage_account} /pass:{args.storage_pass}'
            logging.info(f"Executing command: {cmd}")
            result = subprocess.run(cmd, shell=True, capture_output=True)
            if result.returncode == 0:
                logging.info(f"Credentials for {args.storage_account} stored successfully.")
            else:
                logging.error(f"Error storing credentials for {args.storage_account}: {result.stderr.decode().strip()}")
                sys.exit(1)
    
    # Create output_root folder if specified
    if args.output_root:
        output_root_path = Path(args.output_root)
        try:
            output_root_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"Output root folder created (or already exists): {output_root_path}")
        except Exception as e:
            logging.error(f"Error creating output_root folder: {e}")
            sys.exit(1)
    
    # Print parsed arguments dynamically
    logging.info("==== PARSED ARGUMENTS ====")
    for arg_name, arg_value in vars(args).items():
        if arg_value is not None and arg_value != "":
            logging.info(f"{arg_name.replace('_', ' ').title()}: {arg_value}")
    logging.info("===========================\n")


    if not cmd_file_path.exists():
        logging.error(f"Error: File not found: {cmd_file_path}")
        sys.exit(1)

    # Read command
    with cmd_file_path.open('r', encoding='utf-8') as f:
        original_cmd = f.read().strip()
    logging.info(f"==== original_cmd contents ====")
    logging.info(original_cmd)
    logging.info("====================\n")
    
    if (args.replace_output and original_cmd.find("bmxtranswrap") != -1):
        # Replace output file in original_cmd with replace_output
        logging.error(f"Error: replace_output is set but the original cmd contained bmxtranswrap, this is not implemented.")
        sys.exit(1)

    # Check if bmx_cmd_file is set and read it if exists
    bmx_cmd = None
    if args.bmx_cmd_file:
        has_bmxtranswrap_pipe = bool(re.search(r'\|.*bmxtranswrap', original_cmd))
        if not has_bmxtranswrap_pipe:
            logging.error("Error: The original ffastrans command does not use bmxtranswrap, but bmx_cmd_file is set.")
            sys.exit(1)
        bmx_cmd_path = Path(args.bmx_cmd_file)
        if not bmx_cmd_path.exists():
            logging.error(f"Error: BMX command file not found: {bmx_cmd_path}")
            sys.exit(1)
        with bmx_cmd_path.open('r', encoding='utf-8') as f:
            bmx_cmd = f.read().strip()
            bmx_cmd = bmx_cmd.replace("--track-map .+? ", "")  # Escape backslashes for safe insertion
        logging.info(f"==== bmx_cmd contents ====")
        logging.info(bmx_cmd)
        logging.info("====================\n")
    
    if not original_cmd:
        logging.error("Error: Command file is empty.")
        sys.exit(1)
    
    # Check if original_cmd contains a pipe followed by bmxtranswrap
    

    # Apply the transformation rules
    # Set bmx_cmd on args so apply_rules can access it
    args.bmx_cmd = bmx_cmd
    modified_cmd = apply_rules(original_cmd, args)

    # Show differences instead of printing full commands
    print_diff(original_cmd, modified_cmd)

    # Test mode: just print the command without executing
    if args.test:
        logging.info("TEST MODE: Command not executed")
        logging.info(modified_cmd)
        sys.exit(0)

    # Execute modified command
    try:
        logging.info("Executing modified command...")
        logging.info(modified_cmd)
        
        # Execute command directly without piping - FFmpeg prefers direct console access
        #return_code = subprocess.call(modified_cmd, shell=True)
        process = subprocess.Popen(
            modified_cmd, 
            shell=True, 
            stderr=subprocess.PIPE, 
            stdin=subprocess.DEVNULL,
            universal_newlines=True
        )

        # This iterator will automatically stop when the pipe closes (process ends)
        logging.info("reading stderr...n")
        for line in process.stderr:
            logging.info(line.strip())

        logging.info("getting return code...")
        return_code = process.wait()

        #encoding done, print result
        logging.info(f"Return code: {return_code}")

        # If replace_output was set, check if the file exists and > 0kb
        if args.replace_output:
            output_path = Path(args.replace_output)
            if output_path.exists():
                file_size_kb = output_path.stat().st_size / 1024
                logging.info(f"Output file created: {args.replace_output} ({file_size_kb:.2f} KB)")
                if file_size_kb == 0:
                    logging.warning("Warning: Output file is empty (0 KB)")
            else:
                logging.warning(f"Warning: Output file not found: {args.replace_output}")

        # If duration check is enabled, perform it
        if args.check_duration:
            if not args.input_file or not args.output_file or not args.ffprobe:
                logging.error("Error: --input_file, --output_file, and --ffprobe must be specified for duration check.")
                sys.exit(1)
            duration_match = check_duration(args.input_file, args.output_file, args.ffprobe, args.duration_check_tolerance)
            if not duration_match:
                logging.error("Duration check failed.")
                return_code = 2  # Set return code to indicate duration check failure
        
        #FINAL EXIT
        sys.exit(return_code)
    except Exception as e:
        logging.error(f"Error executing command: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
