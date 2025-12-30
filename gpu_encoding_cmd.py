import sys
import re
import subprocess
from pathlib import Path
import difflib
import argparse


def apply_rules(command_line: str, args) -> str:
    """
    Apply transformation rules to the FFmpeg command line.
    Each rule is a regex substitution.
    """
    additional_options = args.additional_options
    bmx_cmd = args.bmx_cmd if hasattr(args, 'bmx_cmd') else None
    replace_output = args.replace_output if hasattr(args, 'replace_output') else None
    assume_source_fps = args.assume_source_fps if hasattr(args, 'assume_source_fps') else None
    
    insert_filter = "," + args.insert_filter if args.insert_filter != "" else ""
    hwupload_cuda_insertion = ",hwupload_cuda" if hasattr(args, 'insert_hwupload_cuda') else ""

    insert_filter += hwupload_cuda_insertion

    rules = []
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


    #as a last thing, replace libx264 with h264_nvenc plus additional options
    rules.append((r"-c:v libx264", " -c:v h264_nvenc " + additional_options + " "))

    modified = command_line
    for pattern, replacement in rules:
        # For the -i rule, only replace the first occurrence
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
    print("==== COMMAND DIFFERENCES ====")
    for line in diff:
        # Highlight additions and deletions only
        if line.startswith("+ "):
            print(f"\033[92m{line}\033[0m")  # green = added
        elif line.startswith("- "):
            print(f"\033[91m{line}\033[0m")  # red = removed
    print (modified)
        # Uncomment to show unchanged tokens:
        # elif line.startswith("  "):
        #     print(line)
    print("=============================\n")


def main():
    parser = argparse.ArgumentParser(description="Apply transformation rules to FFmpeg command and execute it.")
    parser.add_argument("command_file", help="Path to the command file to read")
    parser.add_argument("--additional-options", default="-preset p5 -rc vbr_hq -cq 22 -b:v 0 -g 50 -bf 3", help="Additional options to pass (optional, default -preset p4 -g 50)")
    parser.add_argument("--bmx_cmd_file", help="Path to a file containing a full bmx cmd, prepared to read from pipe. In this case, the ffastrans cmd must end with a bmx cmd already")
    parser.add_argument("--replace_output", help="Path to output file, only works when no bmx is used in ffastrans cmd")
    parser.add_argument("--insert_hwupload_cuda", help="Inserts hwupload_cuda filter as last video filter (before [vstr1] in filter_complex)", action='store_true')
    parser.add_argument("--assume_source_fps", help="inserts -r xx before -i to assume source fps")
    parser.add_argument("--insert_filter", default="", help="inserts the specified line into filters, e.g. format=yuv422p")
    parser.add_argument("--test", help="Test mode: print modified command without executing it", action='store_true')

    args = parser.parse_args()
    cmd_file_path = Path(args.command_file)
    additional_options = args.additional_options
    replace_output = args.replace_output
    # Print parsed arguments dynamically
    print("==== PARSED ARGUMENTS ====")
    for arg_name, arg_value in vars(args).items():
        if arg_value is not None and arg_value != "":
            print(f"{arg_name.replace('_', ' ').title()}: {arg_value}")
    print("===========================\n")


    if not cmd_file_path.exists():
        print(f"Error: File not found: {cmd_file_path}")
        sys.exit(1)

    # Read command
    with cmd_file_path.open('r', encoding='utf-8') as f:
        original_cmd = f.read().strip()
    print(f"==== original_cmd contents ====")
    print(original_cmd)
    print("====================\n")
    
    if (args.replace_output and original_cmd.find("bmxtranswrap") != -1):
        # Replace output file in original_cmd with replace_output
        print(f"Error: replace_output is set but the original cmd contained bmxtranswrap, this is not implemented.")
        sys.exit(1)

    # Check if bmx_cmd_file is set and read it if exists
    bmx_cmd = None
    if args.bmx_cmd_file:
        has_bmxtranswrap_pipe = bool(re.search(r'\|.*bmxtranswrap', original_cmd))
        if not has_bmxtranswrap_pipe:
            print("Error: The original ffastrans command does not use bmxtranswrap, but bmx_cmd_file is set.")
            sys.exit(1)
        bmx_cmd_path = Path(args.bmx_cmd_file)
        if not bmx_cmd_path.exists():
            print(f"Error: BMX command file not found: {bmx_cmd_path}")
            sys.exit(1)
        with bmx_cmd_path.open('r', encoding='utf-8') as f:
            bmx_cmd = f.read().strip()
            bmx_cmd = bmx_cmd.replace("--track-map .+? ", "")  # Escape backslashes for safe insertion
        print(f"==== bmx_cmd contents ====")
        print(bmx_cmd)
        print("====================\n")
    
    if not original_cmd:
        print("Error: Command file is empty.")
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
        print("TEST MODE: Command not executed")
        print(modified_cmd)
        sys.exit(0)

    # Execute modified command
    try:
        print("Executing modified command...\n")
        print(modified_cmd)
        
        # Execute command directly without piping - FFmpeg prefers direct console access
        return_code = subprocess.call(modified_cmd, shell=True)

        #encoding done, print result
        print(f"\nReturn code: {return_code}")

        # If replace_output was set, check if the file exists and > 0kb
        if args.replace_output:
            output_path = Path(args.replace_output)
            if output_path.exists():
                file_size_kb = output_path.stat().st_size / 1024
                print(f"Output file created: {args.replace_output} ({file_size_kb:.2f} KB)")
                if file_size_kb == 0:
                    print("Warning: Output file is empty (0 KB)")
            else:
                print(f"Warning: Output file not found: {args.replace_output}")

        sys.exit(return_code)
    except Exception as e:
        print(f"Error executing command: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
