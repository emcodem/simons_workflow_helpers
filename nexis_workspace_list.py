import win32com.client
import json
import sys
import argparse

def get_avid_paths(target_path):
    results = []

    try:
        # 1. Create the Shell Application object
        shell = win32com.client.Dispatch("Shell.Application")
        
        # 2. Bind to the namespace provided via command line
        folder = shell.NameSpace(target_path)

        if folder is None:
            # We print errors to stderr so they don't corrupt the JSON in stdout
            print(f"Error: Could not bind to '{target_path}'. Is the path valid and NEXIS running?", file=sys.stderr)
            sys.exit(1)

        # 3. Enumerate the items
        items = folder.Items()
        for i in range(items.Count):
            item = items.Item(i)
            
            results.append({
                "Name": item.Name,
                "Path": item.Path,
                "IsFolder": item.IsFolder,
                "LastModified": str(item.ModifyDate) if item.ModifyDate else None
            })

        # 4. Output the JSON array to stdout
        print(json.dumps(results, indent=4))

    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enumerate Avid NEXIS Workspaces via Windows Shell Namespace.")
    
    # Define the positional argument
    parser.add_argument(
        "path", 
        help="The Avid virtual path (e.g., \\\\MYNEXIS)"
    )

    args = parser.parse_args()
    get_avid_paths(args.path)