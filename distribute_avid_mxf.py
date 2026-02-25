import logging
import argparse
import os
import re
import shutil
from pathlib import Path
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def count_files_in_directory(path):
    """Count total files in directory recursively."""
    try:
        return sum(1 for _ in Path(path).glob('*') if _.is_file())
    except Exception:
        return 0

def main():
    """Distribute MXF files into target directories by group pattern."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Distribute Avid MXF files by group pattern'
    )
    parser.add_argument('--source_path', required=True, help='Source directory containing MXF files')
    parser.add_argument('--target_path', required=True, help='Target directory to distribute files into')
    parser.add_argument('--file_group_pattern', required=True, help='Regex pattern to group related files')
    
    args = parser.parse_args()
    
    source_path = Path(args.source_path)
    target_base = Path(args.target_path)
    pattern_str = args.file_group_pattern
    
    # Validate source path
    if not source_path.exists():
        logger.error(f"Source path does not exist: {source_path}")
        return
    
    if not source_path.is_dir():
        logger.error(f"Source path is not a directory: {source_path}")
        return
    
    # Create initial target path
    target_base.mkdir(parents=True, exist_ok=True)
    logger.info(f"Target base path: {target_base}")
    
    # List all .mxf files
    mxf_files = sorted(source_path.glob('*.mxf'))
    logger.info(f"Found {len(mxf_files)} MXF files in {source_path}")
    
    if not mxf_files:
        logger.warning("No MXF files found")
        return
    
    # Group files by pattern
    groups = defaultdict(list)
    regex = re.compile(pattern_str)
    logger.info(f"Using regex pattern: {regex.pattern}")
    unmatched = []
    
    for file in mxf_files:
        match = regex.search(file.name)
        if match:
            # Use the first capture group if available, otherwise use the whole match
            group_key = match.group(1) if match.groups() else match.group(0)
            groups[group_key].append(file)
        else:
            unmatched.append(file.name)
    
    if unmatched:
        logger.warning(f"{len(unmatched)} files did not match pattern: {', '.join(unmatched[:5])}")
    
    logger.info(f"Grouped {len(mxf_files) - len(unmatched)} files into {len(groups)} groups")
    
    # Sort groups for consistent processing
    sorted_groups = sorted(groups.items())
    
    # Move groups into target directories
    current_target = target_base
    current_count = count_files_in_directory(current_target)
    target_counter = 0
    total_files_moved = 0
    directory_stats = {}  # Track stats per directory
    
    for group_key, files in sorted_groups:
        group_size = len(files)
        
        # Check if we need to create a new target directory
        if current_count > 4900:
            # Log current directory stats
            logger.info(
                f"Directory '{current_target.name}': "
                f"Moved {current_count} files in group"
            )
            directory_stats[str(current_target)] = current_count
            
            # Create new target directory
            target_counter += 1
            current_target = Path(f"{target_base}{target_counter}")
            current_target.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created new target directory: {current_target}")
            current_count = 0
        
        # Move files in group
        for file in files:
            destination = current_target / file.name
            try:
                shutil.move(str(file), str(destination))
                total_files_moved += 1
            except Exception as e:
                logger.error(f"Failed to move {file} to {destination}: {e}")
                continue
        
        current_count += group_size
    
    # Log final directory stats
    if current_count > 0:
        logger.info(
            f"Directory '{current_target.name}': "
            f"Moved {current_count} files"
        )
        directory_stats[str(current_target)] = current_count
    
    # Log summary
    logger.info("=" * 60)
    logger.info("DISTRIBUTION SUMMARY")
    logger.info("=" * 60)
    for target_dir, file_count in sorted(directory_stats.items()):
        logger.info(f"{target_dir}: {file_count} files")
    logger.info(f"Total files moved: {total_files_moved}")
    logger.info(f"Total groups distributed: {len(groups)}")
    
    # Check if source folder is empty and delete if so
    remaining_files = list(source_path.glob('*'))
    if not remaining_files:
        try:
            source_path.rmdir()
            logger.info(f"Source directory deleted (was empty): {source_path}")
        except Exception as e:
            logger.error(f"Failed to delete empty source directory: {e}")
    else:
        logger.info(f"Source directory still contains {len(remaining_files)} items, not deleting")

if __name__ == '__main__':
    main()