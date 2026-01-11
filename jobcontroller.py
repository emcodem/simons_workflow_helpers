#!/usr/bin/env python3
"""
FFAStrans Job Controller
Orchestrates media file processing workflow including:
- File discovery
- Framerate checking and offspeed conversion
- FFAStrans API job submission
- Job monitoring
- Report merging
- AAF creation
"""

import sys
import json
import os
import argparse
import subprocess
import time
import logging
import uuid
import shutil
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

# Import local modules
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import findfiles
import jobcontroller_ffastrans_api

# Set up logging with per-process log files
def setup_logging(source_file=None, timestamp=None):
    """Set up logging with separate file for each process."""
    pid = os.getpid()
    
    if timestamp is None:
        timestamp = time.strftime("%Y_%m_%d_%H_%M_%S")
    log_dir = f"c:\\temp\\jobcontroller_{timestamp}"
    os.makedirs(log_dir, exist_ok=True)
    
    if source_file:
        # Use source file basename (without extension) in log filename
        base_name = os.path.splitext(os.path.basename(source_file))[0]
        log_filename = os.path.join(log_dir, f"{base_name}_PID_{pid}.log")
    else:
        # Fallback to script name if no source file provided
        script_name = os.path.splitext(os.path.basename(__file__))[0]
        log_filename = os.path.join(log_dir, f"{script_name}_PID_{pid}.log")
    
    # Clear any existing handlers
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s - [PID:%(process)d] - %(levelname)s - %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (per-process)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logging.info(f"Logging to file: {log_filename}")
    return log_filename

setup_logging()

# Configuration
CONFIG = {
    'starting_dir': '',
    'ffastrans_encoding_wf_guid': 'ffastrans_encoding_workflow_guid_here',
    'ffastrans_encoding_wf_start_proc': 'ffastrans_encoding_workflow_start_proc_here',
    'ffastrans_api_url': 'http://localhost:3003/jobs',
    'ffastrans_api_getjobdetails_url': 'http://localhost:3003/getjobdetails',
    'aaf_script_root': '',
    'avid_aaf_output_dir': '',
    'avid_mxf_output_dir': '',
    'remove_success_reports': True,
    'concurrent_file_processes': 40,
    'project_fps': 25,
    'job_work_dir': '',
    'report_file': '',
    'ffmpeg_path': 'ffmpeg',
    'ffprobe_path': 'ffprobe',
    'http_max_retries': 10,  # Consecutive HTTP failures before giving up
    'http_poll_interval': 1,  # Seconds between status polls
}


def get_python_executable(script_root: str) -> str:
    """Get path to pythonw.exe in portable Python installation."""
    python_path = os.path.join(script_root, 'python_portable', 'pythonw.exe')
    if os.path.exists(python_path):
        return python_path
    return 'pythonw.exe'


def get_script_path(script_name: str) -> str:
    """Get path to helper script, checking both workflow_helpers and createaaf folders."""
    # Try workflow_helpers folder (same directory as this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path1 = os.path.join(script_dir, script_name)
    if os.path.exists(path1):
        return path1
    
    # Try createaaf folder (sibling to workflow_helpers)
    parent_dir = os.path.dirname(script_dir)
    path2 = os.path.join(parent_dir, 'createaaf', script_name)
    if os.path.exists(path2):
        return path2
    
    # Return the script name and hope it's in PATH
    return script_name


def find_files(starting_dir: str, report_file: str) -> List[Dict[str, Any]]:
    """Find media files using findfiles module."""
    logging.info(f"Finding files in {starting_dir}")
    
    # Use findfiles module directly
    include_files = ['*.mxf', '*.mov', '*.mp4']
    exclude_files = ['*_offspeed_*']
    
    file_paths = findfiles.list_files(
        starting_dir,
        include_files=include_files,
        exclude_files=exclude_files
    )
    
    # Convert to report format (list of dicts with 'original_file' key)
    files = [{'original_file': fp} for fp in file_paths]
    
    # Write report file
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(files, f, indent=2)
    
    logging.info(f"Found {len(files)} files")
    return files


def get_framerate(source_file: str) -> float:
    """Get framerate of video file using ffprobe."""
    info = get_media_info(source_file)
    return info['framerate']


def get_media_info(source_file: str) -> dict:
    """Get framerate and start timecode from video file using ffprobe."""
    # Get framerate
    cmd_fps = [
        CONFIG['ffprobe_path'],
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=r_frame_rate',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        source_file
    ]
    
    # Log the exact command for manual testing
    cmd_fps_str = ' '.join([f'"{arg}"' if ' ' in arg else arg for arg in cmd_fps])
    logging.info(f"Executing ffprobe for framerate: {cmd_fps_str}")
    
    result_fps = subprocess.run(cmd_fps, capture_output=True, text=True)
    if result_fps.returncode != 0:
        logging.error(f"ffprobe failed for {source_file}: {result_fps.stderr}")
        return {'framerate': 0.0, 'timecode': None}
    
    # Parse framerate (format: "25/1" or "30000/1001")
    fps_str = result_fps.stdout.strip()
    if '/' in fps_str:
        num, den = fps_str.split('/')
        framerate = float(num) / float(den)
    else:
        framerate = float(fps_str)
    
    # Get start timecode
    cmd_tc = [
        CONFIG['ffprobe_path'],
        '-v', 'error',
        '-show_entries', 'format_tags=timecode:stream_tags=timecode',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        source_file
    ]
    
    # Log the exact command for manual testing
    cmd_tc_str = ' '.join([f'"{arg}"' if ' ' in arg else arg for arg in cmd_tc])
    logging.info(f"Executing ffprobe for timecode: {cmd_tc_str}")
    
    result_tc = subprocess.run(cmd_tc, capture_output=True, text=True)
    logging.info(f"Timecode ffprobe stdout: '{result_tc.stdout.strip()}'")
    logging.info(f"Timecode ffprobe stderr: '{result_tc.stderr.strip()}'")
    logging.info(f"Timecode ffprobe returncode: {result_tc.returncode}")
    
    timecode = None
    if result_tc.returncode == 0 and result_tc.stdout.strip():
        # Get the first non-empty line (could be multiple timecodes from different streams)
        lines = [line.strip() for line in result_tc.stdout.strip().split('\n') if line.strip()]
        if lines:
            timecode = lines[0]
    
    return {'framerate': framerate, 'timecode': timecode}


def convert_timecode_framerate(timecode: str, source_fps: float, target_fps: float) -> str:
    """Convert timecode frames portion from source framerate to target framerate.
    
    Args:
        timecode: SMPTE timecode in format HH:MM:SS:FF
        source_fps: Source framerate
        target_fps: Target framerate
        
    Returns:
        Converted timecode string
    """
    if not timecode or abs(source_fps - target_fps) < 0.1:
        return timecode
    
    # Parse timecode
    parts = timecode.split(':')
    if len(parts) != 4:
        logging.warning(f"Invalid timecode format: {timecode}")
        return timecode
    
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        frames = int(parts[3])
        
        # Convert frames portion to target framerate
        # Calculate the time position within the second
        frame_time = frames / source_fps
        new_frames = int(frame_time * target_fps)
        
        # Construct new timecode
        new_timecode = f"{hours:02d}:{minutes:02d}:{seconds:02d}:{new_frames:02d}"
        logging.info(f"Converted timecode from {timecode} @ {source_fps}fps to {new_timecode} @ {target_fps}fps")
        return new_timecode
        
    except (ValueError, IndexError) as e:
        logging.error(f"Error converting timecode {timecode}: {e}")
        return timecode


def create_offspeed_copy(source_file: str, original_source_name: str, original_source_path: str, 
                         my_framerate: float, project_fps: float, report_branch_file: str) -> str:
    """Create offspeed copy of video file."""
    logging.info(f"Creating offspeed copy for {source_file} (source fps: {my_framerate}, project fps: {project_fps})")
    
    # Calculate itsscale factor
    itsscale = project_fps / my_framerate
    
    # Create output filename
    offspeed_file = os.path.join(original_source_path, f"{original_source_name}_offspeed_{my_framerate}.mov")
    
    cmd = [
        CONFIG['ffmpeg_path'],
        '-itsscale', str(itsscale),
        '-i', source_file,
        '-c:v', 'copy',
        '-c:a', 'pcm_s24le',
        '-map', '0:v:0',
        '-map', '0:a?',
        offspeed_file,
        '-y'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"ffmpeg offspeed conversion failed: {result.stderr}")
        raise RuntimeError(f"Offspeed conversion failed: {result.stderr}")
    
    # Update branch report with transcoded file
    with open(report_branch_file, 'r', encoding='utf-8') as f:
        branch_report = json.load(f)
    
    branch_report[0]['transcoded_file'] = offspeed_file
    
    with open(report_branch_file, 'w', encoding='utf-8') as f:
        json.dump(branch_report, f, indent=2)
    
    logging.info(f"Added transcoded_file to branch report: {offspeed_file}")
    
    return offspeed_file


def analyze_mxf_colors(source_file: str) -> str:
    """Analyze MXF file for color information."""
    return jobcontroller_ffastrans_api.analyze_mxf_colors(
        source_file, CONFIG['aaf_script_root'], get_python_executable, get_script_path
    )


def process_file(file_entry: Dict[str, Any], file_index: int, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single media file through the workflow."""
    source_file = file_entry.get('original_file')
    
    # Set up logging for this worker process with source file in log name
    setup_logging(source_file, config['run_timestamp'])
    
    logging.info(f"Processing file {file_index}: {source_file}")
    
    try:
        # Create branch report file path
        reports_dir = os.path.join(config['avid_aaf_output_dir'], 'reports')
        os.makedirs(reports_dir, exist_ok=True)
        report_branch_file = os.path.join(reports_dir, f"report_{file_index}.json")
        
        # Write initial branch report
        branch_report = [{'original_file': source_file}]
        with open(report_branch_file, 'w', encoding='utf-8') as f:
            json.dump(branch_report, f, indent=2)
        logging.info(f"Created branch report: {report_branch_file}")
        
        # Check file framerate and timecode
        media_info = get_media_info(source_file)
        my_framerate = media_info['framerate']
        source_timecode = media_info['timecode'] or "00:00:00:00"
        logging.info(f"File framerate: {my_framerate}")
        logging.info(f"Source timecode: {source_timecode}")
        
        # Convert timecode to project framerate if needed
        converted_timecode = convert_timecode_framerate(source_timecode, my_framerate, config['project_fps'])
        
        # Store timecode in branch report
        with open(report_branch_file, 'r', encoding='utf-8') as f:
            branch_report = json.load(f)
        branch_report[0]['source_timecode'] = source_timecode
        branch_report[0]['converted_timecode'] = converted_timecode
        branch_report[0]['source_framerate'] = my_framerate
        with open(report_branch_file, 'w', encoding='utf-8') as f:
            json.dump(branch_report, f, indent=2)
        
        if abs(my_framerate - config['project_fps']) > 0.1:
            # Need offspeed conversion
            original_source_path = os.path.dirname(source_file)
            original_source_name = os.path.splitext(os.path.basename(source_file))[0]
            source_file = create_offspeed_copy(
                source_file, original_source_name, original_source_path,
                my_framerate, config['project_fps'], report_branch_file
            )
        
        # Create encoding output directory
        encoding_output_dir = os.path.join(config['job_work_dir'], 'temp', config['run_timestamp'], str(file_index))
        logging.info(f"Creating encoding output directory: {encoding_output_dir}")
        os.makedirs(encoding_output_dir, exist_ok=True)
        
        # Submit encoding job to FFAStrans
        job_id = jobcontroller_ffastrans_api.submit_encoding_job(
            source_file, file_index, encoding_output_dir, converted_timecode, config, analyze_mxf_colors
        )
        
        # Wait for job completion
        output_value = jobcontroller_ffastrans_api.wait_for_job_completion(job_id, 's_output', config)
        
        # List files in encoding output directory
        encoded_files = []
        if os.path.exists(encoding_output_dir):
            for root, dirs, files in os.walk(encoding_output_dir):
                for file in files:
                    encoded_files.append(os.path.join(root, file))
        
        logging.info(f"Found {len(encoded_files)} encoded files in subfolders of {encoding_output_dir}")
        
        # Move files to final MXF output directory
        moved_files = []
        for encoded_file in encoded_files:
            dest_file = os.path.join(config['avid_mxf_output_dir'], os.path.basename(encoded_file))
            os.makedirs(config['avid_mxf_output_dir'], exist_ok=True)
            shutil.move(encoded_file, dest_file)
            moved_files.append(dest_file)
            logging.info(f"Moved {encoded_file} to {dest_file}")
        
        # Remove empty encoding output directory
        # if os.path.exists(encoding_output_dir) and not os.listdir(encoding_output_dir):
        #     os.rmdir(encoding_output_dir)
        #     logging.info(f"Removed empty encoding output directory: {encoding_output_dir}")
        
        # Add moved files to branch report
        if moved_files:
            with open(report_branch_file, 'r', encoding='utf-8') as f:
                branch_report = json.load(f)
            
            branch_report[0]['avid_files'] = moved_files
            
            with open(report_branch_file, 'w', encoding='utf-8') as f:
                json.dump(branch_report, f, indent=2)
            
            logging.info(f"Added {len(moved_files)} avid_files to branch report")
        
        return {'status': 'success', 'file': source_file, 'encoded_count': len(encoded_files)}
        
    except Exception as e:
        logging.exception(f"Error processing file {source_file}: {e}")
        return {'status': 'error', 'file': source_file, 'error': str(e)}


def merge_branch_reports(full_report: str, branch_report_dir: str):
    """Merge all branch reports back into the full report."""
    logging.info("Merging branch reports")
    
    python_exe = get_python_executable(CONFIG['aaf_script_root'])
    merge_script = get_script_path('merge_branch_reports.py')
    
    cmd = [
        python_exe,
        merge_script,
        '--branch_report_dir', branch_report_dir,
        '--full_report', full_report
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"merge_branch_reports.py failed: {result.stderr}")
        raise RuntimeError(f"Report merging failed: {result.stderr}")
    
    logging.info("Branch reports merged successfully")


def create_aaf(report_file: str):
    """Create AAF file from processed media."""
    logging.info("Creating AAF file")
    
    python_exe = get_python_executable(CONFIG['aaf_script_root'])
    createaaf_script = get_script_path('createaaf.py')
    
    # Generate output filename with timestamp
    timestamp = time.strftime("%Y_%m_%d_%H_%M_%S")
    output_name = f"output_{timestamp}.aaf"
    
    cmd = [
        python_exe,
        createaaf_script,
        '--odir', CONFIG['avid_aaf_output_dir'],
        '--oname', output_name,
        '--lut', 'auto',
        '--skipcheck', '1',
        '--allinone', '1',
        '--debug', '1',
        '--remove-success-report', str(int(CONFIG['remove_success_reports'])),
        '--report', report_file,
        CONFIG['avid_mxf_output_dir']
    ]
    
    # Log the exact command for manual testing
    cmd_str = ' '.join([f'"{arg}"' if ' ' in arg else arg for arg in cmd])
    logging.info(f"Executing createaaf.py: {cmd_str}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"createaaf.py failed: {result.stderr}")
        raise RuntimeError(f"AAF creation failed: {result.stderr}")
    
    logging.info(f"createaaf.py output: {result.stdout}")
    logging.info(f"AAF created successfully: {output_name}")


def main():
    parser = argparse.ArgumentParser(description="FFAStrans Job Controller")
    parser.add_argument("--starting_dir", help="Directory to search for media files", default="C:\\temp\\bearded\\01_Rushes_1sec\\Canon\\2026_01_01\\normal")
    parser.add_argument("--aaf_script_root", required=True, help="Root directory of AAF scripts")
    parser.add_argument("--avid_aaf_output_dir", required=True, help="Output directory for AAF files")
    parser.add_argument("--avid_mxf_output_dir", required=True, help="Output directory for MXF files")
    parser.add_argument("--job_work_dir", required=True, help="Working directory for job files")
    parser.add_argument("--report_file", required=True, help="Path to main report JSON file")
    parser.add_argument("--ffastrans_wf_guid", help="FFAStrans workflow GUID", default="20251227-0053-2550-3978-f4b59a13502d")
    parser.add_argument("--ffastrans_start_proc", help="FFAStrans workflow start processor",default="")
    parser.add_argument("--concurrent_processes", type=int, default=40, help="Number of concurrent file processes")
    parser.add_argument("--project_fps", type=float, default=25, help="Project framerate")
    parser.add_argument("--http_max_retries", type=int, default=100, help="Maximum HTTP retries for job status polling")
    parser.add_argument("--http_poll_interval", type=int, default=5, help="HTTP polling interval in seconds for job status")
    parser.add_argument("--remove_success_reports", action='store_true', help="Remove successful reports")
    
    args = parser.parse_args()
    
    # Update configuration
    CONFIG.update({
        'starting_dir': args.starting_dir,
        'aaf_script_root': args.aaf_script_root,
        'avid_aaf_output_dir': args.avid_aaf_output_dir,
        'avid_mxf_output_dir': args.avid_mxf_output_dir,
        'job_work_dir': args.job_work_dir,
        'report_file': args.report_file,
        'concurrent_file_processes': args.concurrent_processes,
        'project_fps': args.project_fps,
        'http_max_retries': args.http_max_retries,
        'http_poll_interval': args.http_poll_interval,
        'remove_success_reports': args.remove_success_reports,
        'ffastrans_encoding_wf_guid': args.ffastrans_wf_guid,
        'ffastrans_encoding_wf_start_proc': args.ffastrans_start_proc,
    })
    
    try:
        start_time = time.time()
        run_timestamp = time.strftime("%Y_%m_%d_%H_%M_%S")
        CONFIG['run_timestamp'] = run_timestamp
        
        # Modify report_file to include timestamp
        base_report = args.report_file
        report_name = os.path.basename(base_report)
        report_dir = os.path.dirname(base_report)
        name, ext = os.path.splitext(report_name)
        new_name = f"{name}_{run_timestamp}{ext}"
        CONFIG['report_file'] = os.path.join(report_dir, new_name)
        
        # Step 1: Find all media files
        files = find_files(CONFIG['starting_dir'], CONFIG['report_file'])
        
        if not files:
            logging.warning("No files found to process")
            return
        
        # Step 2: Process files in parallel
        results = []
        with ProcessPoolExecutor(max_workers=CONFIG['concurrent_file_processes']) as executor:
            futures = {
                executor.submit(process_file, file_entry, idx, CONFIG): idx 
                for idx, file_entry in enumerate(files)
            }
            
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    logging.info(f"File {idx} completed: {result}")
                except Exception as e:
                    logging.exception(f"File {idx} processing failed: {e}")
                    results.append({'status': 'error', 'file': f'index_{idx}', 'error': str(e)})
        
        # Step 3: Merge branch reports
        branch_report_dir = os.path.join(CONFIG['avid_aaf_output_dir'], 'reports')
        merge_branch_reports(CONFIG['report_file'], branch_report_dir)
        
        # Step 4: Create AAF
        create_aaf(CONFIG['report_file'])
        
        logging.info("Job controller completed successfully")
        
        # Summary
        success_count = sum(1 for r in results if r['status'] == 'success')
        error_count = len(results) - success_count
        duration = time.time() - start_time
        logging.info(f"Summary: {success_count} successful, {error_count} errors, duration: {duration:.2f}s")
        
        sys.exit(0 if error_count == 0 else 1)
        
    except Exception as e:
        logging.exception(f"Job controller failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 
