#!/usr/bin/env python3
"""
FFAStrans API interaction module
Handles job submission and monitoring for FFAStrans workflows
"""

import os
import time
import logging
import uuid
import requests
import urllib.parse
from typing import Dict, Any, Optional


def analyze_mxf_colors(source_file: str, aaf_script_root: str, get_python_executable_func, get_script_path_func) -> str:
    """Analyze MXF file for color information."""
    python_exe = get_python_executable_func(aaf_script_root)
    analyze_script = get_script_path_func('analyze_mxf_colors.py')
    
    import subprocess
    cmd = [python_exe, analyze_script, source_file]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logging.warning(f"analyze_mxf_colors.py failed: {result.stderr}")
        return ""
    
    # Remove newlines
    return result.stdout.strip().replace('\r\n', '').replace('\n', '')


def submit_encoding_job(source_file: str, file_index: int, encoding_output_dir: str, converted_timecode: str, config: Dict[str, Any], analyze_mxf_colors_func) -> str:
    """Submit encoding job to FFAStrans API."""
    logging.info(f"Submitting encoding job for {source_file}")
    
    # Prepare file-specific variables
    avid_name = os.path.splitext(os.path.basename(source_file))[0]
    original_source_name = avid_name
    
    # Get BMX colors
    bmx_colors = analyze_mxf_colors_func(source_file)
    
    # Create import descriptor (URL encoded path)
    import_path = source_file.replace('\\', '/')
    if not import_path.startswith('/'):
        import_path = '/' + import_path
    import_path = urllib.parse.quote(import_path, safe='/')
    avid_meta_import_descr = f"file:{import_path}"
    
    # Generate unique filename GUID
    avid_filename_guid = str(uuid.uuid4())
    
    # Prepare job JSON
    job_data = {
        'wf_id': config['ffastrans_encoding_wf_guid'],
        'inputfile': source_file,
        'start_proc': config['ffastrans_encoding_wf_start_proc'],
        'priority': 3,
        'variables': [
            {'name': 's_timecode', 'data': converted_timecode},
            {'name': 's_project', 'data': 'ffastrans'},
            {'name': 's_clip', 'data': avid_name},
            {'name': 's_import', 'data': f"{avid_meta_import_descr}"},
            {'name': 's_bmx_colors', 'data': f"{bmx_colors}"},
            {'name': 's_camroll', 'data': f""},
            {'name': 's_o', 'data': os.path.join(encoding_output_dir, f"{original_source_name}_{avid_filename_guid}")}
        ]
    }
    
    # POST to FFAStrans API
    response = requests.post(config['ffastrans_api_url'], json=job_data)
    
    if response.status_code != 200:
        logging.error(f"FFAStrans API error: {response.status_code} - {response.text}")
        raise RuntimeError(f"Failed to submit job: {response.text}")
    
    result = response.json()
    job_id = result.get('job_id')
    
    logging.info(f"Job submitted successfully: {job_id}")
    return job_id


def wait_for_job_completion(job_id: str, variable_to_extract: str = 's_output', config: Dict[str, Any] = None) -> Optional[str]:
    """Wait for FFAStrans job to complete and extract variable."""
    logging.info(f"Waiting for job {job_id} to complete")
    
    api_url = f"{config['ffastrans_api_getjobdetails_url']}?jobid={job_id}"
    poll_interval = config.get('http_poll_interval', 1)
    max_retries = config.get('http_max_retries', 10)  # Consecutive HTTP failures before giving up
    
    consecutive_failures = 0
    start_time = time.time()
    
    # Initial delay to allow job to be indexed in the system
    time.sleep(2)
    
    while True:
        elapsed = time.time() - start_time
        
        try:
            response = requests.get(api_url, timeout=10)
            
            if response.status_code != 200:
                consecutive_failures += 1
                logging.warning(f"Failed to get job details (attempt {consecutive_failures}/{max_retries}): {response.status_code}, URL: {api_url}")
                logging.warning(f"Response body: {response.text}")
                
                if consecutive_failures >= max_retries:
                    logging.error(f"Max consecutive HTTP failures ({max_retries}) exceeded for job {job_id}")
                    return None
                
                time.sleep(poll_interval)
                continue
            
            # Reset consecutive failure counter on successful HTTP request
            consecutive_failures = 0
            
            job_details = response.json()
            status = job_details.get('status')
            
            if status == 'finished':
                logging.info(f"Job {job_id} completed successfully (elapsed: {elapsed:.1f}s)")
                
                # Extract variable from workflow object
                wf_object = job_details.get('wf_object', {})
                nodes = wf_object.get('nodes', [])
                
                for node in nodes:
                    properties = node.get('properties', {})
                    variables = properties.get('variables', [])
                    
                    for var in variables:
                        if var.get('name') == variable_to_extract:
                            return var.get('data')
                
                logging.warning(f"Variable {variable_to_extract} not found in job output")
                return None
            
            elif status in ['error', 'failed']:
                logging.error(f"Job {job_id} failed with status: {status}")
                return None
            
            # Job still running
            logging.debug(f"Job {job_id} status: {status} (elapsed: {elapsed:.1f}s)")
            time.sleep(poll_interval)
            
        except requests.exceptions.Timeout:
            consecutive_failures += 1
            logging.warning(f"HTTP timeout checking job status (attempt {consecutive_failures}/{max_retries}), URL: {api_url}")
            
            if consecutive_failures >= max_retries:
                logging.error(f"Max consecutive HTTP failures ({max_retries}) exceeded for job {job_id}")
                return None
            
            time.sleep(poll_interval)
            
        except Exception as e:
            consecutive_failures += 1
            logging.exception(f"Error checking job status (attempt {consecutive_failures}/{max_retries}) for {job_id}, URL: {api_url}: {e}")
            
            if consecutive_failures >= max_retries:
                logging.error(f"Max consecutive failures ({max_retries}) exceeded for job {job_id}")
                return None
            
            time.sleep(poll_interval)
