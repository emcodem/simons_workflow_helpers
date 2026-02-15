"""
FFAStrans Job Launcher - Launch and monitor job workflows

Usage instructions:
- Use logging module for logging, do not use symbols in logging
- Ensure error messages are user-friendly but also useful for debugging
- Log all HTTP errors and retries extensively

Input parameters:
- wf_id: Workflow ID
- input_file: Single file path or path to file containing JSON array
- start_proc: Starting processor
- priority: Job priority
- variable: Variable definitions (name and data), can occur multiple times
- webui_url: Base URL of FFAStrans WebUI
- variables_from_job_id: Optional, copy variables from existing job
- poll_frequency: Optional, default 60 seconds

Flow:
1. Call webui_url/tickets and filter for variables_from_job_id
2. Prepare job data (handle JSON arrays for multiple jobs)
3. POST to webui_url/jobs to launch
4. Poll webui_url/jobs?jobid=... until completion
5. Retry up to 30 minutes with 60 second intervals
6. Report summary with execution times
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def setup_logging() -> logging.Logger:
    """Configure logging without symbols."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(message)s'
    )
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger


def _parse_datetime(date_string: Optional[str]) -> datetime:
    """Parse datetime string ensuring timezone awareness."""
    if not date_string:
        return datetime.now(timezone.utc)
    
    try:
        dt = datetime.fromisoformat(date_string)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def create_session_with_retries(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Tuple = (500, 502, 503, 504)
) -> requests.Session:
    """Create a requests session with retry strategy."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=['GET', 'POST']
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def load_input_file(input_file: str, logger: logging.Logger) -> List[str]:
    """
    Load input file(s).
    If input_file is a JSON array file, return the array.
    Otherwise, return single file as list.
    """
    try:
        file_path = Path(input_file)
        if not file_path.exists():
            logger.error(f'Input file not found: {input_file}')
            return []

        with open(file_path, 'r') as f:
            content = f.read().strip()

        if content.startswith('['):
            data = json.loads(content)
            if isinstance(data, list):
                logger.info(f'Loaded {len(data)} files from JSON array')
                return data
            else:
                logger.error('JSON file does not contain an array')
                return []
        else:
            return [input_file]

    except json.JSONDecodeError as e:
        logger.error(f'Failed to parse JSON from input file: {e}')
        return []
    except Exception as e:
        logger.error(f'Error reading input file: {e}')
        return []


def fetch_variables_from_job(
    webui_url: str,
    job_id: str,
    session: requests.Session,
    logger: logging.Logger
) -> Optional[List[Dict[str, str]]]:
    """Fetch variables from a running job via tickets endpoint."""
    try:
        tickets_url = f'{webui_url}/tickets'
        logger.info(f'Fetching tickets from {tickets_url}')

        response = session.get(tickets_url, timeout=10)
        response.raise_for_status()

        tickets = response.json()
        running_jobs = tickets.get('tickets', {}).get('running', [])

        for job in running_jobs:
            if job.get('job_id') == job_id:
                variables = job.get('variables', [])
                logger.info(
                    f'Found {len(variables)} variables from job {job_id}'
                )
                return variables

        logger.warning(f'Job {job_id} not found in running jobs')
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f'HTTP error fetching tickets: {e}')
        return None
    except json.JSONDecodeError as e:
        logger.error(f'Invalid JSON in tickets response: {e}')
        return None
    except Exception as e:
        logger.error(f'Unexpected error fetching variables from job: {e}')
        return None


def prepare_variables(
    variables_list: List[Tuple[str, str]],
    webui_url: str,
    variables_from_job_id: Optional[str],
    session: requests.Session,
    logger: logging.Logger
) -> List[Dict[str, str]]:
    """Prepare variables for job submission."""
    prepared_vars = []

    if variables_from_job_id:
        fetched_vars = fetch_variables_from_job(
            webui_url,
            variables_from_job_id,
            session,
            logger
        )
        if fetched_vars:
            prepared_vars.extend(fetched_vars)
            logger.info(f'Added {len(fetched_vars)} variables from job')

    for name, data in variables_list:
        prepared_vars.append({'name': name, 'data': data})
        logger.debug(f'Added variable: {name}')

    return prepared_vars


def launch_jobs(
    wf_id: str,
    input_files: List[str],
    start_proc: str,
    priority: str,
    variables: List[Dict[str, str]],
    webui_url: str,
    session: requests.Session,
    logger: logging.Logger
) -> List[Tuple[str, datetime, str]]:
    """
    Launch jobs and return list of (job_id, launch_time, input_file) tuples.
    """
    launched_jobs = []

    for idx, input_file in enumerate(input_files):
        try:
            job_data = {
                'wf_id': wf_id,
                'inputfile': input_file,
                'start_proc': start_proc,
                'priority': priority,
                'variables': variables
            }

            jobs_url = f'{webui_url}/jobs'
            logger.info(
                f'Launching job {idx + 1}/{len(input_files)}: {input_file}'
            )

            response = session.post(
                jobs_url,
                json=job_data,
                timeout=10
            )
            response.raise_for_status()

            result = response.json()
            job_id = result.get('job_id')

            if job_id:
                launched_jobs.append((job_id, datetime.now(timezone.utc), input_file))
                logger.info(f'Job launched successfully: {job_id}')
            else:
                logger.error(f'No job_id in response for {input_file}')

            if idx < len(input_files) - 1:
                time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logger.error(f'HTTP error launching job {idx + 1}: {e}')
        except json.JSONDecodeError as e:
            logger.error(f'Invalid JSON in job response {idx + 1}: {e}')
        except Exception as e:
            logger.error(f'Unexpected error launching job {idx + 1}: {e}')

    return launched_jobs


def poll_job_completion(
    webui_url: str,
    job_id: str,
    session: requests.Session,
    logger: logging.Logger,
    input_file: str = 'Unknown',
    poll_timeout: int = 1800
) -> Optional[Dict[str, Any]]:
    """
    Poll job until completion.
    Retry every 60 seconds, up to 30 minutes.
    Returns job entry from history on success, None on failure.
    """
    max_retries = poll_timeout // 60
    retry_count = 0

    while retry_count < max_retries:
        try:
            jobs_url = f'{webui_url}/jobs?jobid={job_id}'
            response = session.get(jobs_url, timeout=10)
            response.raise_for_status()

            result = response.json()
            history = result.get('history', [])

            for job_entry in history:
                if job_entry.get('job_id') == job_id:
                    state = job_entry.get('state')
                    if state is not None:
                        return job_entry

            retry_count += 1
            logger.debug(
                f'{input_file} not completed yet, retry {retry_count}/{max_retries}'
            )
            time.sleep(60)

        except requests.exceptions.RequestException as e:
            logger.warning(
                f'HTTP error polling {input_file}, retry {retry_count + 1}: {e}'
            )
            retry_count += 1
            time.sleep(60)
        except json.JSONDecodeError as e:
            logger.warning(
                f'Invalid JSON in poll response for {input_file}, retry {retry_count + 1}: {e}'
            )
            retry_count += 1
            time.sleep(60)
        except Exception as e:
            logger.warning(
                f'Unexpected error polling {input_file}, retry {retry_count + 1}: {e}'
            )
            retry_count += 1
            time.sleep(60)

    logger.error(f'{input_file} did not complete within timeout period')
    return None


def monitor_jobs(
    webui_url: str,
    launched_jobs: List[Tuple[str, datetime, str]],
    session: requests.Session,
    logger: logging.Logger,
    poll_frequency: int = 60
) -> Dict[str, Dict[str, Any]]:
    """Monitor all launched jobs and collect results."""
    results = {}
    start_time = datetime.now()

    logger.info(f'Starting to monitor {len(launched_jobs)} jobs')

    for job_id, launch_time, input_file in launched_jobs:
        logger.info(f'Polling for job completion: {input_file}')

        job_result = poll_job_completion(
            webui_url,
            job_id,
            session,
            logger,
            input_file,
            poll_timeout=1800
        )

        if job_result:
            state = job_result.get('state')
            result_str = job_result.get('result', 'Unknown')

            if state == 1:
                logger.info(
                    f'{input_file} completed successfully: {result_str}'
                )
            else:
                logger.error(
                    f'{input_file} failed with state {state}: {result_str}'
                )

            results[job_id] = {
                'input_file': input_file,
                'launch_time': launch_time,
                'completion_time': _parse_datetime(job_result.get('end_time')),
                'state': state,
                'result': result_str,
                'status': 'success' if state == 1 else 'failed'
            }
        else:
            logger.error(f'Failed to get result for {input_file}')
            results[job_id] = {
                'input_file': input_file,
                'launch_time': launch_time,
                'completion_time': datetime.now(timezone.utc),
                'state': None,
                'result': 'Timeout or error',
                'status': 'failed'
            }

    return results


def write_summary(
    results: Dict[str, Dict[str, Any]],
    logger: logging.Logger
) -> None:
    """Write summary with job execution times."""
    logger.info('========== JOB EXECUTION SUMMARY ==========')

    total_jobs = len(results)
    successful_jobs = sum(
        1 for r in results.values() if r['status'] == 'success'
    )
    failed_jobs = total_jobs - successful_jobs

    logger.info(f'Total jobs: {total_jobs}')
    logger.info(f'Successful: {successful_jobs}')
    logger.info(f'Failed: {failed_jobs}')

    for job_id, job_info in results.items():
        input_file = job_info.get('input_file', 'Unknown')
        launch_time = job_info['launch_time']
        completion_time = job_info['completion_time']
        duration = completion_time - launch_time

        status = job_info['status']
        result = job_info['result']

        minutes = int(duration.total_seconds() // 60)
        seconds = int(duration.total_seconds() % 60)

        logger.info(
            f'{input_file}: {status} - {result} - Duration: {minutes}m {seconds}s'
        )

    logger.info('==========================================')


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Launch FFAStrans workflow jobs and monitor completion'
    )

    parser.add_argument(
        '--wf_id',
        required=True,
        help='Workflow ID'
    )
    parser.add_argument(
        '--input_file',
        required=True,
        help='Input file or JSON array file path'
    )
    parser.add_argument(
        '--start_proc',
        default='',
        help='Starting processor'
    )
    parser.add_argument(
        '--priority',
        default='',
        help='Job priority'
    )
    parser.add_argument(
        '--variable',
        action='append',
        nargs=2,
        metavar=('NAME', 'DATA'),
        help='Variable definition (name and data), can occur multiple times'
    )
    parser.add_argument(
        '--webui_url',
        required=True,
        help='Base URL of FFAStrans WebUI'
    )
    parser.add_argument(
        '--variables_from_job_id',
        help='Copy variables from existing job'
    )
    parser.add_argument(
        '--poll_frequency',
        type=int,
        default=60,
        help='Polling frequency in seconds (default: 60)'
    )

    args = parser.parse_args()

    logger = setup_logging()
    logger.info('Starting job launcher')

    session = create_session_with_retries()

    try:
        input_files = load_input_file(args.input_file, logger)
        if not input_files:
            logger.error('No input files to process')
            return 1

        variables_list = args.variable or []
        variables = prepare_variables(
            variables_list,
            args.webui_url,
            args.variables_from_job_id,
            session,
            logger
        )

        launched_jobs = launch_jobs(
            args.wf_id,
            input_files,
            args.start_proc,
            args.priority,
            variables,
            args.webui_url,
            session,
            logger
        )

        if not launched_jobs:
            logger.error('No jobs were successfully launched')
            return 1

        logger.info(f'Successfully launched {len(launched_jobs)} jobs')

        results = monitor_jobs(
            args.webui_url,
            launched_jobs,
            session,
            logger,
            args.poll_frequency
        )

        write_summary(results, logger)

        all_successful = all(
            r['status'] == 'success' for r in results.values()
        )

        return 0 if all_successful else 1

    except Exception as e:
        logger.error(f'Unexpected error in main: {e}', exc_info=True)
        return 1
    finally:
        session.close()


if __name__ == '__main__':
    sys.exit(main())