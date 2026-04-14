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
- disable_polling: Optional, skip job monitoring after launch
- json_escape_input_file: Optional, double-escape input file before JSON submission

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

    handler = logging.StreamHandler(sys.stderr)
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
    """Fetch variables from a running job via tickets endpoint. Retry for 60 seconds if not found."""
    tickets_url = f'{webui_url}/tickets'
    start_time = time.time()
    retry_timeout = 10  # 10 seconds
    
    while (time.time() - start_time) < retry_timeout:
        try:
            logger.info(f'Fetching tickets from {tickets_url}')

            response = session.get(tickets_url, timeout=10)
            response.raise_for_status()

            tickets = response.json()
            logger.debug(f'Tickets response: {tickets}')
            running_jobs = tickets.get('tickets', {}).get('running', [])

            for job in running_jobs:
                if job.get('job_id') == job_id:
                    variables = job.get('variables', [])
                    logger.info(
                        f'Found {len(variables)} variables from job {job_id}'
                    )
                    return variables

            elapsed = int(time.time() - start_time)
            logger.info(f'Job {job_id} not found in running jobs, retrying... ({elapsed}s elapsed)')
            time.sleep(2)

        except requests.exceptions.RequestException as e:
            logger.warning(f'HTTP error fetching tickets: {e}, retrying...')
            time.sleep(2)
        except json.JSONDecodeError as e:
            logger.warning(f'Invalid JSON in tickets response: {e}, retrying...')
            time.sleep(2)
        except Exception as e:
            logger.warning(f'Unexpected error fetching variables from job: {e}, retrying...')
            time.sleep(2)
    
    logger.error(f'Job {job_id} not found in running jobs after {retry_timeout} seconds')
    raise Exception(f'Failed to fetch variables from job {job_id}')


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
        prepared_vars.extend(fetched_vars)
        logger.info(f'Added {len(fetched_vars)} variables from job')

    for name, data in variables_list:
        prepared_vars.append({'name': name, 'data': data})
        logger.debug(f'Added variable: {name}')

    return prepared_vars


def _json_escape_string(value: str) -> str:
    """Return a JSON-escaped string value without the surrounding quotes."""
    return json.dumps(value)


def launch_jobs(
    wf_id: str,
    input_files: List[str],
    start_proc: str,
    priority: str,
    variables: List[Dict[str, str]],
    webui_url: str,
    json_escape_input_file: bool,
    batch_submit: bool,
    session: requests.Session,
    logger: logging.Logger
) -> List[Tuple[str, datetime, str]]:
    """
    Launch jobs and return list of (job_id, launch_time, input_file) tuples.
    If batch_submit is True, submit all jobs in a single request as an array.
    """
    launched_jobs = []
    jobs_url = f'{webui_url}/jobs'

    if batch_submit:
        return _launch_jobs_batch(
            wf_id, input_files, start_proc, priority, variables,
            jobs_url, json_escape_input_file, session, logger
        )

    for idx, input_file in enumerate(input_files):
        try:
            job_input_file = (
                _json_escape_string(input_file)
                if json_escape_input_file
                else input_file
            )
            job_data = {
                'wf_id': wf_id,
                'inputfile': job_input_file,
                'start_proc': start_proc,
                'priority': priority,
                'variables': variables
            }

            logger.info(
                f'Launching job {idx + 1}/{len(input_files)}: {input_file}'
            )

            logger.info("POST Job data: %s", job_data)
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


def _launch_jobs_batch(
    wf_id: str,
    input_files: List[str],
    start_proc: str,
    priority: str,
    variables: List[Dict[str, str]],
    jobs_url: str,
    json_escape_input_file: bool,
    session: requests.Session,
    logger: logging.Logger
) -> List[Tuple[str, datetime, str]]:
    """
    Submit all jobs in a single request as an array.
    Returns list of (job_id, launch_time, input_file) tuples.
    """
    launched_jobs = []

    jobs_array = []
    for input_file in input_files:
        job_input_file = (
            _json_escape_string(input_file)
            if json_escape_input_file
            else input_file
        )
        jobs_array.append({
            'wf_id': wf_id,
            'inputfile': job_input_file,
            'start_proc': start_proc,
            'priority': priority,
            'variables': variables
        })

    logger.info(f'Batch submitting {len(jobs_array)} jobs in single request')
    logger.info("POST Jobs array: %s", jobs_array)

    try:
        response = session.post(
            jobs_url,
            json=jobs_array,
            timeout=30
        )
        response.raise_for_status()

        result = response.json()
        launch_time = datetime.now(timezone.utc)

        # Handle array response (list of job results)
        if isinstance(result, list):
            for idx, job_result in enumerate(result):
                job_id = job_result.get('job_id')
                input_file = input_files[idx] if idx < len(input_files) else 'Unknown'
                if job_id:
                    launched_jobs.append((job_id, launch_time, input_file))
                    logger.info(f'Job {idx + 1} launched successfully: {job_id}')
                else:
                    logger.error(f'No job_id in response for job {idx + 1}: {input_file}')
        # Handle single response with job_ids array
        elif isinstance(result, dict):
            job_ids = result.get('job_ids', [])
            if job_ids:
                for idx, job_id in enumerate(job_ids):
                    input_file = input_files[idx] if idx < len(input_files) else 'Unknown'
                    launched_jobs.append((job_id, launch_time, input_file))
                    logger.info(f'Job {idx + 1} launched successfully: {job_id}')
            # Fallback: single job_id in response
            elif result.get('job_id'):
                job_id = result.get('job_id')
                launched_jobs.append((job_id, launch_time, input_files[0]))
                logger.info(f'Job launched successfully: {job_id}')
            else:
                logger.error('No job_id(s) found in batch response')
        else:
            logger.error(f'Unexpected response format: {type(result)}')

    except requests.exceptions.RequestException as e:
        logger.error(f'HTTP error during batch job submission: {e}')
    except json.JSONDecodeError as e:
        logger.error(f'Invalid JSON in batch job response: {e}')
    except Exception as e:
        logger.error(f'Unexpected error during batch job submission: {e}')

    return launched_jobs



def _job_id_in_tickets(job_id: str, ticket_list: List[str]) -> bool:
    """Check if a job_id appears in any ticket filename string."""
    for ticket_name in ticket_list:
        if job_id in ticket_name:
            return True
    return False


def fetch_job_result(
    webui_url: str,
    job_id: str,
    session: requests.Session,
    logger: logging.Logger,
    retry_count: int = 10,
    retry_delay: int = 5
) -> Optional[Dict[str, Any]]:
    """Fetch final job result from /jobs?jobid=... with retries."""
    jobs_url = f'{webui_url}/jobs?jobid={job_id}'

    for attempt in range(retry_count):
        try:
            response = session.get(jobs_url, timeout=10)
            response.raise_for_status()
            result = response.json()

            for job_entry in result.get('history', []):
                if job_entry.get('job_id') == job_id:
                    state = job_entry.get('state')
                    if state is not None:
                        return job_entry

            logger.debug(
                f'Job {job_id} not yet in history, '
                f'retry {attempt + 1}/{retry_count}'
            )
        except Exception as e:
            logger.warning(
                f'Error fetching result for {job_id}: {e}, '
                f'retry {attempt + 1}/{retry_count}'
            )

        time.sleep(retry_delay)

    logger.error(f'Could not fetch result for job {job_id} after {retry_count} retries')
    return None


def monitor_jobs(
    webui_url: str,
    launched_jobs: List[Tuple[str, datetime, str]],
    session: requests.Session,
    logger: logging.Logger,
    poll_frequency: int = 60
) -> Dict[str, Dict[str, Any]]:
    """
    Monitor all launched jobs and collect results.

    Uses lightweight /tickets?nodetails=true to track job lifecycle:
    - A job must be seen as active (running/queued) at least once before
      its disappearance counts as "finished". This prevents API errors
      from being misinterpreted as job completion.
    - As soon as a previously-seen job disappears from tickets, its final
      result is fetched immediately from /jobs?jobid=... (no waiting for
      all jobs to finish first).
    - On API errors, retries continue for up to 1 hour before giving up.
      An error never marks a job as finished.
    """
    results = {}
    tickets_url = f'{webui_url}/tickets?nodetails=true'

    # Build lookup and tracking state
    job_lookup = {}  # job_id -> (launch_time, input_file)
    for job_id, launch_time, input_file in launched_jobs:
        job_lookup[job_id] = (launch_time, input_file)

    pending = set(job_lookup.keys())   # not yet seen in tickets
    active = {}                        # job_id -> first_seen_time (seen in tickets)
    ticket_times = {}                  # job_id -> {first_seen, last_seen, gone_time}

    logger.info(f'Starting to monitor {len(launched_jobs)} jobs')

    api_error_start = None  # track consecutive API error window
    API_ERROR_TIMEOUT = 3600  # 1 hour

    while pending or active:
        try:
            response = session.get(tickets_url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Successful call - reset error window
            api_error_start = None

            running = data.get('tickets', {}).get('running', [])
            queued = data.get('tickets', {}).get('queued', [])
            all_tickets = running + queued
            now = time.time()

            # Check pending jobs: have they appeared in tickets yet?
            newly_seen = set()
            for jid in pending:
                if _job_id_in_tickets(jid, all_tickets):
                    newly_seen.add(jid)
                    active[jid] = now
                    ticket_times[jid] = {'first_seen': now, 'last_seen': now}
                    _, input_file = job_lookup[jid]
                    logger.info(f'Job {jid} appeared in tickets: {input_file}')

            pending -= newly_seen

            # Check active jobs: have any disappeared from tickets?
            just_gone = set()
            for jid in list(active.keys()):
                if _job_id_in_tickets(jid, all_tickets):
                    ticket_times[jid]['last_seen'] = now
                else:
                    just_gone.add(jid)
                    ticket_times[jid]['gone_time'] = now
                    _, input_file = job_lookup[jid]
                    active_duration = now - ticket_times[jid]['first_seen']
                    logger.info(
                        f'Job {jid} left tickets after {int(active_duration)}s: {input_file}'
                    )

            # Fetch results immediately for jobs that just left tickets
            for jid in just_gone:
                del active[jid]
                launch_time, input_file = job_lookup[jid]
                logger.info(f'Fetching final result for: {input_file}')

                job_result = fetch_job_result(
                    webui_url, jid, session, logger
                )

                if job_result:
                    state = job_result.get('state')
                    result_str = job_result.get('result', 'Unknown')
                    if state == 1:
                        logger.info(f'{input_file} completed successfully: {result_str}')
                    else:
                        logger.error(f'{input_file} failed with state {state}: {result_str}')
                    results[jid] = {
                        'input_file': input_file,
                        'launch_time': launch_time,
                        'completion_time': _parse_datetime(job_result.get('end_time')),
                        'state': state,
                        'result': result_str,
                        'status': 'success' if state == 1 else 'failed',
                        'ticket_times': ticket_times.get(jid)
                    }
                else:
                    logger.error(f'Failed to get result for {input_file}')
                    results[jid] = {
                        'input_file': input_file,
                        'launch_time': launch_time,
                        'completion_time': datetime.now(timezone.utc),
                        'state': None,
                        'result': 'Timeout or error',
                        'status': 'failed',
                        'ticket_times': ticket_times.get(jid)
                    }

            if not pending and not active:
                logger.info('All jobs have completed')
                break

            status_parts = []
            if pending:
                status_parts.append(f'{len(pending)} waiting to appear')
            if active:
                status_parts.append(f'{len(active)} active')
            logger.debug(f'Job status: {", ".join(status_parts)}')

        except Exception as e:
            now = time.time()
            if api_error_start is None:
                api_error_start = now
            error_duration = int(now - api_error_start)

            if error_duration >= API_ERROR_TIMEOUT:
                logger.error(
                    f'Tickets API unreachable for {error_duration}s (limit {API_ERROR_TIMEOUT}s), '
                    f'aborting monitor. Error: {e}'
                )
                # Mark all remaining jobs as failed
                for jid in list(pending) + list(active.keys()):
                    launch_time, input_file = job_lookup[jid]
                    results[jid] = {
                        'input_file': input_file,
                        'launch_time': launch_time,
                        'completion_time': datetime.now(timezone.utc),
                        'state': None,
                        'result': f'API unreachable for {error_duration}s',
                        'status': 'failed',
                        'ticket_times': ticket_times.get(jid)
                    }
                break

            logger.warning(
                f'Tickets API error ({error_duration}s of {API_ERROR_TIMEOUT}s tolerance): {e}'
            )

        time.sleep(poll_frequency)

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
    print(f'Total jobs: {total_jobs}', file=sys.stderr)
    print(f'Successful: {successful_jobs}', file=sys.stderr)
    print(f'Failed: {failed_jobs}', file=sys.stderr)

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

    # Ticket activity report
    logger.info('---------- TICKET ACTIVITY REPORT ----------')
    for job_id, job_info in results.items():
        input_file = job_info.get('input_file', 'Unknown')
        tt = job_info.get('ticket_times')
        if tt and tt.get('first_seen') and tt.get('gone_time'):
            active_secs = int(tt['gone_time'] - tt['first_seen'])
            active_min = active_secs // 60
            active_sec = active_secs % 60
            logger.info(
                f'{input_file}: active in tickets for {active_min}m {active_sec}s'
            )
        elif tt and tt.get('first_seen'):
            logger.info(f'{input_file}: appeared in tickets but completion time unknown')
        else:
            logger.info(f'{input_file}: never seen in tickets')

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
        default=None,
        help='Polling frequency in seconds (default: auto-calculated or 60)'
    )
    parser.add_argument(
        '--disable_polling',
        action='store_true',
        help='Launch jobs without waiting for completion'
    )
    parser.add_argument(
        '--json_escape_input_file',
        action='store_true',
        help='Double-escape input_file before JSON submission'
    )
    parser.add_argument(
        '--batch_submit',
        action='store_true',
        help='Submit all jobs in a single request as an array'
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
            args.json_escape_input_file,
            args.batch_submit,
            session,
            logger
        )

        if not launched_jobs:
            logger.error('No jobs were successfully launched')
            return 1

        logger.info(f'Successfully launched {len(launched_jobs)} jobs')

        if args.disable_polling:
            logger.info('Polling disabled, exiting after launching jobs')
            return 0

        if args.poll_frequency is not None:
            poll_frequency = args.poll_frequency
        else:
            poll_frequency = max(1, min(int(len(launched_jobs) * 0.5), 60))
        
        logger.info(f'Using polling frequency of {poll_frequency} seconds')
        results = monitor_jobs(
            args.webui_url,
            launched_jobs,
            session,
            logger,
            poll_frequency
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