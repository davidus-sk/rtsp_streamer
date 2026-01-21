#!/usr/bin/python3

import json
import subprocess
import time
import sys
import os
import signal
from pathlib import Path

# --- Configuration ---
script_dir = Path(__file__).parent.resolve()
CAMERAS_FILE = f"{script_dir}/../web/cameras.json"
WORKER_SCRIPT = f"{script_dir}/../scripts/rtsp_mqtt5.py"
CHECK_INTERVAL = 5  # Check for dead processes every 5 seconds

# Store active processes. Structure: { ip_address: { 'process': PopenObj, 'url': rtsp_url } }
active_workers = {}

def load_cameras():
    """Reads the JSON file and returns a list of valid cameras with streams."""
    if not os.path.exists(CAMERAS_FILE):
        print(f"Error: {CAMERAS_FILE} not found.")
        return []

    try:
        with open(CAMERAS_FILE, 'r') as f:
            cameras = json.load(f)
            # Filter only entries that have a valid stream_uri
            return [c for c in cameras if c.get('stream_uri') and not c.get('error')]
    except Exception as e:
        print(f"Error reading {CAMERAS_FILE}: {e}")
        return []

def start_worker(rtsp_url):
    """Starts the external python script for a specific URL."""
    cmd = [
        sys.executable,     # Uses the same python interpreter (python3)
        WORKER_SCRIPT,
        '--config', 'config.json',
        '--log-level', 'debug',
        '--rtsp-url', rtsp_url
    ]

    try:
        # Start the process in the background
        process = subprocess.Popen(cmd)
        return process
    except Exception as e:
        print(f"Failed to start worker for {rtsp_url}: {e}")
        return None

def stop_all_workers():
    """Gracefully terminates all child processes."""
    print("\nStopping all workers...")
    for ip, info in active_workers.items():
        if info['process']:
            info['process'].terminate()
    print("Exiting.")

def main():
    print(f"--- Process Manager Started. Monitoring {CAMERAS_FILE} ---")

    # 1. Initial Start
    cameras = load_cameras()
    if not cameras:
        print("No valid cameras found to start.")
        return

    for cam in cameras:
        ip = cam['ip']
        url = cam['stream_uri']

        print(f"[manager] Starting worker for {ip}...")
        proc = start_worker(url)

        if proc:
            active_workers[ip] = {'process': proc, 'url': url}

    # 2. Monitoring Loop
    try:
        while True:
            time.sleep(CHECK_INTERVAL)

            # Iterate through a copy of keys/values to avoid modification issues
            for ip, info in list(active_workers.items()):
                proc = info['process']
                url = info['url']

                # Check if process is still alive (poll() returns None if running)
                exit_code = proc.poll()

                if exit_code is not None:
                    print(f"[manager] ALERT: Worker for {ip} died (Exit Code: {exit_code}). Restarting...")

                    # Restart the process
                    new_proc = start_worker(url)

                    # Update the dictionary with the new process handle
                    if new_proc:
                        active_workers[ip]['process'] = new_proc
                        print(f"[manager] Worker for {ip} restored successfully.")
                    else:
                        print(f"[manager] CRITICAL: Could not restart worker for {ip}.")

    except KeyboardInterrupt:
        stop_all_workers()

if __name__ == "__main__":
    # Ensure the worker script actually exists before running
    if not os.path.exists(WORKER_SCRIPT):
        print(f"Error: External script '{WORKER_SCRIPT}' not found in current directory.")
        sys.exit(1)

    main()
