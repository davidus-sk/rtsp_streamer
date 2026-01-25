#!/usr/bin/python3

import json
import subprocess
import time
import sys
import os
import psutil # Requires: pip3 install psutil
from pathlib import Path

# --- Configuration ---
script_dir = Path(__file__).parent.resolve()
CAMERAS_FILE = f"{script_dir}/../web/cameras.json"
PROCESS_STATS_FILE = f"{script_dir}/../web/processes.json"
WORKER_SCRIPT = f"{script_dir}/../stream/rtsp_mqtt.py"
CHECK_INTERVAL = 5  # Seconds between checks

# Store active processes. Structure: { ip_address: { 'process': PopenObj, 'url': rtsp_url } }
active_workers = {}

def load_cameras():
    """Reads the JSON file and returns a list of valid cameras with streams."""
    file_path = CAMERAS_FILE

    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return []

    try:
        with open(file_path, 'r') as f:
            cameras = json.load(f)
            return [c for c in cameras if c.get('stream_uri') and not c.get('error')]
    except Exception as e:
        print(f"Error reading {CAMERAS_FILE}: {e}")
        return []

def start_worker(rtsp_url):
    """Starts the external python script for a specific URL."""
    script_path = WORKER_SCRIPT

    cmd = [
        str(script_path),
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

def save_process_stats(stats_data):
    """Writes the process statistics to a JSON file atomically."""
    file_path = PROCESS_STATS_FILE
    temp_path = f"{file_path}.tmp"

    try:
        with open(temp_path, 'w') as f:
            json.dump(stats_data, f, indent=4)
        os.rename(temp_path, file_path)
    except Exception as e:
        print(f"Error writing stats: {e}")

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

            current_stats = {}

            # Iterate through a copy of keys/values to safely modify if needed
            for ip, info in list(active_workers.items()):
                proc = info['process']
                url = info['url']

                # Check if IP has changed
                cameras = load_cameras()
                camera_found = False

                if cameras:
                    for cam in cameras:
                        if ip == cam['ip']:
                            camera_found = True

                if not camera_found:
                    print(f"[manager] ALERT: Worker for {ip} needs new address. Restarting...")
                    proc.kill()

                # --- HEALTH CHECK ---
                exit_code = proc.poll()

                if exit_code is not None:
                    # Process died
                    print(f"[manager] ALERT: Worker for {ip} died (Code: {exit_code}). Restarting...")
                    new_proc = start_worker(url)

                    if new_proc:
                        active_workers[ip]['process'] = new_proc
                        # We cannot get stats for a process that just started, mark as initializing
                        current_stats[ip] = {"status": "restarting"}
                    else:
                        current_stats[ip] = {"status": "failed"}

                else:
                    # --- STATS COLLECTION ---
                    try:
                        # Wrap the Popen object in a psutil Process object
                        p = psutil.Process(proc.pid)

                        # Get CPU usage (interval=None compares to last call, non-blocking)
                        # Note: The first call always returns 0.0
                        cpu = p.cpu_percent(interval=None)

                        # Get Memory usage (RSS is 'Resident Set Size' - actual physical RAM)
                        mem_bytes = p.memory_info().rss
                        mem_mb = mem_bytes / (1024 * 1024)

                        current_stats[ip] = {
                            "pid": proc.pid,
                            "cpu_percent": round(cpu, 1),
                            "memory_mb": round(mem_mb, 2),
                            "status": "running",
                            "ts": time.time()
                        }
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # Process might have died between poll() and stats check
                        current_stats[ip] = {"status": "ghost"}

            # Write the collected stats to file
            save_process_stats(current_stats)

    except KeyboardInterrupt:
        stop_all_workers()

if __name__ == "__main__":
    if not os.path.exists(WORKER_SCRIPT):
        print(f"Error: External script '{WORKER_SCRIPT}' not found.")
        sys.exit(1)

    main()
