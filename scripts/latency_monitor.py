#!/usr/bin/python3

import json
import time
import subprocess
import platform
import re
import os
import sys

# Configuration
INPUT_FILE = '/var/www/html/cameras.json'
OUTPUT_FILE = '/var/www/html/latency.json'
INTERVAL_SECONDS = 60

def get_latency(ip_address):
    """
    Pings the IP address and returns the latency in milliseconds.
    Returns None if the host is unreachable.
    """
    # Detect OS to determine the correct ping flag
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    
    # Build command: ping -c 1 -W 2 (1 packet, 2 second timeout)
    command = ['ping', param, '1', ip_address]
    
    # Linux/Mac usually allow -W for timeout, but syntax varies. 
    # We rely on subprocess.timeout to catch hangs.
    
    try:
        # Run the ping command
        if platform.system().lower() == 'windows':
            output = subprocess.check_output(command, timeout=2).decode('cp437')
        else:
            output = subprocess.check_output(command, timeout=2).decode('utf-8')

        # Regex to parse time (Windows: "time=1ms", Linux: "time=1.23 ms")
        match = re.search(r'time[=<]\s*([\d\.]+)\s*(?:ms)?', output, re.IGNORECASE)
        
        if match:
            return float(match.group(1))
        else:
            return None # Could not parse time
            
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None # Host unreachable

def update_latencies():
    """
    Reads cameras.json, checks latency, and writes a Dictionary keyed by IP to latency.json.
    """
    if not os.path.exists(INPUT_FILE):
        print(f"[{time.strftime('%X')}] Error: {INPUT_FILE} not found.")
        return

    try:
        with open(INPUT_FILE, 'r') as f:
            cameras = json.load(f)

        # Dictionary to store results keyed by IP
        latency_results = {}
        
        print(f"[{time.strftime('%X')}] Pinging {len(cameras)} devices...")

        for cam in cameras:
            ip = cam.get('ip')
            if not ip:
                continue
                
            ms = get_latency(ip)
            
            # Map the result directly to the IP key
            latency_results[ip] = {
                "latency_ms": ms,
                "status": "online" if ms is not None else "offline",
                "timestamp": time.time()
            }

        # Atomic write
        temp_file = OUTPUT_FILE + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(latency_results, f, indent=4)
        
        os.replace(temp_file, OUTPUT_FILE)
        print(f"[{time.strftime('%X')}] Updated {OUTPUT_FILE}")

    except json.JSONDecodeError:
        print(f"Error: Failed to decode {INPUT_FILE}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

def main():
    print(f"Starting Latency Monitor. Interval: {INTERVAL_SECONDS}s")
    print("Output format: {'192.168.1.10': {'latency_ms': 12, ...}}")
    
    while True:
        start_time = time.time()
        
        update_latencies()
        
        elapsed = time.time() - start_time
        sleep_time = max(0, INTERVAL_SECONDS - elapsed)
        
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopping monitor...")
        sys.exit(0)
