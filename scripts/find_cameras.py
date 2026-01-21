#!/usr/bin/python3

import argparse
import ipaddress
import socket
import fcntl
import struct
import json
import sys
import time
import os
import cv2
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Configuration
INTERVAL_SECONDS = 60
script_dir = Path(__file__).parent.resolve()

def is_rtsp_valid(rtsp_url, timeout_sec=15):
    """
    Checks if an RTSP URL is valid by attempting to grab a single frame.

    Args:
        rtsp_url (str): The RTSP stream URL (e.g., 'rtsp://user:pass@192.168.1.50:554/live')

    Returns:
        bool: True if the stream works, False otherwise.
    """

    try:
        # Convert seconds to microseconds for FFMPEG 'stimeout' option
        timeout_us = str(timeout_sec * 1000000)

        # Configure FFMPEG options via environment variable before opening
        # 'stimeout' sets the socket timeout (in microseconds)
        # 'rtsp_transport;tcp' forces TCP which is more reliable for scanning than UDP
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = f"rtsp_transport;tcp|stimeout;{timeout_us}"

        # Suppress ffmpeg error messages to keep stdout clean
        os.environ["OPENCV_LOG_LEVEL"] = "OFF"

        # Open the stream
        cap = cv2.VideoCapture(rtsp_url)

        # Check if the connection was initialized
        if not cap.isOpened():
            return False

        # Try to read one frame to ensure it's not just a socket open,
        # but actual video data flowing.
        ret, frame = cap.read()

        # Release the resource
        cap.release()

        return ret

    except Exception as e:
        return False

def get_interface_subnet(interface_name):
    """
    Returns the subnet of a specific interface in CIDR format (e.g., '192.168.1.0/24').
    Only works on Linux/Unix systems.
    """
    try:
        # Create a socket to interact with the kernel
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Helper to pack the interface name into the C struct
        # IFNAMSIZ is usually 16, so we limit to 15 chars + null byte
        packed_iface = struct.pack('256s', interface_name[:15].encode('utf-8'))

        # Get the IP Address (SIOCGIFADDR = 0x8915)
        ip_bytes = fcntl.ioctl(s.fileno(), 0x8915, packed_iface)[20:24]
        ip_addr = socket.inet_ntoa(ip_bytes)

        # Get the Netmask (SIOCGIFNETMASK = 0x891b)
        mask_bytes = fcntl.ioctl(s.fileno(), 0x891b, packed_iface)[20:24]
        netmask = socket.inet_ntoa(mask_bytes)

        # Calculate the Network CIDR
        # strict=False allows passing a host IP (e.g. 192.168.1.5) and 
        # automatically calculates the network address (192.168.1.0)
        network = ipaddress.IPv4Network(f"{ip_addr}/{netmask}", strict=False)

        return str(network)

    except IOError:
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def check_rtsp(ip, port, timeout=1.0):
    """
    Attempts to connect to the specified IP and port.
    Returns the IP if the connection is successful (port is open), else None.
    """
    try:
        # Create a socket object
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            result = sock.connect_ex((str(ip), port))
            if result == 0:
                return str(ip)
    except Exception:
        pass
    return None

def scan_subnet(subnet_str, port=554, max_workers=50):
    """
    Scans a subnet for a specific port using multi-threading.
    """
    found_cameras = []

    try:
        # Create an IPv4 network object
        network = ipaddress.ip_network(subnet_str, strict=False)
    except ValueError as e:
        print(f"Error: Invalid subnet format. {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {network} for RTSP devices on port {port}...", file=sys.stderr)

    # Use ThreadPoolExecutor to scan multiple IPs simultaneously
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map the check function to all hosts in the network
        # network.hosts() avoids the network address and broadcast address
        futures = {executor.submit(check_rtsp, ip, port): ip for ip in network.hosts()}

        for future in futures:
            result = future.result()
            if result:
                found_cameras.append({
                    "ip": result,
                    "port": port,
                    "protocol": "rtsp",
                    "status": "open",
                    "snapshot_url": f"http://{result}/axis-cgi/jpg/image.cgi?resolution=320x240&compression=50",
                    "stream_uri": f"rtsp://{result}/axis-media/media.amp",
                    "rtsp_valid": is_rtsp_valid(f"rtsp://{result}/axis-media/media.amp")
                })

    return found_cameras

def write_array_to_json_file(data_array, file_location):
    """
    Writes a list/array to a file in JSON format.

    Args:
        data_array (list): The list of data to write.
        file_location (str): The path to the file (e.g., '/tmp/output.json').

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # 1. Ensure the directory path exists
        directory = os.path.dirname(file_location)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        # 2. Open file and dump JSON
        with open(file_location, 'w', encoding='utf-8') as f:
            # indent=4 makes the file human-readable
            json.dump(data_array, f, indent=4)

        return True

    except IOError as e:
        print(f"Error writing to file '{file_location}': {e}")
        return False
    except TypeError as e:
        print(f"Error serializing data to JSON: {e}")
        return False

def main():
    print("Getting subnet for eth0...")
    subnet = get_interface_subnet("eth0")

    if subnet != None:
        print(f"Inerface eht0 is on {subnet}")
    else:
        print(f"No subnet found for eth0")

    write_array_to_json_file({"subnet":subnet, "timestamp":time.time()}, f"{script_dir}/../web/subnet.json")

    parser = argparse.ArgumentParser(description="Scan a subnet for RTSP cameras.")
    parser.add_argument("--subnet", default=subnet, help="The subnet to scan in CIDR format (e.g., 192.168.1.0/24)")
    parser.add_argument("--port", type=int, default=554, help="RTSP port to scan (default: 554)")
    parser.add_argument("--threads", type=int, default=100, help="Number of concurrent threads (default: 100)")

    args = parser.parse_args()

    print(f"Starting Camera Scanner. Interval: {INTERVAL_SECONDS}s")

    while True:
        start_time = time.time()

        results = scan_subnet(args.subnet, args.port, args.threads)

        elapsed = time.time() - start_time
        sleep_time = max(0, INTERVAL_SECONDS - elapsed)

        time.sleep(sleep_time)

    # Output solely the JSON array to stdout so it can be piped
    print(json.dumps(results, indent=4))
    write_array_to_json_file(results, f"{script_dir}/../web/cameras.json")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopping monitor...")
        sys.exit(0)
