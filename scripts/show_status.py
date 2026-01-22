#!/app/penv/bin/python

import time
import json
import socket
import os
import psutil
from luma.core.interface.serial import i2c
from luma.core.render import canvas
from luma.oled.device import ssd1306
from PIL import ImageFont
from pathlib import Path

# Configuration
script_dir = Path(__file__).parent.resolve()
CAMERAS_FILE = f"{script_dir}/../web/cameras.json"
UPDATE_INTERVAL = 15  # Seconds
I2C_PORT = 1
I2C_ADDRESS = 0x3C    # Standard address for SSD1306

def get_ip_address():
    """
    Gets the primary IP address by attempting to connect to a public DNS.
    This avoids getting '127.0.0.1'.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('8.8.8.8', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def get_camera_count():
    """Reads the JSON file and counts the entries."""
    if not os.path.exists(CAMERAS_FILE):
        return 0
    try:
        with open(CAMERAS_FILE, 'r') as f:
            data = json.load(f)
            return len(data)
    except Exception:
        return 0

def get_streamer_count(script_name):
    """
    Counts how many running processes contain the specific script name
    in their command line arguments.

    Args:
        script_name (str): The name of the script (e.g., "rtsp_mqtt.py")

    Returns:
        int: The number of matching processes found.
    """
    count = 0

    # Iterate through all running processes
    # We fetch 'cmdline' which is a list like ['python3', 'rtsp_mqtt.py', '--config'...]
    for proc in psutil.process_iter(['cmdline']):
        try:
            cmdline = proc.info['cmdline']

            # Skip empty command lines (system processes)
            if not cmdline:
                continue

            # Check every argument in the command line
            # We use 'in' to handle cases where full path is provided
            # e.g., /home/pi/rtsp_mqtt.py will match "rtsp_mqtt.py"
            for arg in cmdline:
                if script_name in arg:
                    count += 1
                    break # Found a match in this process, move to next process

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Process died or is locked while we were iterating
            pass

    return count

def get_network_bytes():
    """Returns total bytes (sent + recv) across all interfaces."""
    net = psutil.net_io_counters()
    return net.bytes_sent + net.bytes_recv

def main():
    # Initialize Display
    serial = i2c(port=I2C_PORT, address=I2C_ADDRESS)
    device = ssd1306(serial, width=128, height=64) # Adjust height to 32 if you have the smaller screen

    # Load a font (Pixel operator is nice, but default bitmap font works if None)
    # To use a custom font: font = ImageFont.truetype("pixel_font.ttf", 10)
    font = None

    print("Display initialized. Press Ctrl+C to stop.")

    # Initialize network counters for speed calculation
    last_net_bytes = get_network_bytes()
    last_time = time.time()

    try:
        while True:
            # 1. Gather System Stats
            ip = get_ip_address()
            cpu_pct = psutil.cpu_percent(interval=None)
            mem_pct = psutil.virtual_memory().percent
            cam_count = get_camera_count()
            stream_count = get_streamer_count("rtsp_mqtt.py")

            # 2. Calculate Network Speed (Average over the last interval)
            current_net_bytes = get_network_bytes()
            current_time = time.time()

            # Calculate delta
            time_delta = current_time - last_time
            bytes_delta = current_net_bytes - last_net_bytes

            # Speed in KB/s
            if time_delta > 0:
                net_speed = (bytes_delta / time_delta) / 1024 
            else:
                net_speed = 0

            # Update references for next loop
            last_net_bytes = current_net_bytes
            last_time = current_time

            # 3. Draw to Screen
            with canvas(device) as draw:
                # Line 1: IP Address
                draw.text((0, 0),  f"IP: {ip}", fill="white", font=font)

                # Line 2: CPU & Memory
                draw.text((0, 16), f"CPU: {cpu_pct}%  Mem: {mem_pct}%", fill="white", font=font)

                # Line 3: Network Speed
                draw.text((0, 32), f"Net: {net_speed:.1f} KB/s Cams: {cam_count}", fill="white", font=font)

                # Line 4: Cameras Found
                draw.text((0, 48), f"Streams: {stream_count}", fill="white", font=font)

            # 4. Wait
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        # Clear screen on exit
        device.clear()
        print("Display stopped.")

if __name__ == "__main__":
    main()
