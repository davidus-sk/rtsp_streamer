#!/app/penv/bin/python

import time
import math
from luma.core.interface.serial import i2c
from luma.core.render import canvas
from luma.oled.device import ssd1306

# --- Configuration ---
WIDTH = 128
HEIGHT = 64
SPEED = 4.0          # How fast they bounce
AMPLITUDE = 10.0     # How high/low they travel (pixels)
BASE_Y = 32          # Center Y position
RADIUS = 15          # Size of the objects

# --- Init Device ---
serial = i2c(port=1, address=0x3C)
device = ssd1306(serial, width=WIDTH, height=HEIGHT)

def main():
    start_time = time.time()

    # Define two objects with slightly different phases (offset)
    # so they don't move in perfect robotic unison
    objects = [
        {"x": 40, "phase_offset": 0.0},
        {"x": 88, "phase_offset": 0.2} 
    ]

    try:
        while True:
            t = time.time() - start_time
            
            with canvas(device) as draw:
                for obj in objects:
                    # 1. Calculate Vertical Position (Sine Wave)
                    # y = center + (amplitude * sin(time))
                    # We add phase_offset so they aren't perfectly synced
                    wave = math.sin((t * SPEED) + obj["phase_offset"])
                    y = BASE_Y + (wave * AMPLITUDE)

                    # 2. Calculate Deformation (Squash & Stretch)
                    # The deformation is based on the velocity (Cos is derivative of Sin).
                    # When moving fast (middle), stretch vertically.
                    # When stopping (top/bottom), squash slightly.
                    velocity_factor = math.cos((t * SPEED) + obj["phase_offset"])
                    
                    # Base squash is 1.0. We add a fraction of the velocity.
                    stretch_y = 1.0 + (velocity_factor * 0.15)
                    stretch_x = 1.0 - (velocity_factor * 0.15)
                    
                    # 3. Calculate Bounding Box
                    rx = RADIUS * stretch_x
                    ry = RADIUS * stretch_y
                    
                    box = [
                        obj["x"] - rx,
                        y - ry,
                        obj["x"] + rx,
                        y + ry
                    ]

                    # 4. Draw Outline
                    # fill="black" ensures they are hollow
                    # outline="white" draws the rim
                    draw.ellipse(box, fill="black", outline="white")

            # 60 FPS target
            time.sleep(0.016)

    except KeyboardInterrupt:
        device.clear()

if __name__ == "__main__":
    main()
