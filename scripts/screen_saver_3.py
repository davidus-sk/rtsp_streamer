#!/app/venv/bin/python

import time
import math
import random
from luma.core.interface.serial import i2c
from luma.core.render import canvas
from luma.oled.device import ssd1306

# --- Configuration ---
WIDTH = 128
HEIGHT = 64
MODE_DURATION = 8  # Seconds per mode

# --- Init Device ---
serial = i2c(port=1, address=0x3C)
device = ssd1306(serial, width=WIDTH, height=HEIGHT)

# ==========================================
# MODE 1: THE MATRIX RAIN
# ==========================================
class MatrixRain:
    def __init__(self):
        self.cols = []
        # Create a drop for every 2nd column to save CPU
        for x in range(0, WIDTH, 4):
            self.cols.append({
                'x': x,
                'y': random.randint(-HEIGHT, 0),
                'speed': random.randint(2, 5),
                'len': random.randint(5, 20)
            })

    def update(self, draw):
        for drop in self.cols:
            # Draw the trail
            # We draw a vertical line. The head is white.
            # In monochrome, we can't do gradients, so we simulate it by broken lines
            # or just a solid line.
            draw.line((drop['x'], drop['y'] - drop['len'], drop['x'], drop['y']), fill="white")
            
            # Make the head pixel "sparkle" (sometimes disappear to look like code changing)
            if random.random() > 0.1:
                draw.point((drop['x'], drop['y']), fill="white")

            # Move down
            drop['y'] += drop['speed']

            # Reset if off screen
            if drop['y'] - drop['len'] > HEIGHT:
                drop['y'] = random.randint(-20, 0)
                drop['speed'] = random.randint(2, 5)
                drop['len'] = random.randint(5, 20)

# ==========================================
# MODE 2: 3D ROTATING CUBE
# ==========================================
class RotatingCube:
    def __init__(self):
        self.angle_x = 0
        self.angle_y = 0
        self.angle_z = 0
        # Vertices of a cube
        self.vertices = [
            [-1, -1, -1], [1, -1, -1], [1, 1, -1], [-1, 1, -1],
            [-1, -1, 1], [1, -1, 1], [1, 1, 1], [-1, 1, 1]
        ]
        # Edges (indices of vertices)
        self.edges = [
            (0,1), (1,2), (2,3), (3,0),
            (4,5), (5,6), (6,7), (7,4),
            (0,4), (1,5), (2,6), (3,7)
        ]

    def update(self, draw):
        # Rotate angles
        self.angle_x += 0.05
        self.angle_y += 0.03
        self.angle_z += 0.02

        projected_points = []
        
        # Rotation Matrices math
        sx, cx = math.sin(self.angle_x), math.cos(self.angle_x)
        sy, cy = math.sin(self.angle_y), math.cos(self.angle_y)
        sz, cz = math.sin(self.angle_z), math.cos(self.angle_z)

        for v in self.vertices:
            x, y, z = v[0], v[1], v[2]

            # Rotate X
            y, z = y*cx - z*sx, y*sx + z*cx
            # Rotate Y
            x, z = x*cy + z*sy, -x*sy + z*cy
            # Rotate Z
            x, y = x*cz - y*sz, x*sz + y*cz

            # Project 3D -> 2D
            # Scale factor (zoom) / (z + distance)
            scale = 400 / (z + 4)
            px = int(x * scale + WIDTH / 2)
            py = int(y * scale + HEIGHT / 2)
            projected_points.append((px, py))

        # Draw Edges
        for edge in self.edges:
            p1 = projected_points[edge[0]]
            p2 = projected_points[edge[1]]
            draw.line((p1[0], p1[1], p2[0], p2[1]), fill="white")

# ==========================================
# MODE 3: SINE WAVE OSCILLOSCOPE
# ==========================================
class Oscilloscope:
    def __init__(self):
        self.offset = 0

    def update(self, draw):
        self.offset += 0.2
        points = []
        
        # Draw multiple intertwined sine waves
        for i in range(0, WIDTH, 2):
            # Complex wave composition
            y1 = math.sin((i * 0.05) + self.offset) * 15
            y2 = math.sin((i * 0.1) - (self.offset * 0.5)) * 10
            
            y = (HEIGHT // 2) + y1 + y2
            points.append((i, int(y)))
            
        if len(points) > 1:
            draw.line(points, fill="white")
            
        # Draw a second "phase" line
        points2 = []
        for i in range(0, WIDTH, 4):
            y = (HEIGHT // 2) + math.cos((i * 0.08) + self.offset) * 20
            points2.append((i, int(y)))
        
        if len(points2) > 1:
             # Draw points instead of lines for the second wave for style
            draw.point(points2, fill="white")

# ==========================================
# UTILITIES
# ==========================================
def draw_glitch(draw):
    """Draws random noise blocks to simulate screen corruption."""
    for _ in range(10):
        x = random.randint(0, WIDTH)
        y = random.randint(0, HEIGHT)
        w = random.randint(5, 30)
        h = random.randint(1, 3)
        draw.rectangle((x, y, x+w, y+h), fill="white", outline=None)

def boot_sequence():
    """A fake retro boot-up text sequence."""
    messages = [
        "INIT CORE...", "MEM CHECK OK", "LOADING DRIVERS...",
        "GPU: SSD1306 FOUND", "MOUNTING /DEV/NULL", "CONNECTING..."
    ]
    for msg in messages:
        with canvas(device) as draw:
            draw.text((0, 0), "> SYSTEM BOOT", fill="white")
            draw.text((0, 15), f"> {msg}", fill="white")
            # Draw a blinking cursor
            if int(time.time() * 5) % 2 == 0:
                draw.rectangle((0, 30, 8, 38), fill="white")
        time.sleep(0.3)
    
    # Flash screen
    with canvas(device) as draw:
        draw.rectangle((0,0,WIDTH,HEIGHT), fill="white")
    time.sleep(0.1)

# ==========================================
# MAIN LOOP
# ==========================================
def main():
    boot_sequence()

    # Initialize modes
    modes = [MatrixRain(), RotatingCube(), Oscilloscope()]
    current_mode_idx = 0
    last_switch = time.time()
    
    try:
        while True:
            now = time.time()
            
            # Handle Mode Switching
            if now - last_switch > MODE_DURATION:
                # Glitch transition effect
                for _ in range(5):
                    with canvas(device) as draw:
                        draw_glitch(draw)
                    time.sleep(0.05)
                
                # Switch to next mode
                current_mode_idx = (current_mode_idx + 1) % len(modes)
                last_switch = now
            
            # Render Current Mode
            with canvas(device) as draw:
                modes[current_mode_idx].update(draw)
                
                # Overlay a small "Status Bar" at the bottom right
                # Blinking square to show the script is alive
                if int(time.time() * 2) % 2 == 0:
                    draw.point((WIDTH-2, HEIGHT-2), fill="white")

            # Cap framerate slightly to save CPU
            time.sleep(0.02)

    except KeyboardInterrupt:
        # Turn off screen on exit
        device.clear()

if __name__ == "__main__":
    main()
