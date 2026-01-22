#!/app/venv/bin/python

import time
import random
from luma.core.interface.serial import i2c
from luma.core.render import canvas
from luma.oled.device import ssd1306

# --- Configuration ---
WIDTH = 128
HEIGHT = 64
NUM_STARS = 60       # Number of stars on screen
MAX_DEPTH = 32       # Virtual distance (Z-axis) where stars spawn
Z_SPEED = 1.5        # How fast the stars move towards the camera
FOV = 64             # Field of view / perspective scaling factor

# --- Initialize Display ---
serial = i2c(port=1, address=0x3C)
device = ssd1306(serial, width=WIDTH, height=HEIGHT)

class Star:
    def __init__(self):
        self.reset(full_random=True)

    def reset(self, full_random=False):
        """
        Resets the star to the far distance.
        full_random: If True, scatter Z randomly (used for initial state).
        """
        # Random x/y position in virtual space (centered at 0,0)
        # We use a range slightly larger than screen to ensure corner coverage
        self.x = random.randint(-WIDTH, WIDTH)
        self.y = random.randint(-HEIGHT, HEIGHT)
        
        if full_random:
            self.z = random.randint(1, MAX_DEPTH)
        else:
            self.z = MAX_DEPTH

    def update(self):
        """Move the star closer (decrease Z)."""
        self.z -= Z_SPEED
        # If star passes the camera (z <= 0), reset it
        if self.z <= 0:
            self.reset()

def main():
    print(f"Starting Star Field on {WIDTH}x{HEIGHT} display...")
    
    # Initialize our star objects
    stars = [Star() for _ in range(NUM_STARS)]

    try:
        while True:
            with canvas(device) as draw:
                # Center coordinates of the screen
                cx, cy = WIDTH // 2, HEIGHT // 2

                for star in stars:
                    # 1. Update position
                    star.update()

                    # 2. Project 3D (x,y,z) to 2D screen coordinates (sx, sy)
                    # Formula: screen_x = center_x + (x / z) * FOV
                    # We add 0.1 to z to strictly avoid DivisionByZero errors
                    factor = FOV / (star.z + 0.1)
                    sx = int(cx + star.x * factor / MAX_DEPTH)
                    sy = int(cy + star.y * factor / MAX_DEPTH)

                    # 3. Draw the star if it is within screen bounds
                    if 0 <= sx < WIDTH and 0 <= sy < HEIGHT:
                        # Simple brightness/size trick:
                        # If z is small (close), draw a bigger/brighter pixel?
                        # On 128x64, single pixels usually look best, but we can
                        # simulate distance by skipping drawing if it's very far.
                        
                        # Only draw if not resetting immediately
                        if star.z > 0:
                            draw.point((sx, sy), fill="white")
                            
                            # Optional: "Warp" effect - draw a second pixel behind it
                            # to create a tiny streak for very close stars
                            if star.z < 10:
                                # Calculate previous position roughly
                                prev_factor = FOV / (star.z + Z_SPEED + 0.1)
                                px = int(cx + star.x * prev_factor / MAX_DEPTH)
                                py = int(cy + star.y * prev_factor / MAX_DEPTH)
                                draw.line((sx, sy, px, py), fill="white")

            # No explicit sleep needed; the I2C transfer limits the framerate naturally
            # but you can add time.sleep(0.01) if it runs too fast.

    except KeyboardInterrupt:
        print("Stopping simulation...")

if __name__ == "__main__":
    main()
