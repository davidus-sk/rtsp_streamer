#!/app/penv/bin/python

import time
import numpy as np
from luma.core.interface.serial import i2c
from luma.core.render import canvas
from luma.oled.device import ssd1306

# --- Configuration ---
WIDTH = 128
HEIGHT = 64
I2C_PORT = 1
I2C_ADDRESS = 0x3C
INTERVAL = 0.05  # Speed of simulation

# --- Initialize Display ---
# Ensure I2C is enabled on your device (e.g., via raspi-config)
serial = i2c(port=I2C_PORT, address=I2C_ADDRESS)
device = ssd1306(serial, width=WIDTH, height=HEIGHT)

def init_grid(w, h):
    """Create a random initial state (0 or 1)."""
    return np.random.choice([0, 1], size=(h, w), p=[0.8, 0.2])

def update_grid(grid):
    """
    Apply Conway's Game of Life rules using Numpy for speed.
    Rules:
    - Any live cell with < 2 or > 3 neighbors dies.
    - Any live cell with 2 or 3 neighbors lives.
    - Any dead cell with exactly 3 neighbors becomes a live cell.
    """
    # Count neighbors using a cyclic roll (toroidal wrap-around)
    # This checks all 8 neighbors for every cell at once
    neighbors = (
        np.roll(grid, 1, axis=0) + np.roll(grid, -1, axis=0) +
        np.roll(grid, 1, axis=1) + np.roll(grid, -1, axis=1) +
        np.roll(np.roll(grid, 1, axis=0), 1, axis=1) +
        np.roll(np.roll(grid, 1, axis=0), -1, axis=1) +
        np.roll(np.roll(grid, -1, axis=0), 1, axis=1) +
        np.roll(np.roll(grid, -1, axis=0), -1, axis=1)
    )

    # Apply rules
    # 1. Stay alive if currently alive and neighbors are 2 or 3
    # 2. Be born if currently dead and neighbors are exactly 3
    new_grid = ((grid == 1) & ((neighbors == 2) | (neighbors == 3))) | \
               ((grid == 0) & (neighbors == 3))
               
    return new_grid.astype(int)

def main():
    print(f"Starting Game of Life on {WIDTH}x{HEIGHT} display...")
    grid = init_grid(WIDTH, HEIGHT)

    try:
        while True:
            # Draw the grid
            with canvas(device) as draw:
                # Find coordinates of all live cells
                # np.where returns (row_indices, col_indices)
                rows, cols = np.where(grid == 1)
                
                # Draw pixels. Luma expects (x, y), so we zip cols, rows
                # Drawing individual points is slow, so we construct a point list
                points = list(zip(cols, rows))
                if points:
                    draw.point(points, fill="white")

            # Calculate next generation
            grid = update_grid(grid)
            
            # Reset if life goes extinct or static (optional simple check)
            if np.sum(grid) == 0:
                grid = init_grid(WIDTH, HEIGHT)
                
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("Stopping...")

if __name__ == "__main__":
    main()
