import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import sys

def plot_histogram(csv_file, ax, title, color):
    """
    Reads a histogram CSV and plots it on the given axes.
    """
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"File not found: {csv_file}")
        return

    # Filter out extremely high latency (e.g., > 1 sec) if needed,
    # or just plot everything. Here we plot everything.
    # X-axis: bucket_start_ns (converted to microseconds for readability)
    x = df['bucket_start_ns'] / 1000.0
    y = df['count']

    # Use a step plot or line plot
    ax.plot(x, y, color=color, linewidth=1.0, label='Count')
    
    # Fill area under curve for better visibility
    ax.fill_between(x, y, color=color, alpha=0.3)

    ax.set_title(title, fontsize=10, fontweight='bold')
    ax.set_xlabel('Latency (microseconds)', fontsize=9)
    ax.set_ylabel('Count', fontsize=9)
    
    # Log-log scale is usually best for latency histograms
    # because both time (x) and counts (y) span multiple orders of magnitude.
    ax.set_xscale('log')
    ax.set_yscale('log')
    
    ax.grid(True, which="both", ls="-", alpha=0.2)
    
    # Add simple stats annotation if data exists
    if not df.empty:
        total_count = df['count'].sum()
        max_lat = x.max()
        ax.text(0.95, 0.95, f'Total: {total_count}\nMax Lat: {max_lat:.1f} us', 
                transform=ax.transAxes, ha='right', va='top', 
                fontsize=8, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

def main():
    # Find the latest histogram dump files
    # Expected pattern: hist_dump_YYYYMMDD_HHMMSS_*.csv
    # We group them by timestamp prefix.
    
    # Manual prefix override
    if len(sys.argv) > 1:
        prefix = sys.argv[1]
    else:
        # Auto-detect latest
        files = glob.glob("hist_dump_*_network_parsing.csv")
        if not files:
            print("No histogram files found.")
            return
        files.sort()
        # Extract prefix: remove suffix
        latest_file = files[-1]
        prefix = latest_file.replace("_network_parsing.csv", "")

    print(f"Visualizing histograms for prefix: {prefix}")

    files_map = {
        'Network + Parsing': (f"{prefix}_network_parsing.csv", 'blue'),
        'Engine Internal':   (f"{prefix}_engine_internal.csv", 'green'),
        'Auditor Detection': (f"{prefix}_auditor_detection.csv", 'red')
    }

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f'Latency Distribution Analysis ({os.path.basename(prefix)})', fontsize=14)

    for ax, (title, (fname, color)) in zip(axes, files_map.items()):
        plot_histogram(fname, ax, title, color)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    
    output_img = f"{prefix}_plot.png"
    plt.savefig(output_img, dpi=150)
    print(f"Plot saved to: {output_img}")
    # plt.show() # Uncomment to view window

if __name__ == "__main__":
    main()
