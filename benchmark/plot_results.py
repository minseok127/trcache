import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import glob

# --- Configuration ---
RESULTS_DIR = "results"
plt.style.use('seaborn-v0_8-whitegrid')

def parse_summary(log_file):
    """Parses the summary section of a benchmark log file."""
    summary = {}
    with open(log_file, 'r') as f:
        for line in f:
            if "Average Ingestion Throughput" in line:
                match = re.search(r'([\d,]+\.\d+)\s+trades/sec', line)
                if match:
                    summary['throughput'] = float(match.group(1).replace(',', ''))
            elif "Config:" in line:
                workers_match = re.search(r'(\d+)\s+workers', line)
                if workers_match:
                    summary['workers'] = int(workers_match.group(1))
    return summary

def parse_timeseries(log_file):
    """Parses the time-series data table from a log file into a DataFrame."""
    with open(log_file, 'r') as f:
        content = f.read()

    table_match = re.search(
        r'--- Time-Series Performance Data ---\n(.*?)\n---',
        content, re.DOTALL)
    if not table_match:
        return pd.DataFrame()

    table_str = table_match.group(1)
    lines = [line.strip() for line in table_str.strip().split('\n')]

    if len(lines) < 3:
        return pd.DataFrame()

    header = re.split(r'\s{2,}', lines[0])
    data = [re.split(r'\s+', line) for line in lines[2:]]

    df = pd.DataFrame(data, columns=header)
    
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df.dropna()

def plot_scalability():
    """Generates a plot for worker scalability."""
    print("Generating scalability plot...")
    files = glob.glob(os.path.join(RESULTS_DIR, "scalability_workers_*.log"))
    if not files:
        print("  - No scalability log files found. Skipping.")
        return

    data = []
    for f in files:
        summary = parse_summary(f)
        if 'workers' in summary and 'throughput' in summary:
            data.append(summary)

    if not data:
        print("  - Could not parse scalability data. Skipping.")
        return

    df = pd.DataFrame(data).sort_values('workers')

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(
        df['workers'], df['throughput'],
        marker='o', linestyle='-', color='b'
    )
    ax.set_title(
        'trcache Worker Scalability', fontsize=16, fontweight='bold'
    )
    ax.set_xlabel('Number of Worker Threads', fontsize=12)
    ax.set_ylabel('Throughput (trades/sec)', fontsize=12)
    ax.get_yaxis().set_major_formatter(
        mticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.set_xticks(df['workers'])
    plt.tight_layout()
    
    output_path = os.path.join(RESULTS_DIR, "scalability.png")
    plt.savefig(output_path, dpi=150)
    print(f"  - Saved to {output_path}")

def plot_workload_comparison():
    """Generates a plot comparing performance across different candle modes."""
    print("Generating workload comparison plot...")
    modes = {
        'time_only': 'Time Only',
        'tick_only': 'Tick Only',
        'both': 'Both'
    }
    data = []

    for key, name in modes.items():
        log_file = os.path.join(RESULTS_DIR, f"workload_mode_{key}.log")
        if os.path.exists(log_file):
            summary = parse_summary(log_file)
            timeseries_df = parse_timeseries(log_file)
            if 'throughput' in summary and not timeseries_df.empty:
                data.append({
                    'Mode': name,
                    'Throughput': summary['throughput'],
                    'p99 Tick Latency (us)': timeseries_df['p99_Tick(us)'].mean(),
                    'p99 Time Latency (us)': timeseries_df['p99_Time(us)'].mean(),
                })

    if not data:
        print("  - No workload log files found or parsed. Skipping.")
        return
        
    df = pd.DataFrame(data).set_index('Mode')

    fig, ax1 = plt.subplots(figsize=(12, 7))
    
    # Primary axis for Throughput
    df['Throughput'].plot(
        kind='bar', ax=ax1, color='royalblue', width=0.2, position=1.5
    )
    ax1.set_title(
        'Performance by Workload Profile', fontsize=16, fontweight='bold'
    )
    ax1.set_ylabel('Throughput (trades/sec)', color='royalblue', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='royalblue')
    ax1.get_yaxis().set_major_formatter(
        mticker.FuncFormatter(lambda x, p: f'{int(x/1e6)}M'))
    ax1.set_xlabel('')
    ax1.tick_params(axis='x', rotation=0)

    # Secondary axis for Latency
    ax2 = ax1.twinx()
    df['p99 Tick Latency (us)'].plot(
        kind='bar', ax=ax2, color='seagreen', width=0.2, position=-0.5
    )
    df['p99 Time Latency (us)'].plot(
        kind='bar', ax=ax2, color='darkorange', width=0.2, position=-1.5
    )
    ax2.set_ylabel('p99 E2E Latency (µs)', fontsize=12)
    
    fig.legend(
        ['Throughput', 'p99 Tick Latency', 'p99 Time Latency'],
        loc="upper right", bbox_to_anchor=(0.9, 0.9))
    plt.tight_layout()

    output_path = os.path.join(RESULTS_DIR, "workload_comparison.png")
    plt.savefig(output_path, dpi=150)
    print(f"  - Saved to {output_path}")

def plot_stress_test_timeseries():
    """Generates a time-series plot for the flash crash stress test."""
    print("Generating stress test time-series plot...")
    log_file = os.path.join(RESULTS_DIR, "stress_test_flash_crash.log")
    if not os.path.exists(log_file):
        print(f"  - Log file not found: {log_file}. Skipping.")
        return

    df = parse_timeseries(log_file)
    if df.empty:
        print("  - No time-series data found in log. Skipping.")
        return

    fig, ax1 = plt.subplots(figsize=(15, 7))

    # Left Y-axis for Throughput
    ax1.plot(
        df['Time(s)'], df['Throughput'],
        color='cornflowerblue', label='Throughput'
    )
    ax1.set_xlabel('Time (seconds)', fontsize=12)
    ax1.set_ylabel(
        'Throughput (trades/sec)', color='cornflowerblue', fontsize=12
    )
    ax1.tick_params(axis='y', labelcolor='cornflowerblue')
    ax1.get_yaxis().set_major_formatter(
        mticker.FuncFormatter(lambda x, p: f'{int(x/1e6)}M'))

    # Right Y-axis for Latencies
    ax2 = ax1.twinx()
    ax2.plot(
        df['Time(s)'], df['p99_Tick(us)'],
        color='darkorange', linestyle='--', label='p99 Tick Latency'
    )
    ax2.plot(
        df['Time(s)'], df['p99_Closing(us)'], color='seagreen',
        linestyle='-', linewidth=2, label='p99 Closing Trade Latency'
    )
    ax2.set_ylabel('p99 Latency (µs)', fontsize=12)
    ax2.set_yscale('log') # Use log scale for better visibility of spikes
    
    ax1.set_title(
        'System Resilience Under Flash Crash Scenario',
        fontsize=16, fontweight='bold'
    )
    fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.9))
    plt.tight_layout()

    output_path = os.path.join(RESULTS_DIR, "stress_test_timeseries.png")
    plt.savefig(output_path, dpi=150)
    print(f"  - Saved to {output_path}")


if __name__ == "__main__":
    if not os.path.exists(RESULTS_DIR):
        print(f"Error: Results directory '{RESULTS_DIR}' not found.")
        print("Please run './run_benchmarks.sh' first.")
    else:
        plot_scalability()
        plot_workload_comparison()
        plot_stress_test_timeseries()
        print("\nAll plots generated successfully.")
