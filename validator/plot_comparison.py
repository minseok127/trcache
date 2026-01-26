#!/usr/bin/env python3
"""
plot_comparison.py

Generates a side-by-side comparison of Engine Internal latency histograms
for two test runs (e.g., 2 symbols vs 500 symbols).

Usage:
    python3 plot_comparison.py <path1> <path2> [--output <output.png>]

Example:
    python3 plot_comparison.py \
        results/symbol2/hist_dump_20260125_071450_engine_internal.csv \
        results/symbol500/hist_dump_XXXXXXXX_XXXXXX_engine_internal.csv \
        --output results/engine_latency_comparison.png
"""

import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def load_histogram(csv_path: str) -> pd.DataFrame:
    """Load histogram CSV and return DataFrame."""
    df = pd.read_csv(csv_path)
    # Filter out zero counts for cleaner plotting
    return df[df['count'] > 0].copy()


def calculate_percentiles(df: pd.DataFrame) -> dict:
    """
    Calculate P50, P99, P99.9 from histogram data.
    Returns values in microseconds.
    """
    total = df['count'].sum()
    if total == 0:
        return {'p50': 0, 'p99': 0, 'p999': 0, 'max': 0, 'total': 0}

    cumsum = df['count'].cumsum()

    def get_percentile(p: float) -> float:
        target = total * p
        idx = (cumsum >= target).idxmax()
        return df.loc[idx, 'bucket_start_ns'] / 1000.0  # Convert to μs

    # Find max (last non-zero bucket)
    max_ns = df[df['count'] > 0]['bucket_start_ns'].max()

    return {
        'p50': get_percentile(0.50),
        'p99': get_percentile(0.99),
        'p999': get_percentile(0.999),
        'max': max_ns / 1000.0,
        'total': total
    }


def plot_histogram(ax, df: pd.DataFrame, stats: dict, title: str, color: str):
    """Plot a single histogram on the given axes."""
    # Convert to microseconds
    x = df['bucket_start_ns'] / 1000.0
    y = df['count']

    # Plot
    ax.fill_between(x, y, alpha=0.4, color=color)
    ax.plot(x, y, color=color, linewidth=1.0)

    # Log scale for both axes
    ax.set_xscale('log')
    ax.set_yscale('log')

    # Labels
    ax.set_xlabel('Latency (μs)', fontsize=10)
    ax.set_ylabel('Count', fontsize=10)
    ax.set_title(title, fontsize=12, fontweight='bold')

    # Grid
    ax.grid(True, which='both', ls='-', alpha=0.2)

    # Stats annotation
    stats_text = (
        f"Total: {stats['total']:,}\n"
        f"P50: {stats['p50']:.1f} μs\n"
        f"P99: {stats['p99']:.1f} μs\n"
        f"P99.9: {stats['p999']:.1f} μs"
    )
    ax.text(
        0.95, 0.95, stats_text,
        transform=ax.transAxes,
        ha='right', va='top',
        fontsize=9,
        fontfamily='monospace',
        bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='gray')
    )


def plot_comparison(path1: str, path2: str,
                    label1: str, label2: str,
                    output_path: str):
    """Generate side-by-side comparison plot."""

    # Load data
    df1 = load_histogram(path1)
    df2 = load_histogram(path2)

    # Calculate stats
    stats1 = calculate_percentiles(df1)
    stats2 = calculate_percentiles(df2)

    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle('Engine Internal Latency Comparison', fontsize=14, fontweight='bold')

    # Plot both histograms
    plot_histogram(ax1, df1, stats1, label1, '#2E86AB')  # Blue
    plot_histogram(ax2, df2, stats2, label2, '#E94F37')  # Red

    # Synchronize axes for fair comparison
    # Get combined range
    all_x = pd.concat([df1['bucket_start_ns'], df2['bucket_start_ns']]) / 1000.0
    all_y = pd.concat([df1['count'], df2['count']])

    x_min, x_max = all_x.min() * 0.5, all_x.max() * 2
    y_min, y_max = 1, all_y.max() * 2

    for ax in [ax1, ax2]:
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(y_min, y_max)

    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # Save
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"Saved: {output_path}")

    # Print summary
    print("\n" + "="*60)
    print("COMPARISON SUMMARY")
    print("="*60)
    print(f"{'Metric':<15} {label1:<20} {label2:<20}")
    print("-"*60)
    print(f"{'Total':<15} {stats1['total']:>18,} {stats2['total']:>18,}")
    print(f"{'P50':<15} {stats1['p50']:>17.1f}μs {stats2['p50']:>17.1f}μs")
    print(f"{'P99':<15} {stats1['p99']:>17.1f}μs {stats2['p99']:>17.1f}μs")
    print(f"{'P99.9':<15} {stats1['p999']:>17.1f}μs {stats2['p999']:>17.1f}μs")
    print("="*60)


def plot_empty(ax, title: str):
    """Plot an empty placeholder graph."""
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Latency (μs)', fontsize=10)
    ax.set_ylabel('Count', fontsize=10)
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.grid(True, which='both', ls='-', alpha=0.2)
    ax.set_xlim(0.1, 10000)
    ax.set_ylim(1, 1000000)
    ax.text(
        0.5, 0.5, 'Pending',
        transform=ax.transAxes,
        ha='center', va='center',
        fontsize=14,
        color='gray',
        fontweight='bold'
    )


def plot_single_with_empty(path: str, label1: str, label2: str, output_path: str):
    """Generate side-by-side plot with data on left, empty on right."""

    df = load_histogram(path)
    stats = calculate_percentiles(df)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle('Engine Internal Latency Comparison', fontsize=14, fontweight='bold')

    # Left: actual data
    plot_histogram(ax1, df, stats, label1, '#2E86AB')

    # Right: empty placeholder
    plot_empty(ax2, label2)

    # Set left axis range based on data
    x = df['bucket_start_ns'] / 1000.0
    y = df['count']
    ax1.set_xlim(x.min() * 0.5, x.max() * 2)
    ax1.set_ylim(1, y.max() * 2)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"Saved: {output_path}")

    print("\n" + "="*40)
    print("SUMMARY (2 Symbols)")
    print("="*40)
    print(f"Total:  {stats['total']:,}")
    print(f"P50:    {stats['p50']:.1f} μs")
    print(f"P99:    {stats['p99']:.1f} μs")
    print(f"P99.9:  {stats['p999']:.1f} μs")
    print("="*40)


def main():
    parser = argparse.ArgumentParser(
        description='Compare Engine Internal latency histograms'
    )
    parser.add_argument('path1', help='Path to first histogram CSV')
    parser.add_argument('path2', nargs='?', help='Path to second histogram CSV (optional)')
    parser.add_argument('--label1', default='2 Symbols (86h)',
                        help='Label for first dataset')
    parser.add_argument('--label2', default='500 Symbols (72h)',
                        help='Label for second dataset')
    parser.add_argument('--output', '-o', default='engine_latency_comparison.png',
                        help='Output file path')

    args = parser.parse_args()

    if args.path2:
        plot_comparison(args.path1, args.path2,
                       args.label1, args.label2,
                       args.output)
    else:
        # Single mode: show data on left, empty placeholder on right
        plot_single_with_empty(args.path1, args.label1, args.label2, args.output)


if __name__ == '__main__':
    main()
