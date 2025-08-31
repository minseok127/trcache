#!/bin/bash

# ==============================================================================
# trcache Production Readiness Benchmark Suite
#
# This script automates the execution of a comprehensive benchmark plan
# to validate the performance, scalability, and stability of trcache under
# various simulated market conditions.
# ==============================================================================

# --- Configuration ---
# General settings for all benchmark phases.
# Adjust these to match your test environment.
SYMBOLS=1000
DURATION="30s"
WARMUP="10s"
START_CORE=4 # Set to -1 to disable CPU affinity
RESULTS_DIR="results"

# Ensure the benchmark executable exists
if [ ! -f ./benchmark ]; then
    echo "Error: benchmark executable not found."
    echo "Please build it first with 'make'."
    exit 1
fi

# Create results directory
mkdir -p "$RESULTS_DIR"
echo "Saving results to $(pwd)/$RESULTS_DIR"

# --- Helper Function ---
run_test() {
    local test_name=$1
    shift
    local cmd="./benchmark $@"
    local log_file="$RESULTS_DIR/${test_name}.log"

    echo "----------------------------------------------------------------------"
    echo "=> Running Test: ${test_name}"
    echo "   Command: ${cmd}"
    echo "   Log File: ${log_file}"
    echo "----------------------------------------------------------------------"

    # Execute and save output
    $cmd > "$log_file" 2>&1

    # Check for successful execution
    if [ $? -eq 0 ]; then
        echo "=> Test '${test_name}' completed successfully."
    else
        echo "=> ERROR: Test '${test_name}' failed. Check log for details."
    fi
    echo ""
}

# ==============================================================================
# Phase 1: Baseline & Scalability Validation
# Goal: Verify the system's ability to scale with available CPU cores.
# ==============================================================================
echo "### Phase 1: Running Scalability Tests ###"
WORKER_COUNTS=(1 2 4 8) # Add more values like 12, 16 for larger machines
for workers in "${WORKER_COUNTS[@]}"; do
    run_test "scalability_workers_${workers}" \
        "$workers" "$SYMBOLS" "$DURATION" "$WARMUP" 0 2 "$START_CORE"
done

# ==============================================================================
# Phase 2: Workload Profile Analysis
# Goal: Analyze performance differences based on candle processing types.
# ==============================================================================
echo "### Phase 2: Running Workload Profile Tests ###"
# Using a fixed, optimal number of workers from Phase 1 (e.g., 8)
OPTIMAL_WORKERS=8
run_test "workload_mode_time_only" \
    "$OPTIMAL_WORKERS" "$SYMBOLS" "$DURATION" "$WARMUP" 0 0 "$START_CORE"
run_test "workload_mode_tick_only" \
    "$OPTIMAL_WORKERS" "$SYMBOLS" "$DURATION" "$WARMUP" 0 1 "$START_CORE"
run_test "workload_mode_both" \
    "$OPTIMAL_WORKERS" "$SYMBOLS" "$DURATION" "$WARMUP" 0 2 "$START_CORE"

# ==============================================================================
# Phase 3: Stress & Stability Validation
# Goal: Test system resilience under extreme, periodic load.
# ==============================================================================
echo "### Phase 3: Running Stress Test (Flash Crash Scenario) ###"
run_test "stress_test_flash_crash" \
    "$OPTIMAL_WORKERS" "$SYMBOLS" "3m" "30s" 1 2 "$START_CORE"

# ==============================================================================
# Phase 4: Long-Duration Stability (Soak Test)
# Goal: Uncover memory leaks or performance degradation over extended periods.
# NOTE: This test is commented out by default due to its long runtime.
# ==============================================================================
# echo "### Phase 4: Running Long-Duration Soak Test (8 hours) ###"
# echo "This will take a long time. Uncomment to run."
# run_test "soak_test_8h" \
#     "$OPTIMAL_WORKERS" "$SYMBOLS" "8h" "5m" 0 2 "$START_CORE" &
# # Monitor memory usage in parallel
# pid=$!
# echo "Soak test running with PID $pid. Monitoring memory usage..."
# while kill -0 $pid 2> /dev/null; do
#     ps -p $pid -o %cpu,rss,vsz >> "$RESULTS_DIR/soak_test_memory.log"
#     sleep 60
# done


echo "All benchmark phases completed."
echo "You can now run 'python3 plot_results.py' to visualize the data."

