#!/bin/bash

# ====================================================================
# Experiment Configuration
# ====================================================================

# Directory path for saving results (relative to trcache project root)
RESULTS_DIR="results/feed_only_benchmark"

# Path to the benchmark executable (relative to trcache project root)
BENCHMARK_EXE="./feed_only_benchmark"

# Fixed parameters
TOTAL_TIME=60   # Total experiment duration (seconds)
WARMUP_TIME=15  # Warmup duration (seconds)

# Parameter ranges to iterate through
# Zipf exponent (s) values array
ZIPF_S_VALUES=(0.0 1.2)

# Feed thread (N) values array
FEED_THREADS=(1 2 4 8)

# Function to determine Worker thread (M) values based on N
# (Considers Feed threads + Worker threads + 1 Monitor thread <= 16 cores)
get_worker_threads() {
	local n=$1
	if [ "$n" -eq 1 ]; then echo "4 6 8 10 12"; # N=1, M<=12 => Total <= 1+12+1=14
	elif [ "$n" -eq 2 ]; then echo "4 6 8 10 12"; # N=2, M<=12 => Total <= 2+12+1=15
	elif [ "$n" -eq 4 ]; then echo "4 6 8 10";    # N=4, M<=10 => Total <= 4+10+1=15
	elif [ "$n" -eq 8 ]; then echo "4 6";         # N=8, M<=6  => Total <= 8+6+1=15
	else echo ""; fi # Ignore other N values
}

# ====================================================================
# Script Execution
# ====================================================================

# Create the results directory if it doesn't exist
mkdir -p "${RESULTS_DIR}"

# Check if the benchmark executable exists
if [ ! -f "$BENCHMARK_EXE" ]; then
	echo "Error: Benchmark executable not found at $BENCHMARK_EXE"
	echo "Please compile first using 'make benchmark'"
	exit 1
fi

echo "Starting Feed-Only benchmark experiments..."
echo "Results will be saved in: ${RESULTS_DIR}"
echo "-----------------------------------------------------"

# Iterate through all parameter combinations
for s in "${ZIPF_S_VALUES[@]}"; do
	for n in "${FEED_THREADS[@]}"; do
		# Get the list of valid M values for the current N
		WORKER_THREAD_LIST=$(get_worker_threads "$n")
		# Skip if no valid M values for this N
		if [ -z "$WORKER_THREAD_LIST" ]; then continue; fi

		for m in $WORKER_THREAD_LIST; do
			# Generate the output filename (e.g., feed4_worker8_zipf1.0.csv)
			# Use printf for consistent decimal formatting
			s_str=$(printf "%.1f" "$s")
			output_file="${RESULTS_DIR}/feed${n}_worker${m}_zipf${s_str}.csv"

			echo "Running: N=${n}, M=${m}, s=${s_str} (Output: ${output_file})"

			# Execute the benchmark program
			"$BENCHMARK_EXE" \
				-f "$n" \
				-w "$m" \
				-o "$output_file" \
				-t "$TOTAL_TIME" \
				-W "$WARMUP_TIME" \
				-s "$s"

			# Optional: Check the exit code of the benchmark program
			if [ $? -ne 0 ]; then
				echo "Error: Benchmark run failed for N=${n}, M=${m}, s=${s_str}."
				# Decide whether to stop the script or continue
				# exit 1 # Uncomment to stop on error
			fi

			# Optional: Add a short delay between runs for system stabilization
			echo "Waiting 5 seconds..."
			sleep 5
			echo "-----------------------------------------------------"

		done # End M loop
	done # End N loop
done # End S loop

echo "====================================================="
echo "All benchmark runs completed."
echo "Results can be found in: ${RESULTS_DIR}"
echo "====================================================="

exit 0
