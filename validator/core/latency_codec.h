/*
 * validator/core/latency_codec.h
 *
 * Utility to encode and decode high-precision latency information
 * into a single 64-bit integer.
 *
 * Layout (64 bits):
 * [63 ... 32] Total Latency (T_feed - T_exchange) in nanoseconds.
 * [31 ... 00] Feed Timestamp (Lower 32 bits) in nanoseconds.
 */

#ifndef VALIDATOR_CORE_LATENCY_CODEC_H
#define VALIDATOR_CORE_LATENCY_CODEC_H

#include <stdint.h>

/*
 * LatencyCodec - Static helper for timestamp bit-packing.
 */
class LatencyCodec {
public:
	/* Masks and constants */
	static constexpr uint64_t MASK_32 = 0xFFFFFFFFULL;
	static constexpr uint64_t MAX_LATENCY_NS = MASK_32;

	/*
	 * encode - Packs latency and feed timestamp.
	 *
	 * @exch_ts: Exchange timestamp (ns).
	 * @feed_ts: Feed timestamp (ns), just before engine injection.
	 * @return:  Packed 64-bit integer.
	 */
	static inline uint64_t encode(uint64_t exch_ts, uint64_t feed_ts)
	{
		uint64_t latency = 0;
		if (feed_ts > exch_ts) {
			latency = feed_ts - exch_ts;
		}

		/* Clamp latency to fit in 32 bits (~4.29s) */
		if (latency > MAX_LATENCY_NS) {
			latency = MAX_LATENCY_NS;
		}

		/* Pack: [Latency 32bit] | [FeedTS_Low 32bit] */
		return (latency << 32) | (feed_ts & MASK_32);
	}

	/*
	 * decode_and_restore - Unpacks and restores full timestamps.
	 *
	 * Restores the full 64-bit feed_ts using the current local timestamp
	 * to handle the 32-bit wrap-around (every ~4.29s).
	 *
	 * @packed:   The packed 64-bit integer from encode().
	 * @local_ts: Current local timestamp (ns) for reference.
	 * @exch_ts:  [Out] Restored exchange timestamp.
	 * @feed_ts:  [Out] Restored feed timestamp.
	 */
	static inline void decode_and_restore(uint64_t packed,
					      uint64_t local_ts,
					      uint64_t* exch_ts,
					      uint64_t* feed_ts)
	{
		uint32_t feed_low = (uint32_t)(packed & MASK_32);
		uint64_t latency = (packed >> 32);

		/*
		 * Restore full feed_ts.
		 * We assume feed_ts is in the past, relative to local_ts.
		 * Take local_ts high bits, combine with feed_low.
		 */
		uint64_t restored_feed = (local_ts & ~MASK_32) | feed_low;

		/*
		 * Handle wrap-around: if result > local_ts,
		 * it implies the timestamp belongs to the previous cycle.
		 */
		if (restored_feed > local_ts) {
			restored_feed -= (1ULL << 32);
		}

		*feed_ts = restored_feed;
		*exch_ts = restored_feed - latency;
	}
};

#endif /* VALIDATOR_CORE_LATENCY_CODEC_H */
