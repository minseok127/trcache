/*
 * validator/exchanges/binance/binance_client.h
 *
 * Header for the Binance adapter.
 */

#ifndef VALIDATOR_EXCHANGES_BINANCE_CLIENT_H
#define VALIDATOR_EXCHANGES_BINANCE_CLIENT_H

#include "../exchange_client.h"
#include <string>
#include <vector>
#include <atomic>
#include <thread> /* Added for worker threads */
#include <simdjson.h>

class binance_client : public exchange_client {
public:
	binance_client();
	virtual ~binance_client();

	/* Interface Implementation */
	bool init(const struct validator_config& config) override;
	void connect() override;
	void start_feed(struct trcache* cache) override;
	void stop() override;

private:
	/* Internal state */
	std::string ws_base_url;
	std::vector<std::string> target_symbols;
	std::atomic<bool> running;
	struct trcache* cache_ref;

	/*
	 * Worker Threads Management
	 * To support >200 symbols, we must shard connections.
	 */
	std::vector<std::thread> worker_threads;

	/* Helper: Fetch top N symbols by volume via REST API */
	bool fetch_top_symbols(int n, const std::string& rest_url);

	/*
	 * Worker Entry Point
	 * Handles a single WebSocket connection for a subset of symbols.
	 * Removed unused 'cache' parameter.
	 */
	void feed_shard_worker(std::vector<std::string> symbols,
			       int shard_id);

	/* Internal Helper: WebSocket Read Loop */
	void ws_loop_impl(void* ssl_ptr, std::string& leftover,
			  simdjson::dom::parser& parser);

	/* Internal Helper: Parse JSON and push to trcache */
	void parse_and_feed(const char* json_str, size_t len,
			    simdjson::dom::parser& parser);
};

#endif /* VALIDATOR_EXCHANGES_BINANCE_CLIENT_H */
