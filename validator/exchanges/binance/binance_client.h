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
#include <simdjson.h> /* Added simdjson header */

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

	/* * Helper: Fetch top N symbols by volume via REST API
	 * Now uses simdjson internally in .cpp
	 */
	bool fetch_top_symbols(int n, const std::string& rest_url);

	/* * Internal Helper: WebSocket Read Loop
	 * Updated signature to accept reusable parser
	 */
	void ws_loop_impl(void* ssl_ptr, std::string& leftover,
			  simdjson::dom::parser& parser);

	/* * Internal Helper: Parse JSON and push to trcache
	 * Updated signature to accept reusable parser
	 */
	void parse_and_feed(const char* json_str, size_t len,
			    simdjson::dom::parser& parser);
};

#endif /* VALIDATOR_EXCHANGES_BINANCE_CLIENT_H */
