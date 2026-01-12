/*
 * validator/exchanges/binance/binance_client.h
 *
 * Concrete implementation of the exchange adapter for Binance.
 * Handles WebSocket streams for aggTrade.
 */

#ifndef VALIDATOR_EXCHANGES_BINANCE_BINANCE_CLIENT_H
#define VALIDATOR_EXCHANGES_BINANCE_BINANCE_CLIENT_H

#include "../exchange_client.h"
#include <string>
#include <vector>
#include <atomic>

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
	/* Configuration */
	std::string ws_base_url;
	std::vector<std::string> target_symbols;
	
	/* State */
	std::atomic<bool> running;
	struct trcache* cache_ref;

	/* Internal Helpers */
	void ws_loop_impl(void* ssl_ptr, std::string& leftover);
	void parse_and_feed(const char* json_str, size_t len);

	/*
	 * Fetches all tickers, sorts by quote volume, and picks top N.
	 */
	bool fetch_top_symbols(int n, const std::string& rest_url);
};

#endif /* VALIDATOR_EXCHANGES_BINANCE_BINANCE_CLIENT_H */
