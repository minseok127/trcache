/*
 * validator/exchanges/exchange_client.h
 *
 * Abstract base class for exchange adapters.
 */

#ifndef VALIDATOR_EXCHANGES_EXCHANGE_CLIENT_H
#define VALIDATOR_EXCHANGES_EXCHANGE_CLIENT_H

#include "../core/config.h"
#include "trcache.h"

class exchange_client {
public:
	virtual ~exchange_client() {}

	/* Initialize the client with configuration */
	virtual bool init(const struct validator_config& config) = 0;

	/* Establish initial connection (non-blocking) */
	virtual void connect() = 0;

	/* Start the main feed loop (blocking) */
	virtual void start_feed(struct trcache* cache) = 0;

	/* Stop the feed and disconnect */
	virtual void stop() = 0;
};

#endif /* VALIDATOR_EXCHANGES_EXCHANGE_CLIENT_H */
