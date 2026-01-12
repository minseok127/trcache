/*
 * validator/main.cpp
 *
 * Entry point for the Universal Validator.
 * Orchestrates configuration loading, engine initialization,
 * exchange adapter instantiation, and thread management.
 */

#include <iostream>
#include <thread>
#include <atomic>
#include <csignal>
#include <cstring>

/* Core headers */
#include "trcache.h"
#include "core/config_loader.h"
#include "core/types.h"
#include "core/engine.h"
#include "core/auditor.h"

/* Exchange Adapters */
#include "exchanges/exchange_client.h"

/*
 * We include concrete adapter headers here to instantiate them in the factory.
 * In a larger system, we might use a dynamic plugin loader.
 */
#include "exchanges/binance/binance_client.h"

/* Global running flag for graceful shutdown */
std::atomic<bool> g_running(true);

/*
 * signal_handler - Handles SIGINT (Ctrl+C).
 */
void signal_handler(int signum)
{
	(void)signum;
	std::cout << "\n[System] Caught signal, shutting down..." << std::endl;
	g_running = false;
}

/*
 * print_usage - Prints command line usage.
 */
void print_usage(const char* prog_name)
{
	std::cerr << "Usage: " << prog_name << " [-c config_path]"
		  << std::endl;
}

/*
 * main - Application entry point.
 */
int main(int argc, char *argv[])
{
	/* 1. Argument Parsing */
	std::string config_path = "config.json";
	for (int i = 1; i < argc; i++) {
		if (std::strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
			config_path = argv[++i];
		} else {
			print_usage(argv[0]);
			return -1;
		}
	}

	std::cout << "[System] Starting Universal Validator..." << std::endl;
	std::cout << "[System] Loading config from: " << config_path
		  << std::endl;

	/* 2. Load Configuration */
	struct validator_config cfg;
	if (!load_config(config_path, &cfg)) {
		std::cerr << "[Error] Failed to load configuration."
			  << std::endl;
		return -1;
	}

	/* 3. Setup Signal Handler */
	std::signal(SIGINT, signal_handler);

	/* 
	 * 4. Initialize trcache Engine (Wrapper)
	 * engine_init maps the config to trcache callbacks and init_ctx.
	 */
	struct trcache* cache = engine_init(cfg);
	if (!cache) {
		std::cerr << "[Error] Failed to initialize trcache engine."
			  << std::endl;
		return -1;
	}
	std::cout << "[System] trcache engine initialized." << std::endl;

	/* 5. Instantiate Exchange Adapter (Factory Logic) */
	exchange_client* client = nullptr;

	if (cfg.exchange == "binance") {
		client = new binance_client();
	} else {
		std::cerr << "[Error] Unknown exchange: " << cfg.exchange
			  << std::endl;
		engine_destroy(cache);
		return -1;
	}

	/* 6. Initialize and Connect Adapter */
	if (!client->init(cfg)) {
		std::cerr << "[Error] Failed to init exchange client."
			  << std::endl;
		delete client;
		engine_destroy(cache);
		return -1;
	}

	std::cout << "[System] Connecting to " << cfg.exchange << "..."
		  << std::endl;
	try {
		client->connect();
	} catch (const std::exception& e) {
		std::cerr << "[Error] Connection failed: " << e.what()
			  << std::endl;
		delete client;
		engine_destroy(cache);
		return -1;
	}

	/*
	 * 7. Start Worker Threads
	 */
	std::thread feed_thread([&]() {
		client->start_feed(cache);
	});

	std::thread auditor_thread([&]() {
		run_auditor(cache, cfg, g_running);
	});

	/*
	 * We simply wait here. The signal handler will set g_running = false.
	 * In a more complex app, we might handle admin tasks here.
	 */
	while (g_running) {
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	/* 9. Shutdown Sequence */
	std::cout << "[System] Stopping threads..." << std::endl;

	/* Stop the feed first */
	client->stop();
	if (feed_thread.joinable()) {
		feed_thread.join();
	}

	/* Stop the auditor (it shares g_running but we wait for it) */
	if (auditor_thread.joinable()) {
		auditor_thread.join();
	}

	/* 10. Cleanup */
	delete client;
	engine_destroy(cache);

	std::cout << "[System] Cleanup complete. Bye!" << std::endl;
	return 0;
}
