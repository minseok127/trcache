/*
 * validator/exchanges/binance/binance_client.cpp
 *
 * Implementation of the Binance adapter using simdjson.
 * Features:
 * 1. Thread-safe connection logic using getaddrinfo().
 * 2. Optimized SSL Context management (Per-thread CTX).
 * 3. Robust WebSocket frame parsing.
 * 4. Zero-Copy feed to trcache.
 * 5. High-performance JSON parsing via simdjson.
 * 6. Multi-threaded Connection Sharding for scalability (>200 symbols).
 */

#include "binance_client.h"
#include "../../core/latency_codec.h"

#include <iostream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <thread>
#include <chrono>
#include <time.h>

/* HTTP Request */
#include <curl/curl.h>

/* OpenSSL Headers */
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>

/* JSON */
#include <simdjson.h>

/* --------------------------------------------------------------------------
 * Constants & Helpers
 * --------------------------------------------------------------------------
 */

#define WS_BUFFER_SIZE 65536
/* Binance recommends max 200 streams per connection */
#define MAX_STREAMS_PER_CONN 200

static inline uint64_t get_current_ns()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

enum WSOpcode {
	WS_OP_CONTINUATION = 0x0,
	WS_OP_TEXT = 0x1,
	WS_OP_BINARY = 0x2,
	WS_OP_CLOSE = 0x8,
	WS_OP_PING = 0x9,
	WS_OP_PONG = 0xA
};

struct parsed_url {
	std::string host;
	std::string port;
	std::string path;
};

static parsed_url parse_ws_url(const std::string& url)
{
	parsed_url res;
	res.port = "443";
	res.path = "/";

	std::string temp = url;
	const std::string pfx = "wss://";
	if (temp.find(pfx) == 0) {
		temp = temp.substr(pfx.length());
	}

	size_t slash_pos = temp.find('/');
	if (slash_pos != std::string::npos) {
		res.path = temp.substr(slash_pos);
		temp = temp.substr(0, slash_pos);
	}

	size_t colon_pos = temp.find(':');
	if (colon_pos != std::string::npos) {
		res.port = temp.substr(colon_pos + 1);
		res.host = temp.substr(0, colon_pos);
	} else {
		res.host = temp;
	}

	return res;
}

static int tcp_connect(const char* host, const char* port)
{
	struct addrinfo hints = {};
	struct addrinfo *result, *rp;
	int sock = -1;

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	int s = getaddrinfo(host, port, &hints, &result);
	if (s != 0) {
		std::cerr << "[Binance] getaddrinfo failed: " 
			  << gai_strerror(s) << std::endl;
		return -1;
	}

	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sock == -1) continue;

		if (::connect(sock, rp->ai_addr, rp->ai_addrlen) != -1) {
			int flag = 1;
			int ret = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, 
					     (char *)&flag, sizeof(flag));
			if (ret == -1) {
				std::cerr
					<< "[Binance] Warning: TCP_NODELAY failed" 
					<< std::endl;
			}
			break;
		}

		close(sock);
		sock = -1;
	}

	freeaddrinfo(result);
	return sock;
}

static size_t curl_write_cb(void* contents, size_t size, size_t nmemb,
			    void* userp)
{
	((std::string*)userp)->append((char*)contents, size * nmemb);
	return size * nmemb;
}

/* --------------------------------------------------------------------------
 * Constructor / Destructor
 * --------------------------------------------------------------------------
 */

binance_client::binance_client()
	: running(false), cache_ref(nullptr)
{
}

binance_client::~binance_client()
{
	stop();
}

/* --------------------------------------------------------------------------
 * Initialization
 * --------------------------------------------------------------------------
 */

bool binance_client::init(const struct validator_config& config)
{
	this->ws_base_url = config.ws_endpoint;

	if (this->ws_base_url.empty()) {
		this->ws_base_url = "wss://stream.binance.com:9443/ws";
	}

	if (!config.manual_symbols.empty()) {
		this->target_symbols = config.manual_symbols;
	} else if (config.top_n > 0) {
		std::string rest_url = config.rest_endpoint;
		if (rest_url.empty()) {
			rest_url = "https://api.binance.com";
		}
		if (!fetch_top_symbols(config.top_n, rest_url)) {
			std::cerr << "[Binance] Failed to fetch top "
				  << config.top_n << " symbols." << std::endl;
			return false;
		}
	} else {
		this->target_symbols = {"btcusdt", "ethusdt"};
	}

	std::cout << "[Binance] Initialized. URL: " << this->ws_base_url
		  << " Targets: " << this->target_symbols.size() << std::endl;
	return true;
}

/* --------------------------------------------------------------------------
 * Automatic Symbol Fetching (simdjson)
 * --------------------------------------------------------------------------
 */

bool binance_client::fetch_top_symbols(int n, const std::string& rest_url)
{
	CURL* curl;
	CURLcode res;
	std::string read_buffer;

	/*
	 * Determine the correct API path based on the base URL.
	 * 1. Spot:            /api/v3/ticker/24hr
	 * 2. Futures (USD-M): /fapi/v1/ticker/24hr
	 * 3. Futures (Coin-M): /dapi/v1/ticker/24hr
	 *
	 * This prevents 404 errors (HTML response) which cause JSON parsing
	 * failures (TAPE_ERROR).
	 */
	std::string path;
	if (rest_url.find("fapi") != std::string::npos) {
		path = "/fapi/v1/ticker/24hr";
	} else if (rest_url.find("dapi") != std::string::npos) {
		path = "/dapi/v1/ticker/24hr";
	} else {
		path = "/api/v3/ticker/24hr";
	}

	std::string url = rest_url + path;

	curl = curl_easy_init();
	if (!curl) return false;

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &read_buffer);
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "validator/1.0");

	std::cout << "[Binance] Fetching market data from " << url << "..."
		  << std::endl;
	
	res = curl_easy_perform(curl);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		std::cerr << "[Binance] curl_easy_perform() failed: "
			  << curl_easy_strerror(res) << std::endl;
		return false;
	}

	/* Parse using simdjson */
	simdjson::dom::parser parser;
	simdjson::dom::element doc;

	/*
	 * Parse the response buffer.
	 * If the URL was incorrect, the server might return HTML (404/5xx),
	 * causing simdjson to throw a TAPE_ERROR. We catch this here.
	 */
	auto error = parser.parse(read_buffer).get(doc);
	if (error) {
		std::cerr << "[Binance] JSON Parse Error: " << error << "\n"
			  << "   (Hint: Check if rest_endpoint matches the target "
			  << "market type: Spot vs Futures)" << std::endl;
		return false;
	}

	if (!doc.is_array()) return false;

	struct sym_vol {
		std::string_view symbol;
		double quote_vol;
	};

	std::vector<sym_vol> items;
	items.reserve(1000);

	/* Iterate over the ticker array and extract Symbol & QuoteVolume */
	for (auto obj : doc) {
		std::string_view s_sym, s_vol;
		if (obj["symbol"].get(s_sym) == simdjson::SUCCESS &&
		    obj["quoteVolume"].get(s_vol) == simdjson::SUCCESS) {
			
			/* Binance returns numbers as strings, so we convert them */
			try {
				std::string vol_str(s_vol);
				items.push_back({s_sym, std::stod(vol_str)});
			} catch (...) { continue; }
		}
	}

	/* Sort by Quote Volume (Descending) */
	std::sort(items.begin(), items.end(), 
		[](const sym_vol& a, const sym_vol& b) {
			return a.quote_vol > b.quote_vol;
		}
	);

	/* Select top N symbols (USDT pairs only) */
	this->target_symbols.clear();
	int count = 0;
	for (const auto& item : items) {
		std::string sym(item.symbol);
		std::transform(sym.begin(), sym.end(), sym.begin(), ::tolower);

		/* Filter: We only want USDT pairs for this benchmark */
		if (sym.find("usdt") != std::string::npos) {
			this->target_symbols.push_back(sym);
			count++;
		}
		if (count >= n) break;
	}

	return true;
}

/* --------------------------------------------------------------------------
 * Connection & Feed Logic (Multi-threaded)
 * --------------------------------------------------------------------------
 */

void binance_client::connect()
{
	this->running = true;
}

void binance_client::stop()
{
	this->running = false;

	/* Join all worker threads to ensure clean exit */
	for (auto& t : this->worker_threads) {
		if (t.joinable()) {
			t.join();
		}
	}
	this->worker_threads.clear();
}

void binance_client::start_feed(struct trcache* cache)
{
	this->cache_ref = cache;

	/* 1. Register all symbols upfront in the main thread */
	for (const auto& sym : this->target_symbols) {
		trcache_register_symbol(cache, sym.c_str());
	}

	/* 2. Calculate shards */
	int total_syms = this->target_symbols.size();
	int num_shards = (total_syms + MAX_STREAMS_PER_CONN - 1) / 
			 MAX_STREAMS_PER_CONN;

	std::cout << "[Binance] Spawning " << num_shards 
		  << " worker threads for " << total_syms 
		  << " symbols." << std::endl;

	/* 3. Spawn workers */
	for (int i = 0; i < num_shards; ++i) {
		std::vector<std::string> shard_syms;
		int start_idx = i * MAX_STREAMS_PER_CONN;
		int end_idx = std::min(start_idx + MAX_STREAMS_PER_CONN,
				       total_syms);

		for (int j = start_idx; j < end_idx; ++j) {
			shard_syms.push_back(this->target_symbols[j]);
		}

		/* Pass shard data to worker thread. cache argument removed. */
		this->worker_threads.emplace_back(
			&binance_client::feed_shard_worker,
			this, shard_syms, i
		);
	}
}

void binance_client::feed_shard_worker(std::vector<std::string> symbols,
				       int shard_id)
{
	/* Stagger start to prevent connection throttling (Rate Limit) */
	std::this_thread::sleep_for(std::chrono::milliseconds(shard_id * 200));

	parsed_url p_url = parse_ws_url(this->ws_base_url);
	
	/* Create dedicated SSL Context for this thread */
	SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
	if (!ctx) {
		std::cerr << "[Binance] Worker " << shard_id 
			  << ": Failed to create SSL Context" << std::endl;
		return;
	}

	/* Thread-local parser instance */
	simdjson::dom::parser parser;

	std::cout << "[Binance] Worker " << shard_id << " started. Handling "
		  << symbols.size() << " symbols." << std::endl;

	while (this->running) {
		int sock = tcp_connect(p_url.host.c_str(), p_url.port.c_str());
		if (sock < 0) {
			std::cerr << "[Binance] Worker " << shard_id 
				  << ": Connect failed. Retrying..." << std::endl;
			std::this_thread::sleep_for(std::chrono::seconds(1));
			continue;
		}

		SSL* ssl = SSL_new(ctx);
		SSL_set_fd(ssl, sock);
		SSL_set_tlsext_host_name(ssl, p_url.host.c_str());

		if (SSL_connect(ssl) <= 0) {
			std::cerr << "[Binance] Worker " << shard_id 
				  << ": SSL Handshake failed" << std::endl;
			SSL_free(ssl); close(sock);
			std::this_thread::sleep_for(std::chrono::seconds(1));
			continue;
		}

		/* Build Request Path for this shard */
		std::string path = "/stream?streams=";
		for (size_t i = 0; i < symbols.size(); ++i) {
			path += symbols[i] + "@aggTrade";
			if (i < symbols.size() - 1) path += "/";
		}

		std::string request =
			"GET " + path + " HTTP/1.1\r\n"
			"Host: " + p_url.host + ":" + p_url.port + "\r\n"
			"Upgrade: websocket\r\n"
			"Connection: Upgrade\r\n"
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
			"Sec-WebSocket-Version: 13\r\n"
			"\r\n";

		if (SSL_write(ssl, request.c_str(), request.length()) <= 0) {
			SSL_free(ssl); close(sock);
			continue;
		}

		char buf[4096];
		int bytes = SSL_read(ssl, buf, sizeof(buf) - 1);
		if (bytes <= 0) {
			SSL_free(ssl); close(sock);
			continue;
		}
		
		int header_end = -1;
		for (int i = 0; i < bytes - 3; ++i) {
			if (buf[i] == '\r' && buf[i+1] == '\n' && 
			    buf[i+2] == '\r' && buf[i+3] == '\n') {
				header_end = i + 4;
				break;
			}
		}

		std::string response(buf, bytes);
		if (response.find("101") == std::string::npos) {
			std::cerr << "[Binance] Worker " << shard_id 
				  << ": Upgrade Failed" << std::endl;
			SSL_free(ssl); close(sock);
			std::this_thread::sleep_for(std::chrono::seconds(1));
			continue;
		}

		std::cout << "[Binance] Worker " << shard_id 
			  << ": Stream Connected." << std::endl;

		std::string leftover;
		if (header_end != -1 && header_end < bytes) {
			leftover.assign(buf + header_end, bytes - header_end);
		}

		/* Run Loop */
		this->ws_loop_impl(ssl, leftover, parser);

		std::cout << "[Binance] Worker " << shard_id 
			  << ": Disconnected." << std::endl;
		SSL_free(ssl);
		close(sock);
		
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	SSL_CTX_free(ctx);
}

void binance_client::ws_loop_impl(void* ssl_ptr, std::string& leftover,
				  simdjson::dom::parser& parser)
{
	SSL* ssl = (SSL*)ssl_ptr;
	unsigned char buf[WS_BUFFER_SIZE];
	
	auto read_exact = [&](void* dest, size_t n) -> int {
		size_t copied = 0;
		if (!leftover.empty()) {
			size_t take = std::min(n, leftover.size());
			memcpy(dest, leftover.data(), take);
			leftover.erase(0, take);
			copied += take;
			if (copied == n) return n;
		}
		while (copied < n) {
			int r = SSL_read(ssl, (unsigned char*)dest + copied,
					 n - copied);
			if (r <= 0) return -1;
			copied += r;
		}
		return n;
	};

	while (this->running) {
		if (read_exact(buf, 2) != 2) break;

		int opcode = buf[0] & 0x0F;
		unsigned long payload_len = buf[1] & 0x7F;

		if (payload_len == 126) {
			if (read_exact(buf + 2, 2) != 2) break;
			payload_len = (buf[2] << 8) | buf[3];
		} else if (payload_len == 127) {
			if (read_exact(buf + 2, 8) != 8) break;
			payload_len = ((unsigned long)buf[6] << 24) |
				      ((unsigned long)buf[7] << 16) |
				      ((unsigned long)buf[8] << 8) |
				      ((unsigned long)buf[9]);
		}

		if (payload_len > WS_BUFFER_SIZE - 1) {
			std::cerr << "[Binance] Payload too large" << std::endl;
			break; 
		}

		if (read_exact(buf, payload_len) != (int)payload_len) break;
		buf[payload_len] = 0; /* Null-terminate for safety */

		if (opcode == WS_OP_PING) {
			unsigned char pong[] = {0x8A, 0x00};
			SSL_write(ssl, pong, sizeof(pong));
		} else if (opcode == WS_OP_CLOSE) {
			break;
		} else if (opcode == WS_OP_TEXT) {
			this->parse_and_feed((char*)buf, payload_len, parser);
		}
	}
}

void binance_client::parse_and_feed(const char* json_str, size_t len,
				    simdjson::dom::parser& parser)
{
	try {
		simdjson::dom::element doc;
		/* Using parser.parse() with length for safety */
		auto error = parser.parse(json_str, len, false).get(doc);
		
		if (error) return;

		simdjson::dom::element data;
		if (doc["data"].get(data) != simdjson::SUCCESS) return;

		std::string_view s_sym, s_price, s_vol;
		uint64_t ts, tid;

		if (data["s"].get(s_sym) != simdjson::SUCCESS) return;
		if (data["T"].get(ts) != simdjson::SUCCESS) return;
		if (data["a"].get(tid) != simdjson::SUCCESS) return;
		if (data["p"].get(s_price) != simdjson::SUCCESS) return;
		if (data["q"].get(s_vol) != simdjson::SUCCESS) return;

		std::string symbol(s_sym);
		for (auto& c : symbol) c = tolower(c);

		/* Thread-safe lookup */
		int sym_id = trcache_lookup_symbol_id(this->cache_ref,
						      symbol.c_str());
		if (sym_id < 0) return;

		struct trcache_trade_data trade;
		uint64_t exch_ts_ns = ts * 1000000ULL;
		uint64_t feed_ts_ns = get_current_ns();
		
		trade.timestamp = LatencyCodec::encode(exch_ts_ns, feed_ts_ns);
		trade.trade_id = tid;
		trade.price.as_double = std::stod(std::string(s_price));
		trade.volume.as_double = std::stod(std::string(s_vol));

		/* Thread-safe feed */
		if (trcache_feed_trade_data(this->cache_ref, &trade, sym_id) != 0) {
			/* Ignore sporadic queue full errors */
		}

	} catch (...) {
        /* Ignore errors to keep stream alive */
    }
}
