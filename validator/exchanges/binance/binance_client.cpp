/*
 * validator/exchanges/binance/binance_client.cpp
 *
 * Implementation of the Binance adapter.
 * Features:
 * 1. Thread-safe connection logic using getaddrinfo().
 * 2. Optimized SSL Context management (Shared CTX).
 * 3. Robust WebSocket frame parsing.
 * 4. Zero-Copy feed to trcache.
 * 5. Dynamic URL parsing from configuration.
 * 6. Automatic top-N symbol fetching via REST API.
 */

#include "binance_client.h"

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

/* HTTP Request */
#include <curl/curl.h>

/* OpenSSL Headers */
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>

/* JSON */
#include <nlohmann/json.hpp>

using json = nlohmann::json;

/* --------------------------------------------------------------------------
 * Constants & Helpers
 * --------------------------------------------------------------------------
 */

#define WS_BUFFER_SIZE 65536

enum WSOpcode {
	WS_OP_CONTINUATION = 0x0,
	WS_OP_TEXT = 0x1,
	WS_OP_BINARY = 0x2,
	WS_OP_CLOSE = 0x8,
	WS_OP_PING = 0x9,
	WS_OP_PONG = 0xA
};

/*
 * URL Structure helper
 */
struct parsed_url {
	std::string host;
	std::string port;
	std::string path;
};

/*
 * parse_ws_url - Simple parser for wss://host:port/path
 * Defaults: port 443, path /
 */
static parsed_url parse_ws_url(const std::string& url)
{
	parsed_url res;
	res.port = "443"; /* Default SSL port */
	res.path = "/";

	std::string temp = url;
	
	/* Remove protocol prefix */
	const std::string pfx = "wss://";
	if (temp.find(pfx) == 0) {
		temp = temp.substr(pfx.length());
	}

	/* Find Path */
	size_t slash_pos = temp.find('/');
	if (slash_pos != std::string::npos) {
		res.path = temp.substr(slash_pos);
		temp = temp.substr(0, slash_pos);
	}

	/* Find Port */
	size_t colon_pos = temp.find(':');
	if (colon_pos != std::string::npos) {
		res.port = temp.substr(colon_pos + 1);
		res.host = temp.substr(0, colon_pos);
	} else {
		res.host = temp;
	}

	return res;
}

/*
 * tcp_connect - Thread-safe TCP connection helper using getaddrinfo.
 */
static int tcp_connect(const char* host, const char* port)
{
	struct addrinfo hints = {}; /* Zero-initialize all fields */
	struct addrinfo *result, *rp;
	int sock = -1;

	hints.ai_family = AF_UNSPEC;     /* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_STREAM; /* TCP */

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
			break; /* Success */
		}

		close(sock);
		sock = -1;
	}

	freeaddrinfo(result);
	return sock;
}

/*
 * curl_write_cb - Callback for libcurl to write data into a string.
 */
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
		/* Fallback default */
		this->ws_base_url = "wss://stream.binance.com:9443/ws";
	}

	/*
	 * Priority:
	 * 1. Manual Symbols (if provided in config)
	 * 2. Top-N Symbols (fetched dynamically)
	 * 3. Fallback Hardcoded
	 */
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
 * Automatic Symbol Fetching
 * --------------------------------------------------------------------------
 */

bool binance_client::fetch_top_symbols(int n, const std::string& rest_url)
{
	CURL* curl;
	CURLcode res;
	std::string read_buffer;
	std::string url = rest_url + "/api/v3/ticker/24hr";

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

	try {
		json j = json::parse(read_buffer);
		if (!j.is_array()) return false;

		/*
		 * Sort by quoteVolume (descending).
		 * Binance returns numbers as strings, so we must convert using
		 * std::stod for correct comparison.
		 */
		std::sort(j.begin(), j.end(), 
			[](const json& a, const json& b) {
				double v1 = std::stod(
					a.value("quoteVolume", "0"));
				double v2 = std::stod(
					b.value("quoteVolume", "0"));
				return v1 > v2;
			}
		);

		this->target_symbols.clear();
		int count = 0;
		for (const auto& item : j) {
			std::string sym = item["symbol"];
			/* Store as lowercase to match WS stream convention */
			std::transform(sym.begin(), sym.end(), sym.begin(),
				       ::tolower);

			/* Filter out non-USDT pairs if needed, or take all */
			if (sym.find("usdt") != std::string::npos) {
				this->target_symbols.push_back(sym);
				count++;
			}
			if (count >= n) break;
		}

	} catch (const json::exception& e) {
		std::cerr << "[Binance] JSON Parse Error: " << e.what()
			  << std::endl;
		return false;
	}

	return true;
}

/* --------------------------------------------------------------------------
 * Connection & Feed Logic
 * --------------------------------------------------------------------------
 */

void binance_client::connect()
{
	this->running = true;
}

void binance_client::stop()
{
	this->running = false;
}

void binance_client::start_feed(struct trcache* cache)
{
	this->cache_ref = cache;

	for (const auto& sym : this->target_symbols) {
		trcache_register_symbol(cache, sym.c_str());
	}

	/* Parse URL from config */
	parsed_url p_url = parse_ws_url(this->ws_base_url);
	std::cout << "[Binance] Connecting to Host: " << p_url.host
		  << " Port: " << p_url.port << std::endl;

	SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
	if (!ctx) {
		std::cerr << "[Binance] Fatal: Failed to create SSL Context"
			  << std::endl;
		return;
	}

	while (this->running) {
		/* 1. TCP Connect (Dynamic Host/Port) */
		int sock = tcp_connect(p_url.host.c_str(), p_url.port.c_str());
		if (sock < 0) {
			std::cerr << "[Binance] Connect failed. Retrying..."
				  << std::endl;
			std::this_thread::sleep_for(std::chrono::seconds(1));
			continue;
		}

		/* 2. SSL Handshake */
		SSL* ssl = SSL_new(ctx);
		SSL_set_fd(ssl, sock);
		/* SNI is crucial for Cloudflare/AWS hosted services */
		SSL_set_tlsext_host_name(ssl, p_url.host.c_str());

		if (SSL_connect(ssl) <= 0) {
			std::cerr << "[Binance] SSL Handshake failed" << std::endl;
			SSL_free(ssl); close(sock);
			std::this_thread::sleep_for(std::chrono::seconds(1));
			continue;
		}

		std::cout << "[Binance] Connected." << std::endl;

		/* 3. WebSocket Upgrade */
		std::string path = "/stream?streams=";
		for (size_t i = 0; i < target_symbols.size(); ++i) {
			path += target_symbols[i] + "@aggTrade";
			if (i < target_symbols.size() - 1) path += "/";
		}

		/* Construct HTTP Request using parsed Host */
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

		/* Read HTTP Response */
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
			std::cerr << "[Binance] Upgrade Failed" << std::endl;
			SSL_free(ssl); close(sock);
			std::this_thread::sleep_for(std::chrono::seconds(1));
			continue;
		}

		std::cout << "[Binance] Stream Started." << std::endl;

		std::string leftover;
		if (header_end != -1 && header_end < bytes) {
			leftover.assign(buf + header_end, bytes - header_end);
		}

		/* 4. Enter WebSocket Read Loop */
		this->ws_loop_impl(ssl, leftover);

		std::cout << "[Binance] Disconnected." << std::endl;
		SSL_free(ssl);
		close(sock);
		
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	SSL_CTX_free(ctx);
}

/*
 * Internal Helper: WS Read Loop implementation
 */
void binance_client::ws_loop_impl(void* ssl_ptr, std::string& leftover)
{
	SSL* ssl = (SSL*)ssl_ptr;
	unsigned char buf[WS_BUFFER_SIZE];
	
	/*
	 * Added logging for read errors.
	 */
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
			if (r <= 0) {
				int err = SSL_get_error(ssl, r);
				if (err == SSL_ERROR_ZERO_RETURN) {
					std::cerr << "[Binance] EOF (Remote close)."
						  << std::endl;
				} else {
					std::cerr << "[Binance] SSL Error: "
						  << err << std::endl;
				}
				return -1;
			}
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
			std::cerr << "[Binance] Payload too large: "
				  << payload_len << std::endl;
			break; 
		}

		if (read_exact(buf, payload_len) != (int)payload_len) break;
		buf[payload_len] = 0;

		if (opcode == WS_OP_PING) {
			unsigned char pong[] = {0x8A, 0x00};
			SSL_write(ssl, pong, sizeof(pong));
		} else if (opcode == WS_OP_CLOSE) {
			/*
			 * Parse Close Code if available.
			 * The first 2 bytes of payload are the close code.
			 */
			if (payload_len >= 2) {
				int code = (buf[0] << 8) | buf[1];
				std::cout << "[Binance] Received WS_OP_CLOSE "
					  << "frame. Code: " << code
					  << std::endl;
			} else {
				std::cout << "[Binance] Received WS_OP_CLOSE "
					  << "frame. No code." << std::endl;
			}
			break;
		} else if (opcode == WS_OP_TEXT) {
			this->parse_and_feed((char*)buf, payload_len);
		}
	}
}

void binance_client::parse_and_feed(const char* json_str, size_t len)
{
	try {
		json j = json::parse(std::string(json_str, len));
		
		if (!j.contains("data")) return;
		auto& data = j["data"];

		std::string symbol = data["s"];
		for (auto& c : symbol) c = tolower(c);

		int sym_id = trcache_lookup_symbol_id(this->cache_ref,
						      symbol.c_str());
		if (sym_id < 0) return;

		struct trcache_trade_data trade;
		trade.timestamp = data["T"].get<uint64_t>();
		trade.trade_id = data["a"].get<uint64_t>();
		
		double price = std::stod(data["p"].get<std::string>());
		double vol = std::stod(data["q"].get<std::string>());

		trade.price.as_double = price;
		trade.volume.as_double = vol;

		trcache_feed_trade_data(this->cache_ref, &trade, sym_id);

	} catch (...) {
		/* Ignore parse errors */
	}
}
