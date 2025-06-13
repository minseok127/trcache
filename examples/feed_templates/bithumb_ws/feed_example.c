#include <libwebsockets.h>
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../trcache.h"

/*
 * Example demonstrating how to feed realtime trade data from the
 * Bithumb websocket API into trcache.  This is a minimal template
 * that connects to the public trade feed and converts JSON trade
 * messages into trcache_trade_data structures.
 *
 * The example uses libwebsockets for the websocket client and
 * cJSON for JSON parsing.  Only the core logic is shown; robust
 * error handling and reconnect logic should be added for a real
 * application.
 */

#define BITHUMB_WSS "wss://pubwss.bithumb.com/pub/ws"

struct session_data {
    struct trcache *cache;
    int symbol_id;
};

static int
callback_bithumb(struct lws *wsi, enum lws_callback_reasons reason,
                 void *user, void *in, size_t len)
{
    struct session_data *sd = (struct session_data *)user;

    switch (reason) {
    case LWS_CALLBACK_CLIENT_ESTABLISHED: {
        /* Subscribe to BTC_KRW trade feed */
        const char *sub = "{\"type\":\"trade\",\"symbols\":[\"BTC_KRW\"]}";
        lws_write(wsi, (unsigned char *)sub, strlen(sub), LWS_WRITE_TEXT);
        break;
    }
    case LWS_CALLBACK_CLIENT_RECEIVE: {
        /* Parse trade data */
        cJSON *json = cJSON_ParseWithLength(in, len);
        if (!json) {
            fprintf(stderr, "Invalid JSON\n");
            break;
        }
        cJSON *content = cJSON_GetObjectItem(json, "content");
        if (content) {
            double price = cJSON_GetObjectItem(content, "contP")->valuedouble;
            double volume = cJSON_GetObjectItem(content, "contQty")->valuedouble;
            const char *time_str = cJSON_GetObjectItem(content, "contDtm")->valuestring;
            long ts = atol(time_str); /* timestamp in ms */

            struct trcache_trade_data td = {
                .timestamp = (uint64_t)ts,
                .trade_id = 0,
                .price = price,
                .volume = volume,
            };
            trcache_feed_trade_data(sd->cache, &td, sd->symbol_id);
        }
        cJSON_Delete(json);
        break;
    }
    default:
        break;
    }
    return 0;
}

static struct lws_protocols protocols[] = {
    { "bithumb", callback_bithumb, sizeof(struct session_data), 0 },
    { NULL, NULL, 0, 0 }
};

int main(void)
{
    struct lws_context_creation_info info = {0};
    struct lws_context *context;
    struct trcache *cache;
    struct trcache_init_ctx ictx = {
        .num_worker_threads = 1,
        .batch_candle_count_pow2 = 6,
        .flush_threshold_pow2 = 2,
        .candle_type_flags = TRCACHE_1MIN_CANDLE,
        .flush_ops = { .flush = NULL }
    };

    cache = trcache_init(&ictx);
    if (!cache)
        return 1;

    int symbol_id = trcache_register_symbol(cache, "BTC_KRW");

    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    context = lws_create_context(&info);
    if (!context)
        return 1;

    struct lws_client_connect_info ccinfo = {0};
    ccinfo.context = context;
    ccinfo.address = "pubwss.bithumb.com";
    ccinfo.path = "/pub/ws";
    ccinfo.host = lws_canonical_hostname(context);
    ccinfo.origin = "origin";
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL;
    ccinfo.pwsi = NULL;

    struct session_data sd = { cache, symbol_id };
    ccinfo.userdata = &sd;

    if (!lws_client_connect_via_info(&ccinfo))
        return 1;

    while (lws_service(context, 1000) >= 0)
        ;

    lws_context_destroy(context);
    trcache_destroy(cache);
    return 0;
}
