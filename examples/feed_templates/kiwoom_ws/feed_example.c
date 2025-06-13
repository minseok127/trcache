#include <libwebsockets.h>
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../trcache.h"

/*
 * Example template for receiving realtime data from the Kiwoom
 * Securities websocket API and feeding it into trcache.
 *
 * Kiwoom provides a websocket service as part of their OpenAPI.
 * Authentication and detailed message formats are omitted here;
 * consult the Kiwoom API documentation for the exact protocol.
 */

#define KIWOOM_WSS "wss://openapi.kiwoom.com/websocket"

struct session_data {
    struct trcache *cache;
    int symbol_id;
};

static int
callback_kiwoom(struct lws *wsi, enum lws_callback_reasons reason,
                void *user, void *in, size_t len)
{
    struct session_data *sd = (struct session_data *)user;

    switch (reason) {
    case LWS_CALLBACK_CLIENT_ESTABLISHED: {
        /* Send login or subscription message here */
        const char *sub = "{\"type\":\"subscribe\",\"code\":\"A005930\"}";
        lws_write(wsi, (unsigned char *)sub, strlen(sub), LWS_WRITE_TEXT);
        break;
    }
    case LWS_CALLBACK_CLIENT_RECEIVE: {
        /* Parse trade or quote data */
        cJSON *json = cJSON_ParseWithLength(in, len);
        if (!json)
            break;
        cJSON *price = cJSON_GetObjectItem(json, "price");
        cJSON *volume = cJSON_GetObjectItem(json, "volume");
        cJSON *ts = cJSON_GetObjectItem(json, "timestamp");
        if (price && volume && ts) {
            struct trcache_trade_data td = {
                .timestamp = (uint64_t)ts->valuedouble,
                .trade_id = 0,
                .price = price->valuedouble,
                .volume = volume->valuedouble,
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
    { "kiwoom", callback_kiwoom, sizeof(struct session_data), 0 },
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

    int symbol_id = trcache_register_symbol(cache, "KIWOOM_CODE");

    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    context = lws_create_context(&info);
    if (!context)
        return 1;

    struct lws_client_connect_info ccinfo = {0};
    ccinfo.context = context;
    ccinfo.address = "openapi.kiwoom.com";
    ccinfo.path = "/websocket";
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
