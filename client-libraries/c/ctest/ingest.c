#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "modality/error.h"
#include "modality/types.h"
#include "modality/runtime.h"
#include "modality/tracing_subscriber.h"
#include "modality/ingest_client.h"

#ifdef NDEBUG
#error "NDEBUG should not be defined"
#endif

#ifndef AUTH_TOKEN_HEX
#error "AUTH_TOKEN_HEX is not defined"
#endif

#ifndef RUN_ID
#error "RUN_ID is not defined"
#endif

#define INFO(fmt, ...) fprintf(stdout, "\033[0;37m[INFO] \033[0m " fmt "\n", ##__VA_ARGS__)
#define ERR(fmt, ...) fprintf(stdout, "\033[0;31m[ERROR]\033[0m " fmt "\n", ##__VA_ARGS__)

#define NUM_ATTRS (10)

static const char *TIMELINE_ATTR_KEYS[] =
{
    "timeline.foo.timeline.id.type",
    "timeline.foo.string.type",
    "timeline.foo.int.type",
    "timeline.foo.big_int.type",
    "timeline.foo.float.type",
    "timeline.foo.bool.type",
    "timeline.foo.timestamp.type",
    "timeline.foo.logical_time.type",
    "timeline.run_id",
    "timeline.name",
};

static const char *EVENT_ATTR_KEYS[] =
{
    "event.bar.timeline.id.type",
    "event.bar.string.type",
    "event.bar.int.type",
    "event.bar.big_int.type",
    "event.bar.float.type",
    "event.bar.bool.type",
    "event.bar.timestamp.type",
    "event.bar.logical_time.type",
    "event.run_id",
    "event.name",
};

int main(void)
{
    int i;
    int err;
    modality_runtime *rt;
    modality_ingest_client *client;
    modality_timeline_id tid;
    modality_big_int big_int;
    modality_logical_time lt;
    modality_attr timeline_attrs[NUM_ATTRS] = {0};
    modality_attr event_attrs[NUM_ATTRS] = {0};

    err = modality_tracing_subscriber_init();
    assert(err == MODALITY_ERROR_OK);

    err = modality_runtime_new(&rt);
    assert(err == MODALITY_ERROR_OK);

    err = modality_ingest_client_new(rt, &client);
    assert(err == MODALITY_ERROR_OK);

    const int allow_insecure_tls = 1;
    err = modality_ingest_client_connect(client, "modality-ingest://localhost:14182", allow_insecure_tls);
    assert(err == MODALITY_ERROR_OK);

    const char *token = AUTH_TOKEN_HEX;
    err = modality_ingest_client_authenticate(client, token);
    assert(err == MODALITY_ERROR_OK);

    for(i = 0; i < NUM_ATTRS; i += 1)
    {
        err = modality_ingest_client_declare_attr_key(client, TIMELINE_ATTR_KEYS[i], &timeline_attrs[i].key);
        assert(err == MODALITY_ERROR_OK);
    }

    for(i = 0; i < NUM_ATTRS; i += 1)
    {
        err = modality_ingest_client_declare_attr_key(client, EVENT_ATTR_KEYS[i], &event_attrs[i].key);
        assert(err == MODALITY_ERROR_OK);
    }

    err = modality_timeline_id_init(&tid);
    assert(err == MODALITY_ERROR_OK);

    err = modality_big_int_set(&big_int, 0xFF, 0xFF000000000000FF);
    assert(err == MODALITY_ERROR_OK);

    uint64_t bi_lsb;
    uint64_t bi_msb;
    err = modality_big_int_get(&big_int, &bi_lsb, &bi_msb);
    assert(err == MODALITY_ERROR_OK);
    assert(bi_lsb == 0xFF);
    assert(bi_msb == 0xFF000000000000FF);

    err = modality_logical_time_set_unary(&lt, 0xFF);
    assert(err == MODALITY_ERROR_OK);
    err = modality_logical_time_set_trinary(&lt, 0xAA, 0xBB, 0xCC);
    assert(err == MODALITY_ERROR_OK);
    err = modality_logical_time_set_quaternary(&lt, 0xAA, 0xBB, 0xCC, 0xDD);
    assert(err == MODALITY_ERROR_OK);
    err = modality_logical_time_set_binary(&lt, 11, 22);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_timeline_id(&timeline_attrs[0].val, &tid);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_string(&timeline_attrs[1].val, "some string");
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_integer(&timeline_attrs[2].val, 3);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_big_int(&timeline_attrs[3].val, &big_int);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_float(&timeline_attrs[4].val, 1.23);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_bool(&timeline_attrs[5].val, true);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_timestamp(&timeline_attrs[6].val, 12345);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_logical_time(&timeline_attrs[7].val, &lt);
    assert(err == MODALITY_ERROR_OK);

    err = modality_attr_val_set_integer(&timeline_attrs[8].val, RUN_ID);
    assert(err == MODALITY_ERROR_OK);

    for(i = 0; i < NUM_ATTRS; i += 1)
    {
        memcpy(&event_attrs[i].val, &timeline_attrs[i].val, sizeof(modality_attr_val));
    }

    err = modality_attr_val_set_string(&timeline_attrs[9].val, "some-timeline-name");
    assert(err == MODALITY_ERROR_OK);
    err = modality_attr_val_set_string(&event_attrs[9].val, "some-event-name");
    assert(err == MODALITY_ERROR_OK);

    err = modality_ingest_client_open_timeline(client, &tid);
    assert(err == MODALITY_ERROR_OK);

    err = modality_ingest_client_timeline_metadata(client, timeline_attrs, NUM_ATTRS);
    assert(err == MODALITY_ERROR_OK);

    err = modality_ingest_client_event(client, 1, 0, event_attrs, NUM_ATTRS);
    assert(err == MODALITY_ERROR_OK);

    err = modality_ingest_client_close_timeline(client);
    assert(err == MODALITY_ERROR_OK);

    modality_ingest_client_free(client);
    modality_ingest_client_free(NULL);

    modality_runtime_free(rt);
    modality_runtime_free(NULL);

    INFO("Test complete");

    return EXIT_SUCCESS;
}
