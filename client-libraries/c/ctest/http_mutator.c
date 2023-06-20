#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "modality/error.h"
#include "modality/types.h"
#include "modality/runtime.h"
#include "modality/tracing_subscriber.h"
#include "modality/mutator_interface.h"
#include "modality/mutator_http_server.h"

#ifdef NDEBUG
#error "NDEBUG should not be defined"
#endif

#define INFO(fmt, ...) fprintf(stdout, "\033[0;37m[INFO] \033[0m " fmt "\n", ##__VA_ARGS__)
#define ERR(fmt, ...) fprintf(stdout, "\033[0;31m[ERROR]\033[0m " fmt "\n", ##__VA_ARGS__)

static int g_state = 5;
static int g_is_injected = 0;

static const modality_attr_val MIN =
{
    .tag = MODALITY_ATTR_VAL_INTEGER,
    .integer = 0,
};

static const modality_attr_val MAX =
{
    .tag = MODALITY_ATTR_VAL_INTEGER,
    .integer = 100,
};

static const modality_mutator_param_descriptor PARAM_DESCS[] =
{
    {
        .value_type = MODALITY_ATTR_TYPE_INTEGER,
        .name = "my-param",
        .description = "A test parameter",
        .value_min = &MIN,
        .value_max = &MAX,
        .default_value = NULL,
        .least_effect_value = NULL,
        .value_distribution_kind = MODALITY_VALUE_DISTRIBUTION_KIND_CONTINUOUS,
        .value_distribution_scaling = MODALITY_VALUE_DISTRIBUTION_SCALING_NONE,
        .value_distribution_option_set = NULL,
        .value_distribution_option_set_length = 0,
        .organization_custom_metadata = NULL,
    }
};

static const modality_mutator_descriptor MUT_DESC =
{
    .name = "test-mutator",
    .description = "A test mutator",
    .layer = MODALITY_MUTATOR_LAYER_IMPLEMENTATIONAL,
    .group = NULL,
    .operation = MODALITY_MUTATOR_OPERATION_SET_TO_VALUE,
    .statefulness = MODALITY_MUTATOR_STATEFULNESS_TRANSIENT,
    .organization_custom_metadata = NULL,
    .params = PARAM_DESCS,
    .params_length = 1,
};

static void get_description(void *state, const struct modality_mutator_descriptor **desc_ptr)
{
    INFO("Get description");

    assert(state == &g_state);
    assert(desc_ptr != NULL);

    (*desc_ptr) = &MUT_DESC;
}

static int inject(void *state, const struct modality_mutation_id *mid, const struct modality_attr_kv *params, size_t params_len)
{
    INFO("Inject");

    assert(state == &g_state);
    assert(mid != NULL);
    assert(params != NULL);
    assert(params_len == 1);
    g_is_injected = 1;
    return MODALITY_ERROR_OK;
}

static int reset(void *state)
{
    INFO("Reset");

    assert(state == &g_state);
    g_is_injected = 0;
    return MODALITY_ERROR_OK;
}

int main(void)
{
    int err;
    modality_runtime *rt;

    modality_http_mutator http_mutators[] =
    {
        {
            .mutator_correlation_id = "my.local.test-mutator",
            .mutator =
            {
                .state = &g_state,
                .get_description = &get_description,
                .inject = &inject,
                .reset = &reset,
            }
        }
    };

    err = modality_tracing_subscriber_init();
    assert(err == MODALITY_ERROR_OK);

    err = modality_runtime_new(&rt);
    assert(err == MODALITY_ERROR_OK);

    /* TODO compile-time or runtime argv for actually running, so just a compile check by default */
    /* rename this to http_mutator.c or w/e */
    INFO("Starting HTTP server, visit http://localhost:8080/swagger-ui/index.html");

    /* http://localhost:8080/swagger-ui/index.html */
    err = modality_mutator_http_server_run(rt, NULL, 8080, http_mutators, 1);
    assert(err == MODALITY_ERROR_OK);

    modality_runtime_free(rt);
    modality_runtime_free(NULL);

    INFO("Test complete");

    return EXIT_SUCCESS;
}
