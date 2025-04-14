/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "utilities.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <zephyr/fatal.h>
#include <zephyr/logging/log.h>
#include <zephyr/shell/shell_uart.h>
#include <zephyr/sys/bitarray.h>
#include <zephyr/sys/timeutil.h>
#include <zephyr/sys/util.h>

#include <astarte_device_sdk/data.h>
#include <astarte_device_sdk/interface.h>
#include <astarte_device_sdk/mapping.h>
#include <astarte_device_sdk/object.h>
#include <astarte_device_sdk/result.h>

#include "log.h"
#include "utils.h"

/************************************************
 * Constants, static variables and defines
 ***********************************************/

LOG_MODULE_REGISTER(utilities, CONFIG_UTILITIES_LOG_LEVEL); // NOLINT

#define CMP_ARRAY(LEFT, RIGHT, TYPE_SIZE)                                                          \
    if ((LEFT).len != (RIGHT).len) {                                                               \
        return false;                                                                              \
    }                                                                                              \
    return memcmp((LEFT).buf, (RIGHT).buf, (LEFT).len * (TYPE_SIZE)) == 0;

#define CMP_ARRAY_SIZED(LEFT, RIGHT) CMP_ARRAY(LEFT, RIGHT, sizeof((LEFT).buf[0]))

// The limit of interface mappings and following of object entries is 1024
// https://docs.astarte-platform.org/astarte/latest/040-interface_schema.html#interface
#define OBJECT_MAX_ENTRIES 1024

#define MAX_TS_STR_LEN 30

/************************************************
 *         Static functions declaration         *
 ***********************************************/

static bool cmp_string_array(astarte_data_stringarray_t *left, astarte_data_stringarray_t *right);
static bool cmp_binaryblob_array(
    astarte_data_binaryblobarray_t *left, astarte_data_binaryblobarray_t *right);
static astarte_data_t *get_object_entry_data(idata_object_entry_array *entries, const char *key);
// shell bypass callback
static void shell_bypass_halt(const struct shell *shell, uint8_t *data, size_t len);

/************************************************
 * Global functions definition
 ***********************************************/

bool astarte_object_equal(idata_object_entry_array *left, idata_object_entry_array *right)
{
    if (left->len != right->len) {
        return false;
    }

    if (left->len > OBJECT_MAX_ENTRIES) {
        ASTARTE_LOG_ERR("Number of entries of the two object (%zu) exceeds the limit imposed by "
                        "astarte protocol",
            left->len);
        return false;
    }

    if (left->len == 0) {
        return true;
    }

    SYS_BITARRAY_DEFINE(accessed_entries, OBJECT_MAX_ENTRIES);
    size_t accessed_entries_offset = { 0 };
    int bita_alloc_res = sys_bitarray_alloc(&accessed_entries, left->len, &accessed_entries_offset);

    switch (bita_alloc_res) {
        case -EINVAL:
            ASTARTE_LOG_ERR("Allocating invalid number of bits");
            return false;
        case -ENOSPC:
            ASTARTE_LOG_ERR("No space for bitarray");
            return false;
        default:
            ASTARTE_LOG_ERR("Unexpected bitarray allocation return code");
            return false;
        case 0:
            break;
    }

    sys_bitarray_clear_region(&accessed_entries, left->len, accessed_entries_offset);
    bool result = true;

    for (size_t i = 0; i < left->len; ++i) {
        const char *left_key = left->buf[i].path;
        astarte_data_t *left_value = &left->buf[i].data;

        astarte_data_t *right_value = get_object_entry_data(right, left_key);
        // check that the value exist in the right object
        if (!right_value) {
            result = false;
            goto exit;
        }

        astarte_object_entry_t *right_value_entry
            = CONTAINER_OF(right_value, astarte_object_entry_t, data);
        // assert that the right entry hasn't already been checked (no duplicate in left)
        size_t offset = right_value_entry - right->buf;
        int previous_value = { 0 };
        sys_bitarray_test_and_set_bit(&accessed_entries, offset, &previous_value);
        if (previous_value == 1) {
            result = false;
            goto exit;
        }

        // check that the value is equal in the right object
        if (!astarte_data_equal(left_value, right_value)) {
            result = false;
            goto exit;
        }
    }

exit:
    sys_bitarray_free(&accessed_entries, left->len, accessed_entries_offset);
    return result;
}

bool astarte_data_equal(astarte_data_t *left, astarte_data_t *right)
{
    if (left->tag != right->tag) {
        return false;
    }

    switch (left->tag) {
        case ASTARTE_MAPPING_TYPE_BOOLEAN:
            return left->data.boolean == right->data.boolean;
        case ASTARTE_MAPPING_TYPE_DATETIME:
            return left->data.datetime == right->data.datetime;
        case ASTARTE_MAPPING_TYPE_DOUBLE:
            return left->data.dbl == right->data.dbl;
        case ASTARTE_MAPPING_TYPE_INTEGER:
            return left->data.integer == right->data.integer;
        case ASTARTE_MAPPING_TYPE_LONGINTEGER:
            return left->data.longinteger == right->data.longinteger;
        case ASTARTE_MAPPING_TYPE_STRING:
            return strcmp(left->data.string, right->data.string) == 0;
        case ASTARTE_MAPPING_TYPE_BINARYBLOB:
            CMP_ARRAY(left->data.binaryblob, right->data.binaryblob, sizeof(uint8_t))
        case ASTARTE_MAPPING_TYPE_BOOLEANARRAY:
            CMP_ARRAY_SIZED(left->data.boolean_array, right->data.boolean_array)
        case ASTARTE_MAPPING_TYPE_DATETIMEARRAY:
            CMP_ARRAY_SIZED(left->data.datetime_array, right->data.datetime_array)
        case ASTARTE_MAPPING_TYPE_DOUBLEARRAY:
            CMP_ARRAY_SIZED(left->data.double_array, right->data.double_array)
        case ASTARTE_MAPPING_TYPE_INTEGERARRAY:
            CMP_ARRAY_SIZED(left->data.integer_array, right->data.integer_array)
        case ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY:
            CMP_ARRAY_SIZED(left->data.longinteger_array, right->data.longinteger_array)
        case ASTARTE_MAPPING_TYPE_STRINGARRAY:
            return cmp_string_array(&left->data.string_array, &right->data.string_array);
        case ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY:
            return cmp_binaryblob_array(
                &left->data.binaryblob_array, &right->data.binaryblob_array);
        default:
            // something bad happened
            CHECK_HALT(true, "Unsupported mapping type");
    }

    CHECK_HALT(true, "Unreachable, the previous switch should handle all possible cases");
}

void utils_log_timestamp(idata_timestamp_option_t *timestamp)
{
    char tm_str[DATETIME_MAX_BUF_LEN] = { 0 };
    if (timestamp->present) {
        __ASSERT(utils_datetime_to_string(timestamp->value, tm_str) != 0,
            "Buffer size for datetime conversion too small");
        LOG_INF("Timestamp: %s", tm_str); // NOLINT
    } else {
        LOG_INF("No timestamp"); // NOLINT
    }
}

void utils_log_object_entry_array(idata_object_entry_array *obj)
{
    utils_log_astarte_object(obj->buf, obj->len);
}

void block_shell_commands()
{
    // Bypass shell commands until the e2e code re-enables them
    const struct shell *uart_shell = shell_backend_uart_get_ptr();
    shell_set_bypass(uart_shell, shell_bypass_halt);
}

void unblock_shell_commands()
{
    // remove bypass to allow shell callbacks to be called
    const struct shell *uart_shell = shell_backend_uart_get_ptr();
    shell_set_bypass(uart_shell, NULL);
}

/************************************************
 *         Static functions definitions         *
 ***********************************************/

static bool cmp_string_array(astarte_data_stringarray_t *left, astarte_data_stringarray_t *right)
{
    if (left->len != right->len) {
        return false;
    }

    for (int i = 0; i < left->len; ++i) {
        if (strcmp(left->buf[i], right->buf[i]) != 0) {
            return false;
        }
    }

    return true;
}

static bool cmp_binaryblob_array(
    astarte_data_binaryblobarray_t *left, astarte_data_binaryblobarray_t *right)
{
    if (left->count != right->count) {
        return false;
    }

    for (int i = 0; i < left->count; ++i) {
        if (left->sizes[i] != right->sizes[i]) {
            return false;
        }
        if (memcmp(left->blobs[i], right->blobs[i], left->sizes[i]) != 0) {
            return false;
        }
    }

    return true;
}

static astarte_data_t *get_object_entry_data(idata_object_entry_array *entries, const char *key)
{
    for (size_t i = 0; i < entries->len; ++i) {
        if (strcmp(key, entries->buf[i].path) == 0) {
            return &entries->buf[i].data;
        }
    }

    return NULL;
}

static void shell_bypass_halt(const struct shell *shell, uint8_t *data, size_t len)
{
    ARG_UNUSED(shell);

    CHECK_HALT(len > 0 || data[0] != 10, "Shell commands are being ignored blocking execution");
}
