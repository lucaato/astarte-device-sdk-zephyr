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
#include <zephyr/sys/base64.h>
#include <zephyr/sys/bitarray.h>
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

/************************************************
 *         Static functions declaration         *
 ***********************************************/

static bool cmp_string_array(astarte_data_stringarray_t *left, astarte_data_stringarray_t *right);
static bool cmp_binaryblob_array(
    astarte_data_binaryblobarray_t *left, astarte_data_binaryblobarray_t *right);
static astarte_data_t *get_object_entry_data(e2e_object_entry_array_t *entries, const char *key);

/************************************************
 * Global functions definition
 ***********************************************/

bool astarte_object_equal(e2e_object_entry_array_t *left, e2e_object_entry_array_t *right)
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

void astarte_object_print(e2e_object_entry_array_t *obj)
{
    for (size_t i = 0; i < obj->len; ++i) {
        ASTARTE_LOG_INF("Path: %s", obj->buf[i].path);
        utils_log_astarte_data(obj->buf[i].data);
    }
}

void skip_parameter(char ***args, size_t *argc)
{
    if (*argc < 1) {
        // no more arguments
        return;
    }

    *args += 1;
    *argc -= 1;
}

char *next_alloc_string_parameter(char ***args, size_t *argc)
{
    if (*argc < 1) {
        // no more arguments
        return NULL;
    }

    const char *const arg = (*args)[0];

    size_t arg_len = strlen(arg);
    char *const copied_arg = calloc(arg_len + 1, sizeof(char));
    CHECK_HALT(!copied_arg, "Could not copy string parameter");
    memcpy(copied_arg, arg, arg_len + 1);

    // move to the next parameter for caller
    *args += 1;
    *argc -= 1;
    return (char *) copied_arg;
}

e2e_byte_array next_alloc_base64_parameter(char ***args, size_t *argc)
{
    if (*argc < 1) {
        // no more arguments
        return (e2e_byte_array) {};
    }

    const char *const arg = (*args)[0];
    const size_t arg_len = strlen(arg);

    size_t byte_array_length = 0;
    int res = base64_decode(NULL, 0, &byte_array_length, arg, arg_len);
    if (byte_array_length == 0) {
        LOG_ERR("Error while computing base64 decode buffer length: %d", res); // NOLINT
        return (e2e_byte_array) {};
    }

    LOG_DBG("The size of the decoded buffer is: %d", byte_array_length); // NOLINT

    uint8_t *const byte_array = calloc(byte_array_length, sizeof(uint8_t));
    CHECK_HALT(!byte_array, "Out of memory");

    res = base64_decode(byte_array, byte_array_length, &byte_array_length, arg, arg_len);
    if (res != 0) {
        LOG_ERR("Error while decoding base64 argument %d", res); // NOLINT
        return (e2e_byte_array) {};
    }

    // move to the next parameter for caller
    *args += 1;
    *argc -= 1;
    return (e2e_byte_array) {
        .buf = byte_array,
        .len = byte_array_length,
    };
}

e2e_timestamp_option_t next_timestamp_parameter(char ***args, size_t *argc)
{
    const int base = 10;

    if (*argc < 1) {
        // no more arguments
        return (e2e_timestamp_option_t) {};
    }

    const char *const arg = (*args)[0];
    const int64_t timestamp = (int64_t) strtoll(arg, NULL, base);

    // move to the next parameter for caller
    *args += 1;
    *argc -= 1;
    return (e2e_timestamp_option_t) {
        .value = timestamp,
        .present = true,
    };
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

static astarte_data_t *get_object_entry_data(e2e_object_entry_array_t *entries, const char *key)
{
    for (size_t i = 0; i < entries->len; ++i) {
        if (strcmp(key, entries->buf[i].path) == 0) {
            return &entries->buf[i].data;
        }
    }

    return NULL;
}
