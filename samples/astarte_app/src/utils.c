/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "utils.h"

#include <stdlib.h>

#include <zephyr/logging/log.h>
#include <zephyr/sys/timeutil.h>

LOG_MODULE_REGISTER(utils, CONFIG_APP_LOG_LEVEL); // NOLINT

/************************************************
 * Constants, static variables and defines
 ***********************************************/

// NOLINTBEGIN(readability-magic-numbers, cppcoreguidelines-avoid-magic-numbers)
const uint8_t utils_binary_blob_data[8] = { 0x53, 0x47, 0x56, 0x73, 0x62, 0x47, 0x38, 0x3d };
static const uint8_t binblob_1[8] = { 0x53, 0x47, 0x56, 0x73, 0x62, 0x47, 0x38, 0x3d };
static const uint8_t binblob_2[5] = { 0x64, 0x32, 0x39, 0x79, 0x62 };
const uint8_t *const utils_binary_blobs_data[2] = { binblob_1, binblob_2 };
const size_t utils_binary_blobs_sizes_data[2] = { ARRAY_SIZE(binblob_1), ARRAY_SIZE(binblob_2) };
const bool utils_boolean_data = true;
const bool utils_boolean_array_data[3] = { true, false, true };
const int64_t utils_unix_time_data = 1710940988984;
const int64_t utils_unix_time_array_data[1] = { 1710940988984 };
const double utils_double_data = 15.42;
const double utils_double_array_data[2] = { 1542.25, 88852.6 };
const int32_t utils_integer_data = 42;
const int32_t utils_integer_array_data[3] = { 4525, 0, 11 };
const int64_t utils_longinteger_data = 8589934592;
const int64_t utils_longinteger_array_data[3] = { 8589930067, 42, 8589934592 };
const char utils_string_data[] = "Hello world!";
const char *const utils_string_array_data[2] = { "Hello ", "world!" };
// NOLINTEND(readability-magic-numbers, cppcoreguidelines-avoid-magic-numbers)

/************************************************
 * Global functions definition
 ***********************************************/

// NOLINTNEXTLINE(hicpp-function-size)
void utils_log_astarte_data(astarte_data_t data)
{
    char tm_str[DATETIME_MAX_BUF_LEN] = { 0 };

    switch (astarte_data_get_type(data)) {
        case ASTARTE_MAPPING_TYPE_BINARYBLOB:
            void *blob = NULL;
            size_t blob_len = 0;
            (void) astarte_data_to_binaryblob(data, &blob, &blob_len);
            LOG_HEXDUMP_INF(blob, blob_len, "Astarte binaryblob:"); // NOLINT
            break;
        case ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY:
            LOG_INF("Astarte binaryblobarray:"); // NOLINT
            const void **blobs = NULL;
            size_t *sizes = NULL;
            size_t count = 0;
            (void) astarte_data_to_binaryblob_array(data, &blobs, &sizes, &count);
            for (size_t i = 0; i < count; i++) {
                LOG_HEXDUMP_INF(blobs[i], sizes[i], ""); // NOLINT
            }
            break;
        case ASTARTE_MAPPING_TYPE_BOOLEAN:
            bool boolean = false;
            (void) astarte_data_to_boolean(data, &boolean);
            LOG_INF("Astarte boolean: %s", (boolean) ? "true" : "false"); // NOLINT
            break;
        case ASTARTE_MAPPING_TYPE_BOOLEANARRAY:
            LOG_INF("Astarte booleanarray:"); // NOLINT
            bool *bools = NULL;
            size_t bools_len = 0;
            (void) astarte_data_to_boolean_array(data, &bools, &bools_len);
            for (size_t i = 0; i < bools_len; i++) {
                LOG_INF("    %zi: %s", i, (bools[i]) ? "true" : "false"); // NOLINT
            }
            break;
        case ASTARTE_MAPPING_TYPE_DATETIME:
            int64_t datetime = false;
            (void) astarte_data_to_datetime(data, &datetime);
            __ASSERT(utils_datetime_to_string(datetime, tm_str) != 0,
                "Buffer size for datetime conversion too small");
            LOG_INF("Astarte datetime: %s", tm_str); // NOLINT
            break;
        case ASTARTE_MAPPING_TYPE_DATETIMEARRAY:
            LOG_INF("Astarte datetimearray:"); // NOLINT
            int64_t *datetimes = NULL;
            size_t datetimes_len = 0;
            (void) astarte_data_to_datetime_array(data, &datetimes, &datetimes_len);
            for (size_t i = 0; i < datetimes_len; i++) {
                __ASSERT(utils_datetime_to_string(datetimes[i], tm_str),
                    "Buffer size for datetime conversion too small");
                LOG_INF("    %zi: %s", i, tm_str); // NOLINT
            }
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLE:
            double dbl = false;
            (void) astarte_data_to_double(data, &dbl);
            LOG_INF("Astarte double: %f", dbl); // NOLINT
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLEARRAY:
            LOG_INF("Astarte doublearray:"); // NOLINT
            double *doubles = NULL;
            size_t doubles_len = 0;
            (void) astarte_data_to_double_array(data, &doubles, &doubles_len);
            for (size_t i = 0; i < doubles_len; i++) {
                LOG_INF("    %zi: %f", i, doubles[i]); // NOLINT
            }
            break;
        case ASTARTE_MAPPING_TYPE_INTEGER:
            int32_t integer = false;
            (void) astarte_data_to_integer(data, &integer);
            LOG_INF("Astarte integer: %i", integer); // NOLINT
            break;
        case ASTARTE_MAPPING_TYPE_INTEGERARRAY:
            LOG_INF("Astarte integerarray:"); // NOLINT
            int32_t *integers = NULL;
            size_t integers_len = 0;
            (void) astarte_data_to_integer_array(data, &integers, &integers_len);
            for (size_t i = 0; i < integers_len; i++) {
                LOG_INF("    %zi: %i", i, integers[i]); // NOLINT
            }
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGER:
            int64_t longinteger = false;
            (void) astarte_data_to_longinteger(data, &longinteger);
            LOG_INF("Astarte longinteger: %lli", longinteger); // NOLINT
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY:
            LOG_INF("Astarte longintegerarray:"); // NOLINT
            int64_t *longintegers = NULL;
            size_t longintegers_len = 0;
            (void) astarte_data_to_longinteger_array(data, &longintegers, &longintegers_len);
            for (size_t i = 0; i < longintegers_len; i++) {
                LOG_INF("    %zi: %lli", i, longintegers[i]); // NOLINT
            }
            break;
        case ASTARTE_MAPPING_TYPE_STRING:
            const char *string = false;
            (void) astarte_data_to_string(data, &string);
            LOG_INF("Astarte string: %s", string); // NOLINT
            break;
        case ASTARTE_MAPPING_TYPE_STRINGARRAY:
            LOG_INF("Astarte stringarray:"); // NOLINT
            const char **strings = NULL;
            size_t strings_len = 0;
            (void) astarte_data_to_string_array(data, &strings, &strings_len);
            for (size_t i = 0; i < strings_len; i++) {
                LOG_INF("    %zi: %s", i, strings[i]); // NOLINT
            }
            break;
        default:
            LOG_ERR("Astarte data has invalid tag!"); // NOLINT
            break;
    }
}

void utils_log_astarte_object(astarte_object_entry_t *entries, size_t entries_length)
{
    LOG_INF("Astarte object:"); // NOLINT

    for (size_t i = 0; i < entries_length; i++) {
        const char *mapping_path = NULL;
        astarte_data_t data = { 0 };
        astarte_result_t astarte_rc
            = astarte_object_entry_to_path_and_data(entries[i], &mapping_path, &data);
        if (astarte_rc == ASTARTE_RESULT_OK) {
            LOG_INF("Mapping path: %s", mapping_path); // NOLINT
            utils_log_astarte_data(data);
        }
    }
}

size_t utils_datetime_to_string(int64_t datetime, char out_string[const DATETIME_MAX_BUF_LEN])
{
    struct tm *tm_obj = gmtime(&datetime);
    return strftime(out_string, DATETIME_MAX_BUF_LEN - 1, "%Y-%m-%dT%H:%M:%S%z", tm_obj);
}
