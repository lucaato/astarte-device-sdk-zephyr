/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef E2EUTILITIES_H
#define E2EUTILITIES_H

#include <stdint.h>

#include <zephyr/fatal.h>

#include <astarte_device_sdk/data.h>
#include <astarte_device_sdk/interface.h>
#include <astarte_device_sdk/object.h>
#include <astarte_device_sdk/util.h>

/**
 * @file e2eutilities.h
 * @brief Utilities needed in e2e tests
 */

#define CHECK_HALT(expr, ...)                                                                      \
    if (expr) {                                                                                    \
        LOG_ERR(__VA_ARGS__); /* NOLINT */                                                         \
        k_fatal_halt(-1);                                                                          \
    }

#define CHECK_ASTARTE_OK_HALT(expr, ...) CHECK_HALT(expr != ASTARTE_RESULT_OK, __VA_ARGS__)

#define CHECK_RET_1(expr, ...)                                                                     \
    if (expr) {                                                                                    \
        LOG_ERR(__VA_ARGS__); /* NOLINT */                                                         \
        return 1;                                                                                  \
    }

#define CHECK_ASTARTE_OK_RET_1(expr, ...) CHECK_RET_1(expr != ASTARTE_RESULT_OK, __VA_ARGS__)

#define CHECK_GOTO(expr, label, ...)                                                               \
    if (expr) {                                                                                    \
        LOG_ERR(__VA_ARGS__); /* NOLINT */                                                         \
        goto label;                                                                                \
    }

#define CHECK_ASTARTE_OK_GOTO(expr, label, ...)                                                    \
    CHECK_GOTO(expr != ASTARTE_RESULT_OK, label, __VA_ARGS__)

// clang-format on

// Timestamp option used to store a valid timestamp value and its presence
typedef struct
{
    int64_t value;
    bool present;
} e2e_timestamp_option_t;

ASTARTE_UTIL_DEFINE_ARRAY(e2e_byte_array, uint8_t);

ASTARTE_UTIL_DEFINE_ARRAY(e2e_object_entry_array_t, astarte_object_entry_t);

void astarte_object_print(e2e_object_entry_array_t *obj);

bool astarte_object_equal(e2e_object_entry_array_t *left, e2e_object_entry_array_t *right);
bool astarte_data_equal(astarte_data_t *left, astarte_data_t *right);

void skip_parameter(char ***args, size_t *argc);
e2e_timestamp_option_t next_timestamp_parameter(char ***args, size_t *argc);
// the return of this function needs to be deallocated
char *next_alloc_string_parameter(char ***args, size_t *argc);
// the return of this function needs to be deallocated
e2e_byte_array next_alloc_base64_parameter(char ***args, size_t *argc);

#endif /* E2EUTILITIES_H */
