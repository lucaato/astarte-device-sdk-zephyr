/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef E2EUTILITIES_H
#define E2EUTILITIES_H

#include <stdint.h>

#include <zephyr/fatal.h>
#include <zephyr/shell/shell.h>

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
        __VA_OPT__(LOG_ERR(__VA_ARGS__)); /* NOLINT */                                             \
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

// Timestamp option used to store a valid timestamp value and its presence
typedef struct
{
    int64_t value;
    bool present;
} idata_timestamp_option_t;

ASTARTE_UTIL_DEFINE_ARRAY(idata_byte_array, uint8_t);

ASTARTE_UTIL_DEFINE_ARRAY(idata_object_entry_array, astarte_object_entry_t);

void utils_log_timestamp(idata_timestamp_option_t *timestamp);
void utils_log_object_entry_array(idata_object_entry_array *obj);

bool astarte_object_equal(idata_object_entry_array *left, idata_object_entry_array *right);
bool astarte_data_equal(astarte_data_t *left, astarte_data_t *right);

// should be called at the start of the application to avoid user input before
// the shell is actually ready and the device connected
void block_shell_commands();
// lifts the block on the shell commands
void unblock_shell_commands();

#endif /* E2EUTILITIES_H */
