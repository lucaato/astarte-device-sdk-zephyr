/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "runner.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <zephyr/fatal.h>
#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/shell/shell.h>
#include <zephyr/shell/shell_uart.h>
#include <zephyr/sys/bitarray.h>
#include <zephyr/sys/util.h>

#include <astarte_device_sdk/data.h>
#include <astarte_device_sdk/device.h>
#include <astarte_device_sdk/interface.h>
#include <astarte_device_sdk/mapping.h>
#include <astarte_device_sdk/object.h>
#include <astarte_device_sdk/pairing.h>
#include <astarte_device_sdk/result.h>
#include <astarte_device_sdk/util.h>
#include <data_private.h>
#include <interface_private.h>
#include <object_private.h>

#include "device_handler.h"
#include "idata.h"
#include "shell_handlers.h"
#include "utilities.h"

#include "astarte_generated_interfaces.h"
#include "utils.h"

/************************************************
 * Shell commands declaration
 ***********************************************/

SHELL_STATIC_SUBCMD_SET_CREATE(expect_property_subcommand,
    SHELL_CMD_ARG(set, NULL,
        "Expect a property with the data passed as argument."
        " This command expects <interface_name> <path> <bson_value>",
        cmd_expect_property_set_handler, 4, 0),
    SHELL_CMD_ARG(unset, NULL,
        "Expect an unset of the property with the data passed as argument."
        " This command expects <interface_name> <path>",
        cmd_expect_property_unset_handler, 3, 0),
    SHELL_SUBCMD_SET_END);

SHELL_STATIC_SUBCMD_SET_CREATE(expect_subcommand,
    SHELL_CMD_ARG(individual, NULL,
        "Expect an individual property from the device with the data passed as argument."
        " This command expects <interface_name> <path> <bson_value> <optional_timestamp>",
        cmd_expect_individual_handler, 4, 1),
    SHELL_CMD_ARG(object, NULL,
        "Expect an object with the data passed as argument."
        " This command expects <interface_name> <path> <bson_value> <optional_timestamp>",
        cmd_expect_object_handler, 4, 1),
    SHELL_CMD(property, &expect_property_subcommand, "Expect a property.", NULL),
    SHELL_CMD(verify, NULL, "Check that all the expected messages got received.",
        cmd_expect_verify_handler),
    SHELL_SUBCMD_SET_END);

SHELL_STATIC_SUBCMD_SET_CREATE(send_property_subcommand,
    SHELL_CMD_ARG(set, NULL,
        "Set a property with the data passed as argument."
        " This command expects <interface_name> <path> <bson_value>",
        cmd_send_property_set_handler, 4, 0),
    SHELL_CMD_ARG(unset, NULL,
        "Unset a property with the data passed as argument."
        " This command expects <interface_name> <path>",
        cmd_send_property_unset_handler, 3, 0),
    SHELL_SUBCMD_SET_END);

SHELL_STATIC_SUBCMD_SET_CREATE(send_subcommand,
    SHELL_CMD_ARG(individual, NULL,
        "Send an individual property from the device with the data passed as argument."
        " This command expects <interface_name> <path> <bson_value> <optional_timestamp>",
        cmd_send_individual_handler, 4, 1),
    SHELL_CMD_ARG(object, NULL,
        "Send an object from the device with the data passed as argument."
        " This command expects <interface_name> <path> <bson_value> <optional_timestamp>",
        cmd_send_object_handler, 4, 1),
    SHELL_CMD(property, &send_property_subcommand, "Handle send of property interfaces subcommand.",
        NULL),
    SHELL_SUBCMD_SET_END);

// TODO if the return code of a command is 1 i would expect the e2etest to fail currently however
// this is not the case and everything continues as normal
SHELL_CMD_REGISTER(expect, &expect_subcommand, "Set the data expected from the server", NULL);
SHELL_CMD_REGISTER(send, &send_subcommand, "Send device data", NULL);
SHELL_CMD_REGISTER(
    disconnect, NULL, "Disconnect the device and end the executable", cmd_disconnect);

/************************************************
 * Constants, static variables and defines
 ***********************************************/

LOG_MODULE_REGISTER(runner, CONFIG_RUNNER_LOG_LEVEL); // NOLINT

const astarte_interface_t *interfaces[] = {
    &org_astarte_platform_zephyr_e2etest_DeviceAggregate,
    &org_astarte_platform_zephyr_e2etest_DeviceDatastream,
    &org_astarte_platform_zephyr_e2etest_DeviceProperty,
    &org_astarte_platform_zephyr_e2etest_ServerAggregate,
    &org_astarte_platform_zephyr_e2etest_ServerDatastream,
    &org_astarte_platform_zephyr_e2etest_ServerProperty,
};

/************************************************
 * Static functions declaration
 ***********************************************/

static void device_thread_entry_point(void *device_handle, void *unused1, void *unused2);
static void connection_callback(astarte_device_connection_event_t event);
static void disconnection_callback(astarte_device_disconnection_event_t event);
// device data callbacks
static void device_individual_callback(astarte_device_datastream_individual_event_t event);
static void device_object_callback(astarte_device_datastream_object_event_t event);
static void device_property_set_callback(astarte_device_property_set_event_t event);
static void device_property_unset_callback(astarte_device_data_event_t event);
// --
/**
 * This function generates a unique key from a interface name.
 */
// NOTE change this function if interfaces name, or set of interfaces, change. It relies
// on current interface names to create a simple but unique hash.
// If more interfaces are added this function should also change.
static uint64_t interfaces_perfect_hash(char* key_string, size_t len);

/************************************************
 * Global functions definition
 ***********************************************/

// guarded by the semaphore e2e_run_semaphore
void run_e2e_test()
{
    LOG_INF("Running e2e test"); // NOLINT
    // initialize idata, no need to lock since no shell is active currenlty
    idata_init(interfaces, ARRAY_SIZE(interfaces), interfaces_perfect_hash);
    // sets up the global device_handle
    astarte_device_config_t config = {
        .device_id = CONFIG_DEVICE_ID,
        .cred_secr = CONFIG_CREDENTIAL_SECRET,
        .interfaces = interfaces,
        .interfaces_size = ARRAY_SIZE(interfaces),
        .http_timeout_ms = CONFIG_HTTP_TIMEOUT_MS,
        .mqtt_connection_timeout_ms = CONFIG_MQTT_CONNECTION_TIMEOUT_MS,
        .mqtt_poll_timeout_ms = CONFIG_MQTT_POLL_TIMEOUT_MS,
        .datastream_individual_cbk = device_individual_callback,
        .datastream_object_cbk = device_object_callback,
        .property_set_cbk = device_property_set_callback,
        .property_unset_cbk = device_property_unset_callback,
        .connection_cbk = connection_callback,
        .disconnection_cbk = disconnection_callback,
    };
    device_setup(config);

    // wait while device connects
    wait_for_connection();

    // we are ready to send and receive data
    const struct shell *uart_shell = shell_backend_uart_get_ptr();
    shell_start(uart_shell);
    shell_print(uart_shell, "Device shell ready");

    // the disconnection happens when the "disconnect" shell command gets called
    // afterwards the device is deleted
    wait_for_destroyed_device();

    shell_print(uart_shell, "Disconnected, closing shell...");
    shell_stop(uart_shell);

    idata_free();
}

/************************************************
 * Static functions definitions
 ***********************************************/

static void device_individual_callback(astarte_device_datastream_individual_event_t event)
{
    LOG_INF("Individual datastream callback");

    e2e_individual_data_t *individual = { 0 };
    // inizialization
    idata_get_individual(&expected_data, event.base_event.interface_name, &individual);

    for (; individual != NULL;
        idata_get_individual(&expected_data, event.base_event.interface_name, &individual)) {
        if (strcmp(individual->path, event.base_event.path) != 0) {
            // skip element if on a different path
            continue;
        }

        LOG_DBG("Comparing values\nExpected:");
        utils_log_astarte_data(individual->data);
        LOG_DBG("Received:");
        utils_log_astarte_data(event.data);

        if (!astarte_data_equal(&individual->data, &event.data)) {
            LOG_ERR("Unexpected element received on path '%s'", individual->path);
            unexpected_data_count += 1;
            goto unlock;
        }

        LOG_INF("Received expected value on '%s' '%s'", event.base_event.interface_name,
            individual->path);

        utils_log_astarte_data(event.data);

        expected_data_count += 1;
        idata_remove_individual(individual);

        goto unlock;
    }

    LOG_ERR("No more expected individual but got data on interface '%s'",
        event.base_event.interface_name);

unlock:
    k_mutex_unlock(&expected_data_mutex);
}

static void device_object_callback(astarte_device_datastream_object_event_t event)
{
    LOG_INF("Object datastream callback");

    CHECK_HALT(
        k_mutex_lock(&expected_data_mutex, K_FOREVER) != 0, "Could not lock expected data mutex");
    if (idata_is_empty(&expected_data)) {
        LOG_INF("Idata is empty not performing any check");
        goto unlock;
    }

    e2e_object_entry_array_t received = {
        .buf = event.entries,
        .len = event.entries_len,
    };

    e2e_object_data_t *object = NULL;
    // inizialization
    idata_get_object(&expected_data, event.base_event.interface_name, &object);

    for (; object != NULL;
        idata_get_object(&expected_data, event.base_event.interface_name, &object)) {
        if (strcmp(object->path, event.base_event.path) != 0) {
            // skip element if on a different path
            continue;
        }

        if (!astarte_object_equal(&object->entries, &received)) {
            LOG_ERR("Unexpected element received on path '%s'", object->path);
            unexpected_data_count += 1;
            goto unlock;
        }

        LOG_INF(
            "Received expected value on '%s' '%s'", event.base_event.interface_name, object->path);

        expected_data_count += 1;
        idata_remove_object(object);

        goto unlock;
    }

    LOG_ERR(
        "No more expected objects but got data on interface '%s'", event.base_event.interface_name);

unlock:
    k_mutex_unlock(&expected_data_mutex);
}

static void device_property_set_callback(astarte_device_property_set_event_t event)
{
    LOG_INF("Property set callback");

    CHECK_HALT(
        k_mutex_lock(&expected_data_mutex, K_FOREVER) != 0, "Could not lock expected data mutex");
    if (idata_is_empty(&expected_data)) {
        LOG_INF("Idata is empty not performing any check");
        goto unlock;
    }

    e2e_property_data_t *property = NULL;
    // inizialization
    idata_get_property(&expected_data, event.base_event.interface_name, &property);

    for (; property != NULL;
        idata_get_property(&expected_data, event.base_event.interface_name, &property)) {
        if (strcmp(property->path, event.base_event.path) != 0) {
            // skip element if on a different path
            continue;
        }

        LOG_DBG("Comparing values\nExpected:");
        utils_log_astarte_data(property->data);
        LOG_DBG("Received:");
        utils_log_astarte_data(event.data);

        if (!astarte_data_equal(&property->data, &event.data)) {
            LOG_ERR("Unexpected element received on path '%s'", property->path);
            unexpected_data_count += 1;
            goto unlock;
        }

        LOG_INF("Received expected value on '%s' '%s'", event.base_event.interface_name,
            property->path);

        utils_log_astarte_data(event.data);

        expected_data_count += 1;
        idata_remove_property(property);

        goto unlock;
    }

    LOG_ERR("No more expected properties but got data on interface '%s'",
        event.base_event.interface_name);

unlock:
    k_mutex_unlock(&expected_data_mutex);
}

static void device_property_unset_callback(astarte_device_data_event_t event)
{
    LOG_INF("Property unset callback");

    CHECK_HALT(
        k_mutex_lock(&expected_data_mutex, K_FOREVER) != 0, "Could not lock expected data mutex");
    if (idata_is_empty(&expected_data)) {
        LOG_INF("Idata is empty not performing any check");
        goto unlock;
    }

    e2e_property_data_t *property = NULL;
    // inizialization
    idata_get_property(&expected_data, event.interface_name, &property);

    for (; property != NULL; idata_get_property(&expected_data, event.interface_name, &property)) {
        if (strcmp(property->path, event.path) != 0) {
            // skip element if on a different path
            continue;
        }

        if (!property->unset) {
            LOG_ERR("Unexpected unset received on path '%s'", property->path);
            unexpected_data_count += 1;
            goto unlock;
        }

        LOG_INF("Received expected unset on '%s' '%s'", event.interface_name, property->path);

        expected_data_count += 1;
        idata_remove_property(property);

        goto unlock;
    }

    LOG_ERR("No more expected unsets but got data on interface '%s'", event.interface_name);

unlock:
    k_mutex_unlock(&expected_data_mutex);
}

static void connection_callback(astarte_device_connection_event_t event)
{
    (void) event;
    LOG_INF("Astarte device connected"); // NOLINT
    set_connected();
}

static void disconnection_callback(astarte_device_disconnection_event_t event)
{
    (void) event;
    LOG_INF("Astarte device disconnected"); // NOLINT
    set_disconnected();
}

static uint64_t interfaces_perfect_hash(char* key_string, size_t len) {
    const char interface_dname[] = "org.astarte-platform.zephyr.e2etest.";
    const size_t interface_dname_sd_idetifier = 36;
    const size_t interface_dname_adp_idetifier = 43;

    // check that the string is a known interface name and has enough characters for our check
    CHECK_HALT(strstr(key_string, interface_dname) == key_string && len > 44,
        "Received an invalid or unexpected interface name, please update the hash function");

    uint32_t result = { 0 };
    uint8_t *result_bytes = (uint8_t *) &result;
    result_bytes[0] = key_string[interface_dname_sd_idetifier];
    result_bytes[1] = key_string[interface_dname_sd_idetifier];
    result_bytes[2] = key_string[interface_dname_adp_idetifier];
    result_bytes[3] = key_string[interface_dname_adp_idetifier];

    return result;
}
