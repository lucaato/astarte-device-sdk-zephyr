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

#include "device_handler.h"
#include "idata.h"
#include "shell_handlers.h"
#include "utilities.h"
// used when CONFIG_LOG_ONLY is enabled
#include "utils.h"

#include "astarte_generated_interfaces.h"

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

// device callbacks declaration
static void device_individual_callback(astarte_device_datastream_individual_event_t event);
static void device_object_callback(astarte_device_datastream_object_event_t event);
static void device_property_set_callback(astarte_device_property_set_event_t event);
static void device_property_unset_callback(astarte_device_data_event_t event);
// --
// checks that the interface data is empty and no more messages are expected
static size_t check_idata_size(idata_handle_t idata);
/**
 * This function generates a unique key from a interface name.
 */
// NOTE change this function if interfaces name, or set of interfaces, change. It relies
// on current interface names to create a simple but unique hash.
// If more interfaces are added this function should also change.
static uint64_t interfaces_perfect_hash(const char *key_string, size_t len);

/************************************************
 * Global functions definition
 ***********************************************/

void run_e2e_test()
{
    LOG_INF("Running e2e test"); // NOLINT
#if CONFIG_LOG_ONLY
    LOG_WRN("Running with device callbacks in log only mode"); // NOLINT
    LOG_WRN("Data received will NOT be checked against expected data"); // NOLINT
#endif
    // NOTE the order matters, we must initialize idata and device before the shell is started
    idata_handle_t idata = idata_init(interfaces, ARRAY_SIZE(interfaces), interfaces_perfect_hash);
    // sets up the global device_handle
    astarte_device_config_t config = {
        .device_id = CONFIG_DEVICE_ID,
        .cred_secr = CONFIG_CREDENTIAL_SECRET,
        .interfaces = interfaces,
        .interfaces_size = ARRAY_SIZE(interfaces),
        .http_timeout_ms = CONFIG_HTTP_TIMEOUT_MS,
        .mqtt_connection_timeout_ms = CONFIG_MQTT_CONNECTION_TIMEOUT_MS,
        .mqtt_poll_timeout_ms = CONFIG_MQTT_POLL_TIMEOUT_MS,
        .cbk_user_data = idata,
        .datastream_individual_cbk = device_individual_callback,
        .datastream_object_cbk = device_object_callback,
        .property_set_cbk = device_property_set_callback,
        .property_unset_cbk = device_property_unset_callback,
    };
    // NOTE the order matters, we must initialize idata and device before the shell is started
    device_setup(config);
    // NOTE then we pass it to the shell handlers called before `shell_start`
    init_shell(get_device(), idata);

    // wait while device connects
    wait_for_connection();

    // we are ready to send and receive data
    const struct shell *uart_shell = shell_backend_uart_get_ptr();
    shell_start(uart_shell);
    shell_print(uart_shell, "Device shell ready");
    unblock_shell_commands();

    // wait until a command disconnects the device
    wait_for_disconnection();
    CHECK_HALT(check_idata_size(idata) > 0, "Some expected messages didn't get received");

    // NOTE we also free the device and idata after `shell_stop`
    shell_print(uart_shell, "Disconnected, closing shell...");
    shell_stop(uart_shell);

    free_device();
    idata_free(idata);

#if CONFIG_LOG_ONLY
    LOG_WRN("Run e2e with device callbacks in log only mode"); // NOLINT
    LOG_WRN("Data received didn't get checked against expected data"); // NOLINT
#endif
}

/************************************************
 * Static functions definitions
 ***********************************************/

// device data callbacks that check received data against expected
static void device_individual_callback(astarte_device_datastream_individual_event_t event)
{
    LOG_INF("Individual datastream callback");
    idata_handle_t idata = event.base_event.user_data;
    const astarte_interface_t *interface = idata_get_interface(
        idata, event.base_event.interface_name, strlen(event.base_event.interface_name));
    CHECK_HALT(
        interface == NULL, "The interface name received as event does not match any interface");

#if !CONFIG_LOG_ONLY
    idata_individual_t expected = { 0 };
    CHECK_HALT(idata_pop_individual(idata, interface, &expected) != 0, "No more expected data");

    CHECK_HALT(strcmp(expected.path, event.base_event.path) != 0,
        "Received path does not match expected one");
    CHECK_HALT(!astarte_data_equal(&expected.data, &event.data), "Unexpected element received");

    free_individual(expected);
    LOG_INF("Individual received matched expected one");
#else
    LOG_INF("Individual received on %s%s", interface->name, event.base_event.path);
    utils_log_astarte_data(event.data);
#endif
}

static void device_object_callback(astarte_device_datastream_object_event_t event)
{
    LOG_INF("Object datastream callback");

    idata_handle_t idata = event.base_event.user_data;
    idata_object_entry_array received = {
        .buf = event.entries,
        .len = event.entries_len,
    };
    const astarte_interface_t *interface = idata_get_interface(
        idata, event.base_event.interface_name, strlen(event.base_event.interface_name));
    CHECK_HALT(
        interface == NULL, "The interface name received as event does not match any interface");

#if !CONFIG_LOG_ONLY
    idata_object_t expected = {};
    CHECK_HALT(idata_pop_object(idata, interface, &expected) != 0, "No more expected data");

    CHECK_HALT(strcmp(expected.path, event.base_event.path) != 0,
        "Received path does not match expected one");
    CHECK_HALT(!astarte_object_equal(&expected.entries, &received), "Unexpected element received");

    free_object(expected);
    LOG_INF("Object received matched expected one");
#else
    LOG_INF("Aggregate data received on %s%s", interface->name, event.base_event.path);
    utils_log_e2e_object_entry_array(&received);
#endif
}

static void device_property_set_callback(astarte_device_property_set_event_t event)
{
    LOG_INF("Property set callback");

    idata_handle_t idata = event.base_event.user_data;
    const astarte_interface_t *interface = idata_get_interface(
        idata, event.base_event.interface_name, strlen(event.base_event.interface_name));
    CHECK_HALT(
        interface == NULL, "The interface name received as event does not match any interface");

#if !CONFIG_LOG_ONLY
    idata_property_t expected = { 0 };
    // inizialization
    CHECK_HALT(idata_pop_property(idata, interface, &expected), "No more expected data");

    CHECK_HALT(strcmp(expected.path, event.base_event.path) != 0,
        "Received path does not match expected one");
    CHECK_HALT(!astarte_data_equal(&expected.data, &event.data), "Unexpected element received");

    free_property(expected);
    LOG_INF("Property received matched expected one");
#else
    LOG_INF("Individual property set received on %s%s", interface->name, event.base_event.path);
    utils_log_astarte_data(event.data);
#endif
}

static void device_property_unset_callback(astarte_device_data_event_t event)
{
    LOG_INF("Property unset callback");

    idata_handle_t idata = event.user_data;
    const astarte_interface_t *interface = idata_get_interface(
        idata, event.interface_name, strlen(event.interface_name));
    CHECK_HALT(
        interface == NULL, "The interface name received as event does not match any interface");

#if !CONFIG_LOG_ONLY
    idata_property_t expected = { 0 };
    // inizialization
    CHECK_HALT(idata_pop_property(idata, interface, &expected), "No more expected data");

    CHECK_HALT(strcmp(expected.path, event.path) != 0, "Received path does not match expected one");
    CHECK_HALT(!expected.unset, "Unexpected unset received");

    free_property(expected);
    LOG_INF("Expected property unset received");
#else
    LOG_INF("Individual property unset received on %s%s", interface->name, event.path);
#endif
}

static uint64_t interfaces_perfect_hash(const char *key_string, size_t len)
{
    const char interface_dname[] = "org.astarte-platform.zephyr.e2etest.";
    const size_t interface_dname_ownership_idetifier = 36;
    const size_t interface_dname_type_idetifier = 43;

    // check that the string is a known interface name and has enough characters for our check
    // NOTE this depends completely on the names used in this case this works because we have names
    // - ServerProperty
    // - DeviceProperty
    // - ServerAggregate
    // - DeviceAggregate
    // - ServerDatastream
    // - DeviceDatastream
    // hence the names are uniquely identified by the first letter and the seventh after the
    // reverse domain notation base of the interfaces (`interfaace_dname`)
    CHECK_HALT(strstr(key_string, interface_dname) != key_string || len <= 44,
        "Received an invalid or unexpected interface name, please update the hash function");

    uint32_t result = { 0 };
    uint8_t *result_bytes = (uint8_t *) &result;
    result_bytes[0] = key_string[interface_dname_ownership_idetifier];
    result_bytes[1] = key_string[interface_dname_ownership_idetifier];
    result_bytes[2] = key_string[interface_dname_type_idetifier];
    result_bytes[3] = key_string[interface_dname_type_idetifier];

    return result;
}

static size_t check_idata_size(idata_handle_t idata)
{
    size_t not_received_count = 0;

    for (size_t i = 0; i < ARRAY_SIZE(interfaces); ++i) {
        const astarte_interface_t *interface = interfaces[i];
        not_received_count += idata_get_count(idata, interface);

        if (interface->type == ASTARTE_INTERFACE_TYPE_PROPERTIES) {
            idata_property_t *property = {};
            if (idata_peek_property(idata, interface, &property) == 0) {
                utils_log_e2e_property(property);
            }
        } else if (interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_OBJECT) {
            idata_object_t *object = {};
            if (idata_peek_object(idata, interface, &object) == 0) {
                utils_log_e2e_object(object);
            }
        } else if (interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL) {
            idata_individual_t *individual = {};
            if (idata_peek_individual(idata, interface, &individual) == 0) {
                utils_log_e2e_individual(individual);
            }
        }
    }

    return not_received_count;
}
