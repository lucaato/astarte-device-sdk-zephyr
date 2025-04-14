/*
 * (C) Copyright 2025, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "device_handler.h"

#include <zephyr/logging/log.h>
#include <zephyr/sys/atomic.h>

#include "astarte_device_sdk/device.h"
#include "utilities.h"
#include "utils.h"
#include "zephyr/kernel.h"

#define MAIN_THREAD_SLEEP_MS 500

enum e2e_thread_flags
{
    DEVICE_CONNECTED = 0,
    THREAD_TERMINATION,
};

K_THREAD_STACK_DEFINE(device_thread_stack_area, CONFIG_DEVICE_THREAD_STACK_SIZE);
static struct k_thread device_thread_data;

static astarte_device_handle_t device_handle;
K_SEM_DEFINE(device_sem, 1, 1);

static atomic_t device_thread_flags;

LOG_MODULE_REGISTER(device_handler, CONFIG_DEVICE_HANDLER_LOG_LEVEL); // NOLINT

static bool get_termination();
static void connection_callback(astarte_device_connection_event_t event);
static void disconnection_callback(astarte_device_disconnection_event_t event);
// write flag DEVICE_CONNECTED
static void set_connected();
static void set_disconnected();
// --
static void device_thread_entry_point(void *unused1, void *unused2, void *unused3);

void device_setup(astarte_device_config_t config)
{
    // override with local callbacks
    config.connection_cbk = connection_callback;
    config.disconnection_cbk = disconnection_callback;

    astarte_device_handle_t temp_handle = NULL;
    CHECK_HALT(k_sem_count_get(&device_sem) == 0, "The device is already initialized");

    LOG_INF("Creating static astarte_device by calling astarte_device_new."); // NOLINT
    CHECK_ASTARTE_OK_HALT(
        astarte_device_new(&config, &temp_handle), "Astarte device creation failure.");

    // we take the semaphore after initializing the device to make sure that errors from
    // astarte_device_new can be handled separately
    CHECK_HALT(k_sem_take(&device_sem, K_NO_WAIT) != 0,
        "Could not take the semaphore, the device is already initialized");
    device_handle = temp_handle;

    LOG_INF("Spawning a new thread to poll data from the Astarte device."); // NOLINT
    k_thread_create(&device_thread_data, device_thread_stack_area,
        K_THREAD_STACK_SIZEOF(device_thread_stack_area), device_thread_entry_point, NULL, NULL,
        NULL, CONFIG_DEVICE_THREAD_PRIORITY, 0, K_NO_WAIT);
}

astarte_device_handle_t get_device()
{
    CHECK_HALT(k_sem_count_get(&device_sem) > 0 || get_termination(),
        "The device is not initialized or is terminating");

    return device_handle;
}

void set_termination()
{
    atomic_set_bit(&device_thread_flags, THREAD_TERMINATION);
}

void wait_for_connection()
{
    while (!atomic_test_bit(&device_thread_flags, DEVICE_CONNECTED)) {
        k_sleep(K_MSEC(MAIN_THREAD_SLEEP_MS));
    }
}

void wait_for_disconnection()
{
    while (atomic_test_bit(&device_thread_flags, DEVICE_CONNECTED)) {
        k_sleep(K_MSEC(MAIN_THREAD_SLEEP_MS));
    }
}

void free_device()
{
    set_termination();

    CHECK_HALT(k_thread_join(&device_thread_data, K_FOREVER) != 0,
        "Failed in waiting for the Astarte thread to terminate.");

    LOG_INF("Destroing Astarte device and freeing resources."); // NOLINT
    CHECK_ASTARTE_OK_HALT(
        astarte_device_destroy(device_handle), "Astarte device destruction failure.");

    LOG_INF("Astarte device destroyed."); // NOLINT

    LOG_INF("Giving back the semaphore lock"); // NOLINT
    // allow creating another device with `device_setup`
    device_handle = NULL;
    k_sem_give(&device_sem);
}

static bool get_termination()
{
    return atomic_test_bit(&device_thread_flags, THREAD_TERMINATION);
}

static void set_connected()
{
    atomic_set_bit(&device_thread_flags, DEVICE_CONNECTED);
}

static void set_disconnected()
{
    atomic_clear_bit(&device_thread_flags, DEVICE_CONNECTED);
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

static void device_thread_entry_point(void *unused1, void *unused2, void *unused3)
{
    ARG_UNUSED(unused1);
    ARG_UNUSED(unused2);
    ARG_UNUSED(unused3);

    LOG_INF("Starting e2e device thread."); // NOLINT

    CHECK_ASTARTE_OK_HALT(
        astarte_device_connect(device_handle), "Astarte device connection failure.");

    while (!get_termination()) {
        k_timepoint_t timepoint = sys_timepoint_calc(K_MSEC(CONFIG_DEVICE_POLL_PERIOD_MS));

        astarte_result_t res = astarte_device_poll(device_handle);
        CHECK_HALT(res != ASTARTE_RESULT_TIMEOUT && res != ASTARTE_RESULT_OK,
            "Astarte device poll failure.");

        k_sleep(sys_timepoint_timeout(timepoint));
    }

    CHECK_ASTARTE_OK_HALT(astarte_device_disconnect(device_handle, K_SECONDS(10)),
        "Astarte device disconnection failure.");

    LOG_INF("Exiting from the polling thread."); // NOLINT
}
