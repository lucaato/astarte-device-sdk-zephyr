#include "device_handler.h"

#include <zephyr/sys/atomic.h>

#include "utils.h"
#include "astarte_device_sdk/device.h"
#include "utilities.h"
#include "zephyr/kernel.h"

K_THREAD_STACK_DEFINE(device_thread_stack_area, CONFIG_DEVICE_THREAD_STACK_SIZE);
static struct k_thread device_thread_data;

static astarte_device_handle_t device_handle;
K_MUTEX_DEFINE(device_mutex);

enum e2e_thread_flags
{
    DEVICE_INITIALIZED = 0,
    DEVICE_CONNECTED,
    THREAD_TERMINATION,
};
static atomic_t device_thread_flags;

LOG_MODULE_REGISTER(runner, CONFIG_DEVICE_HANDLER_LOG_LEVEL); // NOLINT

static bool get_termination();

static void device_thread_entry_point(void *device_handle, void *unused1, void *unused2)
{
    ARG_UNUSED(unused1);
    ARG_UNUSED(unused2);

    astarte_device_handle_t device = (astarte_device_handle_t) device_handle;

    LOG_INF("Starting e2e device thread."); // NOLINT

    CHECK_ASTARTE_OK_HALT(astarte_device_connect(device), "Astarte device connection failure.");

    while (!get_termination()) {
        k_timepoint_t timepoint = sys_timepoint_calc(K_MSEC(CONFIG_DEVICE_POLL_PERIOD_MS));

        astarte_result_t res = astarte_device_poll(device);
        CHECK_HALT(res != ASTARTE_RESULT_TIMEOUT && res != ASTARTE_RESULT_OK,
            "Astarte device poll failure.");

        k_sleep(sys_timepoint_timeout(timepoint));
    }

    CHECK_ASTARTE_OK_HALT(
        astarte_device_disconnect(device, K_SECONDS(10)), "Astarte device disconnection failure.");

    LOG_INF("Destroing Astarte device and freeing resources."); // NOLINT
    CHECK_ASTARTE_OK_HALT(
        astarte_device_destroy(device_handle), "Astarte device destruction failure.");

    // allow creating another device with `device_setup`
    CHECK_HALT(
        k_mutex_lock(&device_mutex, K_FOREVER) != 0, "Could not lock device mutex for initialization");
    atomic_clear_bit(&device_thread_flags, DEVICE_INITIALIZED);
    device_handle = NULL;
    k_mutex_unlock(&device_mutex);

    LOG_INF("Exiting from the polling thread."); // NOLINT
}

void device_setup(astarte_device_config_t config)
{
    astarte_device_handle_t temp_handle = NULL;

    // lock the device until it exits the thread and disconnects
    CHECK_HALT(
        k_mutex_lock(&device_mutex, K_FOREVER) != 0, "Could not lock device mutex for initialization");
    if (!atomic_test_and_set_bit(&device_thread_flags, DEVICE_INITIALIZED)) {
        LOG_INF("Creating static astarte_device by calling astarte_device_new."); // NOLINT
        CHECK_ASTARTE_OK_HALT(
            astarte_device_new(&config, &temp_handle), "Astarte device creation failure.");
    }
    else {
        // if the device was already initialized
        // in this else path we do not need to unlock since we are halting execution
        LOG_ERR("The device is already initialized"); // NOLINT
        k_fatal_halt(-1);
    }
    device_handle = temp_handle;
    k_mutex_unlock(&device_mutex);

    LOG_INF("Spawning a new thread to poll data from the Astarte device."); // NOLINT
    k_thread_create(&device_thread_data, device_thread_stack_area,
        K_THREAD_STACK_SIZEOF(device_thread_stack_area), device_thread_entry_point, device_handle,
        NULL, NULL, CONFIG_DEVICE_THREAD_PRIORITY, 0, K_NO_WAIT);
}

void set_connected() {
    atomic_set_bit(&device_thread_flags, DEVICE_CONNECTED);
}

void set_disconnected() {
    atomic_clear_bit(&device_thread_flags, DEVICE_CONNECTED);
}

void set_termination() {
    atomic_set_bit(&device_thread_flags, THREAD_TERMINATION);
}

void wait_for_destroyed_device() {
    while(!get_termination()) {
        k_sleep(K_MSEC(MAIN_THREAD_SLEEP_MS));
    }

    CHECK_HALT(k_thread_join(&device_thread_data, K_FOREVER) != 0,
        "Failed in waiting for the Astarte thread to terminate.");
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

static bool get_termination() {
    return atomic_test_bit(&device_thread_flags, THREAD_TERMINATION);
}
