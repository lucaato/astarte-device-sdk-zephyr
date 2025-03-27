/*
 * (C) Copyright 2025, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef E2EDEVICE_HANDLERS_H
#define E2EDEVICE_HANDLERS_H

/**
 * @file device_handler.h
 * @brief Handle device object and threads
 */

#include <astarte_device_sdk/device.h>

#define MAIN_THREAD_SLEEP_MS 500

void device_setup(astarte_device_config_t config);

// these functions read device_thread_flags and wait appropriately
// flag DEVICE_CONNECTED_FLAG
void wait_for_connection();
void wait_for_disconnection();
// flag THREAD_TERMINATION_FLAG
bool get_termination();
void wait_for_destroyed_device();
// --
// these functions write device_thread_flags
// flag DEVICE_CONNECTED_FLAG
void set_connected();
void set_disconnected();
// flag THREAD_TERMINATION_FLAG
void set_termination();
// --

#endif /* E2ESHELL_HANDLERS_H */
