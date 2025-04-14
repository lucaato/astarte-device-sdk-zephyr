/*
 * (C) Copyright 2025, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef E2EDEVICE_HANDLERS_H
#define E2EDEVICE_HANDLERS_H

/**
 * @file device_handler.h
 * @brief Handle device object and threads, these function should be called by one thread only
 */

#include <astarte_device_sdk/device.h>

// Used only as a token to avoid
typedef void *test_device_handle_t;

void device_setup(astarte_device_config_t config);
astarte_device_handle_t get_device();
// these functions read device_thread_flags and wait appropriately
// flag DEVICE_CONNECTED
void wait_for_connection();
void wait_for_disconnection();
// these functions write device_thread_flags
// flag THREAD_TERMINATION
void set_termination();
void free_device();
// --

#endif /* E2ESHELL_HANDLERS_H */
