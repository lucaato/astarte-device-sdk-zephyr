/*
 * (C) Copyright 2025, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef E2ESHELL_HANDLERS_H
#define E2ESHELL_HANDLERS_H

/**
 * @file shell_handlers.h
 * @brief Contains the functions called by the zephyr shell
 *
 * @details This file contains the callback used by the shell zephyr library.
 * For more info take a look at https://docs.zephyrproject.org/latest/services/shell/index.html.
 */

#include <zephyr/shell/shell.h>

#include <astarte_device_sdk/device.h>

#include "idata.h"

// NOTE should be called before everything else
void init_shell(astarte_device_handle_t device, idata_handle_t idata);

int cmd_expect_object_handler(const struct shell *shell, size_t argc, char **argv);
int cmd_expect_individual_handler(const struct shell *shell, size_t argc, char **argv);
int cmd_expect_property_set_handler(const struct shell *shell, size_t argc, char **argv);
int cmd_expect_property_unset_handler(const struct shell *shell, size_t argc, char **argv);

int cmd_send_individual_handler(const struct shell *shell, size_t argc, char **argv);
int cmd_send_object_handler(const struct shell *shell, size_t argc, char **argv);
int cmd_send_property_set_handler(const struct shell *shell, size_t argc, char **argv);
int cmd_send_property_unset_handler(const struct shell *shell, size_t argc, char **argv);

int cmd_disconnect(const struct shell *shell, size_t argc, char **argv);

#endif /* E2ESHELL_HANDLERS_H */
