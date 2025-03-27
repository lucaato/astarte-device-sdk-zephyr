/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef E2ERUNNER_H
#define E2ERUNNER_H

// should be called at the start of the application to avoid user input before
// the shell is actually ready and the device connected
void block_shell_commands();
// run the e2e test on all test devices
// must be called only once, it takes ownership of a semaphore so it will block until the first call
// exits
void run_e2e_test();

#endif /* E2ERUNNER_H */
