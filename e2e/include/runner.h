/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef E2ERUNNER_H
#define E2ERUNNER_H

// run the e2e test on all test devices
// must be called only once, it takes ownership of a semaphore so it will block until the first call
// exits
void run_e2e_test();

#endif /* E2ERUNNER_H */
