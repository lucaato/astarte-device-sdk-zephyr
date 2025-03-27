/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef IDATA_H
#define IDATA_H

/**
 * @file idata.h
 * @brief Interface data (idata) stored in the e2e test to perform checks
 */

#include <zephyr/sys/dlist.h>
#include <zephyr/sys/hash_map.h>
#include <zephyr/sys/spsc_lockfree.h>

#include <astarte_device_sdk/data.h>
#include <astarte_device_sdk/interface.h>
#include <astarte_device_sdk/object.h>

#include "utilities.h"

// Individual data used to store expected values from astarte
typedef struct
{
    const char *path;
    astarte_data_t data;
    e2e_timestamp_option_t timestamp;
} e2e_individual_data_t;

// Property data used to store expected values from astarte
// if unset is true the data element will contain an invalid value
typedef struct
{
    const char *path;
    astarte_data_t data;
    bool unset;
} e2e_property_data_t;

// Object data used to store expected values from astarte
typedef struct
{
    const char *path;
    e2e_object_entry_array_t entries;
    e2e_byte_array object_bytes;
    e2e_timestamp_option_t timestamp;
} e2e_object_data_t;

typedef union {
    e2e_property_data_t property;
    e2e_individual_data_t individual;
    e2e_object_data_t object;
} astarte_message_t;

SPSC_DECLARE(astarte_messages, astarte_message_t); // NOLINT

typedef struct
{
    const astarte_interface_t *interface;
    // order of reception is enforced it is advisable to test one message at a time
    // since only two messages will be stored in the buffer
    // for example you should expect only one element of an individual interface
    struct spsc_astarte_messages expected;
    astarte_message_t *expected_buf[2];
} idata_map_value_t;

typedef uint64_t (*interfaces_hash_t)(char* key_string, size_t len);

typedef struct
{
    struct sys_hashmap iface_map;
    interfaces_hash_t hash_fn;
} e2e_idata_t;

void idata_init(const astarte_interface_t *interfaces[], size_t interfaces_len, interfaces_hash_t hash_fn);
const astarte_interface_t *idata_get_interface(e2e_idata_t *idata, char *interface_name);
int idata_expect_individual(const astarte_interface_t *interface, e2e_individual_data_t expected_individual);
//int idata_add_property(e2e_idata_t *idata, const astarte_interface_t *interface,
//    e2e_property_data_t expected_property);
//int idata_add_object(
//    e2e_idata_t *idata, const astarte_interface_t *interface, e2e_object_data_t expected_object);
//// get functions, these fetch the next idata unit matching the passed interface name using the
//// passed out_individual as starting point if out individual is null they return the first
//// occurrence
//int idata_get_individual(
//    e2e_idata_t *idata, const char *interface, e2e_individual_data_t **out_individual);
//int idata_get_property(
//    e2e_idata_t *idata, const char *interface, e2e_property_data_t **out_property);
//int idata_get_object(e2e_idata_t *idata, const char *interface, e2e_object_data_t **out_object);
//bool idata_is_empty(e2e_idata_t *idata);
//e2e_idata_unit_t *idata_iter(e2e_idata_t *idata);
//e2e_idata_unit_t *idata_iter_next(e2e_idata_t *idata, e2e_idata_unit_t *current);
//int idata_remove(e2e_idata_unit_t *idata_unit);
//int idata_remove_individual(e2e_individual_data_t *individual);
//int idata_remove_property(e2e_property_data_t *property);
//int idata_remove_object(e2e_object_data_t *object);
void idata_free();

#endif // IDATA_H
