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
    idata_timestamp_option_t timestamp;
} idata_individual_t;

// Property data used to store expected values from astarte
// if unset is true the data element will contain an invalid value
typedef struct
{
    const char *path;
    astarte_data_t data;
    bool unset;
} idata_property_t;

// Object data used to store expected values from astarte
typedef struct
{
    const char *path;
    idata_object_entry_array entries;
    idata_byte_array object_bytes;
    idata_timestamp_option_t timestamp;
} idata_object_t;

typedef union
{
    idata_property_t property;
    idata_individual_t individual;
    idata_object_t object;
} idata_message_t;

// TODO replace struct definition with with SPSC_DECLARE once they remove the static
struct spsc_astarte_messages
{
    struct spsc _spsc;
    idata_message_t *const buffer;
};

typedef struct
{
    const astarte_interface_t *interface;
    // order of reception is enforced it is advisable to test one message at a time
    // since only two messages will be stored in the buffer
    // for example you should expect only one element of an individual interface
    struct spsc_astarte_messages messages;
    idata_message_t messages_buf[2];
} idata_map_value_t;

/**
 * Function pointer used as parater in `idata_init`.
 * The function should be able to hash each input interface name to
 * a unique uint64_t. That is because the hashmap currently does not
 * support pointer to arbitrary types as keys. So we have to provide
 * a unique mapping between interface_name and uint64_t.
 */
typedef uint64_t (*interfaces_hash_t)(const char *key_string, size_t len);

typedef struct idata_t *idata_handle_t;

idata_handle_t idata_init(
    const astarte_interface_t *interfaces[], size_t interfaces_len, interfaces_hash_t hash_fn);

// get an interface object of the specified interface name
// the interfaces map got initialized in `idata_init`
const astarte_interface_t *idata_get_interface(
    idata_handle_t idata, const char *interface_name, size_t len);

// add an expected message to the specified interface
int idata_add_individual(idata_handle_t idata, const astarte_interface_t *interface,
    idata_individual_t expected_individual);
int idata_add_property(
    idata_handle_t idata, const astarte_interface_t *interface, idata_property_t expected_property);
int idata_add_object(
    idata_handle_t idata, const astarte_interface_t *interface, idata_object_t expected_object);

// get current count of elements
size_t idata_get_count(idata_handle_t idata, const astarte_interface_t *interface);

// pop next element expected for the specified interface (if set the return needs to be freed)
int idata_pop_individual(
    idata_handle_t idata, const astarte_interface_t *interface, idata_individual_t *out_individual);
int idata_pop_property(
    idata_handle_t idata, const astarte_interface_t *interface, idata_property_t *out_property);
int idata_pop_object(
    idata_handle_t idata, const astarte_interface_t *interface, idata_object_t *out_object);

int idata_peek_individual(idata_handle_t idata, const astarte_interface_t *interface,
    idata_individual_t **out_individual_ref);
int idata_peek_property(idata_handle_t idata, const astarte_interface_t *interface,
    idata_property_t **out_property_ref);
int idata_peek_object(
    idata_handle_t idata, const astarte_interface_t *interface, idata_object_t **out_object_ref);

// after popping items you need to free the element (you will get ownership of the popped element)
void free_individual(idata_individual_t individual);
void free_object(idata_object_t object);
void free_property(idata_property_t property);

void idata_free(idata_handle_t idata);

void utils_log_e2e_individual(idata_individual_t *individual);
void utils_log_e2e_object(idata_object_t *object);
void utils_log_e2e_property(idata_property_t *property);

#endif // IDATA_H
