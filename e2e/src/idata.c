/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "idata.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <astarte_device_sdk/interface.h>
#include <data_private.h>
#include <log.h>
#include <object_private.h>

#include "utilities.h"
#include "zephyr/kernel.h"
#include "zephyr/logging/log.h"
#include "zephyr/sys/dlist.h"
#include "zephyr/sys/hash_map.h"
#include "zephyr/sys/hash_map_api.h"
#include "zephyr/sys/spsc_lockfree.h"
#include "zephyr/sys/util.h"
#include "zephyr/toolchain.h"

/************************************************
 * Constants, static variables and defines
 ***********************************************/

#define MAIN_THREAD_SLEEP_MS 500

LOG_MODULE_REGISTER(idata, CONFIG_IDATA_LOG_LEVEL); // NOLINT

// NOTE Global idata object so that both shell handlers and device callbacks can access it
static e2e_idata_t idata;
K_SEM_DEFINE(idata_sem, 1, 1);

/************************************************
 * Static functions declaration
 ***********************************************/

static uint64_t idata_hash_intf(const astarte_interface_t *interface);
static uint64_t idata_hash_name(const char *interface_name, size_t len);

static bool map_get(const astarte_interface_t *interface, idata_map_value_t **value);
static bool map_get_by_name(const char *interface_name, idata_map_value_t **value);

static void free_map_entry_callback(uint64_t key, uint64_t value, void *user_data);

/************************************************
 * Global functions definition
 ***********************************************/
void idata_init(const astarte_interface_t *interfaces[], size_t interfaces_len, const interfaces_hash_t hash_fn)
{
    CHECK_HALT(idata.hash_fn, "Idata was already initialized"); 

    struct sys_hashmap_config *hashmap_config = malloc(sizeof(struct sys_hashmap_config));
    CHECK_HALT(!hashmap_config, "Could not allocate map required memory");
    *hashmap_config = SYS_HASHMAP_CONFIG(SIZE_MAX, SYS_HASHMAP_DEFAULT_LOAD_FACTOR);

    struct sys_hashmap_data *hashmap_data = calloc(1, sizeof(struct sys_hashmap_data));
    CHECK_HALT(!hashmap_data, "Could not allocate map required memory");

    struct sys_hashmap interface_map = (struct sys_hashmap) {
        .api = &sys_hashmap_sc_api,
        .config = (const struct sys_hashmap_config *) hashmap_config,
        .data = hashmap_data,
        .hash_func = sys_hash32,
        .alloc_func = SYS_HASHMAP_DEFAULT_ALLOCATOR,
    };

    for (size_t i = 0; i < interfaces_len; ++i) {
        uint64_t key = idata_hash_intf(interfaces[i]);
        idata_map_value_t *allocated_value = malloc(sizeof(idata_map_value_t));
        idata_map_value_t initialized_value = (idata_map_value_t) {
            .interface = interfaces[i],
            // the parameters of SPSC_INITIALIZER needs to point to allocated_value
            // since that will be the final memory location
            .expected = SPSC_INITIALIZER(ARRAY_SIZE(allocated_value->expected_buf), (astarte_message_t * const) allocated_value->expected_buf),
            .expected_buf = { 0 },
        };
        memcpy(allocated_value, &initialized_value, sizeof(initialized_value));

        sys_hashmap_insert(&interface_map, key, POINTER_TO_UINT(allocated_value), NULL);
    }

    // FIXME i should add a check to every function to verify that the idata is initialized
    k_sem_take(&idata_sem, K_FOREVER);
    idata = (e2e_idata_t) {
        .iface_map = interface_map,
        .hash_fn = hash_fn,
    };
}

const astarte_interface_t *idata_get_interface(const char *interface_name) {
    uint64_t key = idata_hash_name(interface_name, strlen(interface_name));

    uint64_t value = { 0 };
    sys_hashmap_get(&idata.iface_map, key, &value);

    if (value) {
        // NOLINTNEXTLINE
        idata_map_value_t *map_value = UINT_TO_POINTER(value);
        return map_value->interface;
    }

     return NULL;
}

int idata_expect_individual(const astarte_interface_t *interface,
    e2e_individual_data_t expected_individual)
{
    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect aggregation type in interface passed to expected_add_individual");
    CHECK_RET_1(interface->type != ASTARTE_INTERFACE_TYPE_DATASTREAM,
        "Incorrect interface type in interface passed to expected_add_individual");

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get(interface, &value), "Unknown passed interface");

    astarte_message_t *message = spsc_acquire(&value->expected);
    CHECK_RET_1(message == NULL, "Space for expected messages is exhausted");
    message->individual = expected_individual;
    spsc_produce(&value->expected);

    return 0;
}

int idata_expect_property(
    const astarte_interface_t *interface, e2e_property_data_t expected_property)
{
    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect aggregation type in interface passed to expected_add_property");
    CHECK_RET_1(interface->type != ASTARTE_INTERFACE_TYPE_PROPERTIES,
        "Incorrect interface type in interface passed to expected_add_property");

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get(interface, &value), "Unknown passed interface");

    astarte_message_t *message = spsc_acquire(&value->expected);
    CHECK_RET_1(message == NULL, "Space for expected messages is exhausted");
    message->property = expected_property;
    spsc_produce(&value->expected);

    return 0;
}

int idata_expect_object(
    const astarte_interface_t *interface, e2e_object_data_t expected_object)
{
    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_OBJECT,
        "Incorrect aggregation type in interface passed");
    CHECK_RET_1(interface->type != ASTARTE_INTERFACE_TYPE_DATASTREAM,
        "Incorrect interface type in interface passed");

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get(interface, &value), "Unknown passed interface");

    astarte_message_t *message = spsc_acquire(&value->expected);
    CHECK_RET_1(message == NULL, "Space for expected messages is exhausted");
    message->object = expected_object;
    spsc_produce(&value->expected);

    return 0;
}

int idata_pop_individual(
    const astarte_interface_t *interface, e2e_individual_data_t *out_individual)
{
    CHECK_RET_1(out_individual == NULL, "Passed out pointer is null");

    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect interface aggregation associated with interface name passed");
    CHECK_RET_1(interface->type == ASTARTE_INTERFACE_TYPE_PROPERTIES,
        "Incorrect interface type associated with interface name passed");

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get(interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_consume(&value->expected);
    CHECK_RET_1(message == NULL, "No more expected messages");

    *out_individual = message->individual;

    return 0;
}

int idata_pop_property(
    const astarte_interface_t *interface, e2e_property_data_t *out_property)
{
    CHECK_RET_1(out_property == NULL, "Passed out pointer is null");

    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect interface aggregation associated with interface name passed");
    CHECK_RET_1(interface->type != ASTARTE_INTERFACE_TYPE_PROPERTIES,
        "Incorrect interface type associated with interface name passed");

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get(interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_consume(&value->expected);
    CHECK_RET_1(message == NULL, "No more expected messages");

    *out_property = message->property;
    return 0;
}

int idata_pop_object(const astarte_interface_t *interface, e2e_object_data_t *out_object)
{
    CHECK_RET_1(out_object == NULL, "Passed out pointer is null");

    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_OBJECT,
        "Incorrect interface aggregation associated with interface name passed");
    CHECK_RET_1(interface->type != ASTARTE_INTERFACE_TYPE_DATASTREAM,
        "Incorrect interface type in interface passed");

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get(interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_consume(&value->expected);
    CHECK_RET_1(message == NULL, "No more expected messages");

    *out_object = message->object;
    return 0;
}

void free_individual(e2e_individual_data_t individual) {
    free((char *) individual.path);
    astarte_data_destroy_deserialized(individual.data);
}

void free_object(e2e_object_data_t object) {
    free((char *) object.path);
    free(object.object_bytes.buf);
    astarte_object_entries_destroy_deserialized(
        object.entries.buf, object.entries.len);
}

void free_property(e2e_property_data_t property) {
    // unsets do not store an individual value
    if (!property.unset) {
        astarte_data_destroy_deserialized(property.data);
    }

    free((char *) property.path);
}

void idata_free()
{
    sys_hashmap_clear(&idata.iface_map, free_map_entry_callback, NULL);

    free((void *) idata.iface_map.config);
    free(idata.iface_map.data);
}

/************************************************
 * Static functions definitions
 ***********************************************/

static void free_map_entry_callback(uint64_t key, uint64_t value, void *user_data) {
    ARG_UNUSED(key);
    ARG_UNUSED(user_data);

    // NOLINTNEXTLINE
    idata_map_value_t *data_value = UINT_TO_POINTER(value);
        
    if (data_value->interface->type == ASTARTE_INTERFACE_TYPE_PROPERTIES) {
        e2e_property_data_t property = {};
        while(idata_pop_property(data_value->interface, &property) == 0) {
            free_property(property);
        }
    }
    else if (data_value->interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_OBJECT) {
        e2e_object_data_t object = {};
        while(idata_pop_object(data_value->interface, &object) == 0) {
            free_object(object);
        }
    }
    else if (data_value->interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL) {
        e2e_individual_data_t individual = {};
        while(idata_pop_individual(data_value->interface, &individual) == 0) {
            free_individual(individual);
        }
    }
}

static uint64_t idata_hash_intf(const astarte_interface_t *interface) {
    return idata_hash_name(interface->name, strlen(interface->name));
}

static uint64_t idata_hash_name(const char *interface_name, size_t len) {
    return idata.hash_fn(interface_name, len);
}

static bool map_get(const astarte_interface_t *interface, idata_map_value_t **out_value) {
    uint64_t key = idata_hash_intf(interface);
    uint64_t value = { 0 };
    
    if (!sys_hashmap_get(&idata.iface_map, key, &value)) {
        return false;
    }

    // NOLINTNEXTLINE
    *out_value = UINT_TO_POINTER(value);

    return true;
}
