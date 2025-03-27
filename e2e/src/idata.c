/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "idata.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <zephyr/logging/log.h>
#include <zephyr/sys/dlist.h>
#include <zephyr/sys/hash_map.h>
#include <zephyr/sys/hash_map_api.h>
#include <zephyr/sys/spsc_lockfree.h>
#include <zephyr/sys/util.h>
#include <zephyr/toolchain.h>

#include <astarte_device_sdk/interface.h>
#include <data_private.h>
#include <log.h>
#include <object_private.h>
#include <utils.h>

/************************************************
 * Constants, static variables and defines
 ***********************************************/

struct idata_t
{
    struct sys_hashmap iface_map;
    interfaces_hash_t hash_fn;
};

LOG_MODULE_REGISTER(idata, CONFIG_IDATA_LOG_LEVEL); // NOLINT

#define CHECK_INDIVIDUAL_DS(aggregation, type)                                                     \
    CHECK_RET_1((aggregation) != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,                         \
        "Incorrect aggregation type in interface passed expected individual datastream");          \
    CHECK_RET_1((type) != ASTARTE_INTERFACE_TYPE_DATASTREAM,                                       \
        "Incorrect interface type in interface passed expected individual datastream");

#define CHECK_AGGREGATE_DS(aggregation, type)                                                      \
    CHECK_RET_1((aggregation) != ASTARTE_INTERFACE_AGGREGATION_OBJECT,                             \
        "Incorrect aggregation type in interface passed expected aggregate datastream");           \
    CHECK_RET_1((type) != ASTARTE_INTERFACE_TYPE_DATASTREAM,                                       \
        "Incorrect interface type in interface passed expected aggregate datastream");

#define CHECK_INDIVIDUAL_PROP(aggregation, type)                                                   \
    CHECK_RET_1((aggregation) != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,                         \
        "Incorrect aggregation type in interface passed expected individual property");            \
    CHECK_RET_1((type) != ASTARTE_INTERFACE_TYPE_PROPERTIES,                                       \
        "Incorrect interface type in interface passed expected individual property");

/************************************************
 * Static functions declaration
 ***********************************************/

static uint64_t idata_hash_intf(interfaces_hash_t hash_fn, const astarte_interface_t *interface);
static uint64_t idata_hash_name(interfaces_hash_t hash_fn, const char *interface_name, size_t len);

static bool map_get_intf(
    idata_handle_t idata, const astarte_interface_t *interface, idata_map_value_t **value);
static bool map_get_name(
    idata_handle_t idata, const char *interface_name, size_t len, idata_map_value_t **out_value);

static void free_map_entry_callback(uint64_t key, uint64_t value, void *user_data);

/************************************************
 * Global functions definition
 ***********************************************/
idata_handle_t idata_init(
    const astarte_interface_t *interfaces[], size_t interfaces_len, const interfaces_hash_t hash_fn)
{
    struct sys_hashmap_config *hashmap_config = malloc(sizeof(struct sys_hashmap_config));
    CHECK_HALT(!hashmap_config, "Could not allocate map required memory");
    *hashmap_config
        = (struct sys_hashmap_config) SYS_HASHMAP_CONFIG(SIZE_MAX, SYS_HASHMAP_DEFAULT_LOAD_FACTOR);

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
        uint64_t key = idata_hash_intf(hash_fn, interfaces[i]);
        idata_map_value_t *allocated_value = malloc(sizeof(idata_map_value_t));
        idata_map_value_t initialized_value = (idata_map_value_t) {
            .interface = interfaces[i],
            // NOTE the parameters of SPSC_INITIALIZER needs to point to allocated_value
            // since that will be the final memory location
            .messages = SPSC_INITIALIZER(
                ARRAY_SIZE(allocated_value->messages_buf), allocated_value->messages_buf),
            .messages_buf = { {} },
        };
        memcpy(allocated_value, &initialized_value, sizeof(initialized_value));

        sys_hashmap_insert(&interface_map, key, POINTER_TO_UINT(allocated_value), NULL);
    }

    struct idata_t *const idata = malloc(sizeof(struct idata_t));
    *idata = (struct idata_t) {
        .iface_map = interface_map,
        .hash_fn = hash_fn,
    };

    return idata;
}

const astarte_interface_t *idata_get_interface(
    idata_handle_t idata, const char *interface_name, size_t len)
{
    idata_map_value_t *map_value = NULL;

    if (map_get_name(idata, interface_name, len, &map_value)) {
        return map_value->interface;
    }

    return NULL;
}

int idata_add_individual(idata_handle_t idata, const astarte_interface_t *interface,
    e2e_individual_data_t expected_individual)
{
    CHECK_INDIVIDUAL_DS(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");

    astarte_message_t *message = spsc_acquire(&value->messages);
    CHECK_RET_1(message == NULL, "Space for expected messages is exhausted");
    message->individual = expected_individual;
    spsc_produce(&value->messages);

    return 0;
}

int idata_add_property(idata_handle_t idata, const astarte_interface_t *interface,
    e2e_property_data_t expected_property)
{
    CHECK_INDIVIDUAL_PROP(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");

    astarte_message_t *message = spsc_acquire(&value->messages);
    CHECK_RET_1(message == NULL, "Space for expected messages is exhausted");
    message->property = expected_property;
    spsc_produce(&value->messages);

    return 0;
}

int idata_add_object(
    idata_handle_t idata, const astarte_interface_t *interface, e2e_object_data_t expected_object)
{
    CHECK_AGGREGATE_DS(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");

    astarte_message_t *message = spsc_acquire(&value->messages);
    CHECK_RET_1(message == NULL, "Space for expected messages is exhausted");
    message->object = expected_object;
    spsc_produce(&value->messages);

    return 0;
}

size_t idata_get_count(idata_handle_t idata, const astarte_interface_t *interface)
{
    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");

    return spsc_consumable(&value->messages);
}

int idata_pop_individual(idata_handle_t idata, const astarte_interface_t *interface,
    e2e_individual_data_t *out_individual)
{
    CHECK_RET_1(out_individual == NULL, "Passed out pointer is null");
    CHECK_INDIVIDUAL_DS(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_consume(&value->messages);
    CHECK_RET_1(message == NULL, "No more expected messages");
    *out_individual = message->individual;
    spsc_release(&value->messages);

    return 0;
}

int idata_pop_property(
    idata_handle_t idata, const astarte_interface_t *interface, e2e_property_data_t *out_property)
{
    CHECK_RET_1(out_property == NULL, "Passed out pointer is null");
    CHECK_INDIVIDUAL_PROP(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_consume(&value->messages);
    CHECK_RET_1(message == NULL, "No more expected messages");
    *out_property = message->property;
    spsc_release(&value->messages);

    return 0;
}

int idata_pop_object(
    idata_handle_t idata, const astarte_interface_t *interface, e2e_object_data_t *out_object)
{
    CHECK_RET_1(out_object == NULL, "Passed out pointer is null");
    CHECK_AGGREGATE_DS(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_consume(&value->messages);
    CHECK_RET_1(message == NULL, "No more expected messages");
    *out_object = message->object;
    spsc_release(&value->messages);

    return 0;
}

int idata_peek_individual(idata_handle_t idata, const astarte_interface_t *interface,
    e2e_individual_data_t **out_individual_ref)
{
    CHECK_INDIVIDUAL_DS(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_peek(&value->messages);
    CHECK_RET_1(!message);

    if (out_individual_ref) {
        *out_individual_ref = &message->individual;
    }

    return 0;
}
int idata_peek_property(idata_handle_t idata, const astarte_interface_t *interface,
    e2e_property_data_t **out_property_ref)
{
    CHECK_INDIVIDUAL_PROP(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_peek(&value->messages);
    CHECK_RET_1(!message);

    if (out_property_ref) {
        *out_property_ref = &message->property;
    }

    return 0;
}

int idata_peek_object(
    idata_handle_t idata, const astarte_interface_t *interface, e2e_object_data_t **out_object_ref)
{
    CHECK_AGGREGATE_DS(interface->aggregation, interface->type);

    idata_map_value_t *value = {};
    CHECK_RET_1(!map_get_intf(idata, interface, &value), "Unknown passed interface");
    astarte_message_t *message = spsc_peek(&value->messages);
    CHECK_RET_1(!message);

    if (out_object_ref) {
        *out_object_ref = &message->object;
    }

    return 0;
}

void free_individual(e2e_individual_data_t individual)
{
    free((char *) individual.path);
    astarte_data_destroy_deserialized(individual.data);
}

void free_object(e2e_object_data_t object)
{
    free((char *) object.path);
    free(object.object_bytes.buf);
    astarte_object_entries_destroy_deserialized(object.entries.buf, object.entries.len);
}

void free_property(e2e_property_data_t property)
{
    // unsets do not store an individual value
    if (!property.unset) {
        astarte_data_destroy_deserialized(property.data);
    }

    free((char *) property.path);
}

void idata_free(idata_handle_t idata)
{
    sys_hashmap_clear(&idata->iface_map, free_map_entry_callback, idata);

    free((void *) idata->iface_map.config);
    free(idata->iface_map.data);

    free(idata);
}

void utils_log_e2e_individual(e2e_individual_data_t *individual)
{
    LOG_INF("Individual path: %s", individual->path);
    utils_log_e2e_timestamp(&individual->timestamp);
    utils_log_astarte_data(individual->data);
}

void utils_log_e2e_object(e2e_object_data_t *object)
{
    LOG_INF("Object path: %s", object->path);
    utils_log_e2e_timestamp(&object->timestamp);
    utils_log_e2e_object_entry_array(&object->entries);
}

void utils_log_e2e_property(e2e_property_data_t *property)
{
    LOG_INF("Property path: %s", property->path);
    if (property->unset) {
        LOG_INF("Property Unset");
    } else {
        utils_log_astarte_data(property->data);
    }
}

/************************************************
 * Static functions definitions
 ***********************************************/

static void free_map_entry_callback(uint64_t key, uint64_t value, void *user_data)
{
    ARG_UNUSED(key);
    idata_handle_t idata = user_data;

    // NOLINTNEXTLINE
    idata_map_value_t *data_value = UINT_TO_POINTER(value);

    if (data_value->interface->type == ASTARTE_INTERFACE_TYPE_PROPERTIES) {
        e2e_property_data_t property = {};
        while (idata_pop_property(idata, data_value->interface, &property) == 0) {
            free_property(property);
        }
    } else if (data_value->interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_OBJECT) {
        e2e_object_data_t object = {};
        while (idata_pop_object(idata, data_value->interface, &object) == 0) {
            free_object(object);
        }
    } else if (data_value->interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL) {
        e2e_individual_data_t individual = {};
        while (idata_pop_individual(idata, data_value->interface, &individual) == 0) {
            free_individual(individual);
        }
    }
}

static uint64_t idata_hash_intf(interfaces_hash_t hash_fn, const astarte_interface_t *interface)
{
    return idata_hash_name(hash_fn, interface->name, strlen(interface->name));
}

static uint64_t idata_hash_name(interfaces_hash_t hash_fn, const char *interface_name, size_t len)
{
    return hash_fn(interface_name, len);
}

static bool map_get_intf(
    idata_handle_t idata, const astarte_interface_t *interface, idata_map_value_t **out_value)
{
    return map_get_name(idata, interface->name, strlen(interface->name), out_value);
}

static bool map_get_name(
    idata_handle_t idata, const char *interface_name, size_t len, idata_map_value_t **out_value)
{
    uint64_t key = idata_hash_name(idata->hash_fn, interface_name, len);
    uint64_t value = { 0 };

    if (!sys_hashmap_get(&idata->iface_map, key, &value)) {
        return false;
    }

    // NOLINTNEXTLINE
    *out_value = UINT_TO_POINTER(value);

    return true;
}
