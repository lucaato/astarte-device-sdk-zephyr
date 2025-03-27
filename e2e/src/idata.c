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
#include "zephyr/sys/util.h"

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

static idata_map_value_t map_get();

// called during initialization to initlialize the hashmap entry
static void idata_init_interface(struct sys_hashmap *interface_map, const astarte_interface_t *interface);

static void free_hashmap_value(uint64_t key, uint64_t value, void *cookie);

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
        idata_init_interface(&interface_map, interfaces[i]);
    }

    // FIXME i should add a check to every function to verify that the idata is initialized
    k_sem_take(&idata_sem, K_FOREVER);
    idata = (e2e_idata_t) {
        .iface_map = interface_map,
        .hash_fn = hash_fn,
    };
}

const astarte_interface_t *idata_get_interface(e2e_idata_t *idata, char *interface_name) {
    uint64_t key = idata_hash_name(interface_name, strlen(interface_name));

    uint64_t value = { 0 };
    sys_hashmap_get(&idata->iface_map, key, &value);

    if (value) {
        idata_map_value_t *map_value = UINT_TO_POINTER(value);
        return map_value->interface;
    }
    else {
        return NULL;
    }
}

int idata_expect_individual(const astarte_interface_t *interface,
    e2e_individual_data_t expected_individual)
{
    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect aggregation type in interface passed to expected_add_individual");
    CHECK_RET_1(interface->type == ASTARTE_INTERFACE_TYPE_PROPERTIES,
        "Incorrect interface type in interface passed to expected_add_individual");

    uint64_t key = idata_hash_name(interface_name, strlen(interface_name));
    uint64_t value = { 0 };
    sys_hashmap_get(&idata->iface_map, key, &value);

    return 0;
}

int idata_add_property(
    e2e_idata_t *idata, const astarte_interface_t *interface, e2e_property_data_t expected_property)
{
    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect aggregation type in interface passed to expected_add_property");
    CHECK_RET_1(interface->type != ASTARTE_INTERFACE_TYPE_PROPERTIES,
        "Incorrect interface type in interface passed to expected_add_property");

    e2e_idata_unit_t *element = calloc(1, sizeof(e2e_idata_unit_t));
    CHECK_HALT(!element, "Could not allocate an e2e_expected_unit_t");

    *element = (e2e_idata_unit_t) {
        .interface = interface,
        .values.property = expected_property,
    };

    sys_dnode_init(&element->node);
    sys_dlist_append(idata->list, &element->node);

    return 0;
}

int idata_add_object(
    e2e_idata_t *idata, const astarte_interface_t *interface, e2e_object_data_t expected_object)
{
    CHECK_RET_1(interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_OBJECT,
        "Incorrect aggregation type in interface passed to e2e_interface_t object constructor");

    e2e_idata_unit_t *element = calloc(1, sizeof(e2e_idata_unit_t));
    CHECK_HALT(!element, "Could not allocate an e2e_expected_unit_t");

    *element = (e2e_idata_unit_t) {
        .interface = interface,
        .values.object = expected_object,
    };

    sys_dnode_init(&element->node);
    sys_dlist_append(idata->list, &element->node);

    return 0;
}

bool idata_is_empty(e2e_idata_t *idata)
{
    return sys_dlist_is_empty(idata->list);
}

int idata_get_individual(
    e2e_idata_t *idata, const char *interface, e2e_individual_data_t **out_individual)
{
    e2e_idata_unit_t *unit = NULL;

    if (*out_individual != NULL) {
        unit = CONTAINER_OF(*out_individual, e2e_idata_unit_t, values.individual);
        // we expect the passed pointer to be the last match so we skip it (i dont like it)
        unit = idata_iter_next(idata, unit);
    } else {
        unit = idata_iter(idata);
    }

    unit = idata_get_fist_interface_unit(idata, unit, interface);

    if (!unit) {
        *out_individual = NULL;
        return 0;
    }

    CHECK_RET_1(unit->interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect interface aggregation associated with interface name passed");
    CHECK_RET_1(unit->interface->type == ASTARTE_INTERFACE_TYPE_PROPERTIES,
        "Incorrect interface type associated with interface name passed");

    *out_individual = &unit->values.individual;
    return 0;
}

int idata_get_property(
    e2e_idata_t *idata, const char *interface, e2e_property_data_t **out_property)
{
    e2e_idata_unit_t *unit = NULL;

    if (*out_property != NULL) {
        unit = CONTAINER_OF(*out_property, e2e_idata_unit_t, values.property);
        // we expect the passed pointer to be the last match so we skip it (i dont like it)
        unit = idata_iter_next(idata, unit);
    } else {
        unit = idata_iter(idata);
    }

    unit = idata_get_fist_interface_unit(idata, unit, interface);

    if (!unit) {
        *out_property = NULL;
        return 0;
    }

    CHECK_RET_1(unit->interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL,
        "Incorrect interface aggregation associated with interface name passed");
    CHECK_RET_1(unit->interface->type != ASTARTE_INTERFACE_TYPE_PROPERTIES,
        "Incorrect interface type associated with interface name passed");

    *out_property = &unit->values.property;
    return 0;
}

int idata_get_object(e2e_idata_t *idata, const char *interface, e2e_object_data_t **out_object)
{
    e2e_idata_unit_t *unit = NULL;

    if (*out_object != NULL) {
        unit = CONTAINER_OF(*out_object, e2e_idata_unit_t, values.object);
        // we expect the passed pointer to be the last match so we skip it (i dont like it)
        unit = idata_iter_next(idata, unit);
    } else {
        unit = idata_iter(idata);
    }

    unit = idata_get_fist_interface_unit(idata, unit, interface);

    if (!unit) {
        *out_object = NULL;
        return 0;
    }

    CHECK_RET_1(unit->interface->aggregation != ASTARTE_INTERFACE_AGGREGATION_OBJECT,
        "Incorrect interface aggregation associated with interface name passed");

    *out_object = &unit->values.object;
    return 0;
}

e2e_idata_unit_t *idata_iter(e2e_idata_t *idata)
{
    e2e_idata_unit_t *current = NULL;

    return SYS_DLIST_PEEK_HEAD_CONTAINER(idata->list, current, node);
}

e2e_idata_unit_t *idata_iter_next(e2e_idata_t *idata, e2e_idata_unit_t *current)
{
    return SYS_DLIST_PEEK_NEXT_CONTAINER(idata->list, current, node);
}

int idata_remove_individual(e2e_individual_data_t *individual)
{
    e2e_idata_unit_t *unit = CONTAINER_OF(individual, e2e_idata_unit_t, values.individual);

    return idata_remove(unit);
}

int idata_remove_property(e2e_property_data_t *property)
{
    e2e_idata_unit_t *unit = CONTAINER_OF(property, e2e_idata_unit_t, values.property);

    return idata_remove(unit);
}

int idata_remove_object(e2e_object_data_t *object)
{
    e2e_idata_unit_t *unit = CONTAINER_OF(object, e2e_idata_unit_t, values.object);

    return idata_remove(unit);
}

int idata_remove(e2e_idata_unit_t *idata_unit)
{
    if (idata_unit->interface->type == ASTARTE_INTERFACE_TYPE_PROPERTIES) {
        e2e_property_data_t *property_data = &idata_unit->values.property;

        // unsets do not store an individual value
        if (!property_data->unset) {
            astarte_data_destroy_deserialized(property_data->data);
        }

        free((char *) property_data->path);
    } else if (idata_unit->interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_INDIVIDUAL) {
        e2e_individual_data_t *individual_data = &idata_unit->values.individual;

        free((char *) individual_data->path);
        astarte_data_destroy_deserialized(individual_data->data);
    } else if (idata_unit->interface->aggregation == ASTARTE_INTERFACE_AGGREGATION_OBJECT) {
        e2e_object_data_t *object_data = &idata_unit->values.object;

        free((char *) object_data->path);
        free(object_data->object_bytes.buf);
        astarte_object_entries_destroy_deserialized(
            object_data->entries.buf, object_data->entries.len);
    } else {
        CHECK_HALT(true, "Unkown interface type");
    }

    sys_dlist_remove(&idata_unit->node);
    free(idata_unit);
    return 0;
}

void idata_free()
{
    free(idata.iface_map.config);
    free(idata.iface_map.data);
}

/************************************************
 * Static functions definitions
 ***********************************************/

static uint64_t idata_hash_intf(const astarte_interface_t *interface) {
    return idata_hash_name(interface->name, strlen(interface->name));
}

static uint64_t idata_hash_name(const char *interface_name, size_t len) {
    return idata->hash_fn(interface_name, strlen(interface_name));
}

static void idata_init_interface(struct sys_hashmap *interface_map, const astarte_interface_t *interface)
{
    uint64_t key = idata_hash_intf(interface);
    idata_map_value_t value = (idata_map_value_t) {
        .interface = interface,
        .expected = SPSC_INITIALIZER(ARRAY_SIZE(value.expected_buf), value.expected_buf),
        .expected_buf = { 0 },
    };

    sys_hashmap_insert(&idata->iface_map, key, value, NULL);
}
