#include "shell_handlers.h"

#include <astarte_device_sdk/device.h>
#include <astarte_device_sdk/interface.h>
#include <data_private.h>
#include <interface_private.h>
#include <object_private.h>

#include "device_handler.h"
#include "idata.h"
#include "utilities.h"

LOG_MODULE_REGISTER(shell_handlers, CONFIG_SHELL_HANDLERS_LOG_LEVEL); // NOLINT

static idata_handle_t idata;
static astarte_device_handle_t device;

static const astarte_interface_t *next_interface_parameter(
    char ***args, size_t *argc);
static int parse_alloc_astarte_invividual(const astarte_interface_t *interface, char *path,
    e2e_byte_array *buf, astarte_data_t *out_data);
static int parse_alloc_astarte_object(const astarte_interface_t *interface, char *path,
    e2e_byte_array *buf, astarte_object_entry_t **entries, size_t *entries_length);

void init_shell(astarte_device_handle_t device_new, idata_handle_t idata_new) {
    device = device_new;
    idata = idata_new;
}

// start expect commands handler
int cmd_expect_individual_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Expect individual command handler"); // NOLINT

    char *path = NULL;
    e2e_byte_array individual_value = { 0 };
    astarte_data_t data = { 0 };
    // the first parameter is the command name
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");
    individual_value = next_alloc_base64_parameter(&argv, &argc);
    CHECK_GOTO(individual_value.len == 0, cleanup, "Invalid individual parameter passed");
    e2e_timestamp_option_t timestamp = next_timestamp_parameter(&argv, &argc);

    CHECK_GOTO(parse_alloc_astarte_invividual(interface, path, &individual_value, &data) != 0,
        cleanup, "Could not parse and allocate astarte individual");

    // path and individual will be freed by the idata_unit free function
    CHECK_GOTO(idata_expect_individual(idata, interface, (e2e_individual_data_t) {
                       .data = data,
                       .path = path,
                       .timestamp = timestamp,
                   })
            != 0,
        cleanup, "Could not insert individual in expected_data");

    // should get freed even if no errors occur because it is not stored anywhere and it's not
    // needed
    free(individual_value.buf);
    return 0;

cleanup:
    free(path);
    free(individual_value.buf);
    astarte_data_destroy_deserialized(data);
    return 1;
}

int cmd_expect_object_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Expect object command handler"); // NOLINT

    char *path = NULL;
    e2e_byte_array object_bytes = { 0 };
    astarte_object_entry_t *entries = { 0 };
    size_t entries_length = { 0 };
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");
    object_bytes = next_alloc_base64_parameter(&argv, &argc);
    CHECK_GOTO(object_bytes.len == 0, cleanup, "Invalid object parameter passed");
    e2e_timestamp_option_t timestamp = next_timestamp_parameter(&argv, &argc);

    CHECK_GOTO(
        parse_alloc_astarte_object(interface, path, &object_bytes, &entries, &entries_length) != 0,
        cleanup, "Could not parse and allocate astarte object entries");

    // path, object_bytes and object_entries will be freed by the idata_unit free function
    CHECK_GOTO(idata_expect_object(idata, interface, (e2e_object_data_t) {
             .entries = {
                 .buf = entries,
                 .len = entries_length,
             },
            .path = path,
            // do not free in case of objects since entries keys are pointing to this buffer
            .object_bytes = object_bytes,
            .timestamp = timestamp,
         }) != 0, cleanup, "Could not add object entry to idata list");

    return 0;

cleanup:
    free(path);
    free(object_bytes.buf);
    astarte_object_entries_destroy_deserialized(entries, entries_length);
    return 1;
}

int cmd_expect_property_set_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Expect set property command handler"); // NOLINT

    char *path = NULL;
    e2e_byte_array property_value = { 0 };
    astarte_data_t data = { 0 };
    // ignore the first parameter since it's the name of the command itself
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    // path check should not fail no checks performed
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");
    property_value = next_alloc_base64_parameter(&argv, &argc);
    CHECK_GOTO(property_value.len == 0, cleanup, "Invalid data parameter passed");

    CHECK_GOTO(parse_alloc_astarte_invividual(interface, path, &property_value, &data) != 0,
        cleanup, "Could not deserialize and allocate astarte data");

    // path and data will be freed by the idata_unit free function
    CHECK_GOTO(idata_expect_property(idata, interface,
                   (e2e_property_data_t) {
                       .data = data,
                       .path = path,
                   })
            != 0,
        cleanup, "Could not add property to idata list");

    // should get freed even if no errors occur because it is not stored anywhere and it's not
    // needed
    free(property_value.buf);
    return 0;

cleanup:
    free(path);
    free(property_value.buf);
    astarte_data_destroy_deserialized(data);
    return 1;
}

int cmd_expect_property_unset_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Expect unset property command handler"); // NOLINT

    char *path = NULL;
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");

    CHECK_GOTO(idata_expect_property(idata, interface,
                   (e2e_property_data_t) {
                       .path = path,
                       .unset = true,
                   })
            != 0,
        cleanup, "Could not add property to idata list");

    return 0;

cleanup:
    free(path);
    return 1;
}

//int cmd_expect_verify_handler(const struct shell *sh, size_t argc, char **argv)
//{
//    wait_for_connection();
//
//    LOG_INF("Expect verify command handler"); // NOLINT
//
//    size_t num = 0;
//
//    CHECK_RET_1(
//        k_mutex_lock(&expected_data_mutex, K_FOREVER) != 0, "Could not lock expected data mutex");
//
//    e2e_idata_unit_t *expected_iter = idata_iter(&expected_data);
//
//    while (expected_iter) {
//        ASTARTE_LOG_ERR(
//            "Expected interface '%s' data not received", expected_iter->interface->name);
//        idata_unit_log(expected_iter);
//
//        num += 1;
//        expected_iter = idata_iter_next(&expected_data, expected_iter);
//    }
//
//    if (num == 0) {
//        ASTARTE_LOG_DBG("No more expected data");
//
//        if (unexpected_data_count > 0) {
//            ASTARTE_LOG_DBG("Received %zu units of unexpected data", expected_data_count);
//            shell_print(sh, "Received unexpected data");
//        } else if (expected_data_count > 0) {
//            ASTARTE_LOG_DBG("Received %zu units of data", expected_data_count);
//            shell_print(sh, "All expected data received");
//        } else {
//            ASTARTE_LOG_WRN("No data was received but there was no expected data");
//            shell_print(sh, "No data was expected and no data was received");
//        }
//    } else {
//        shell_print(sh, "Missing %zu expected units of data", num);
//    }
//
//    // reset expected and unexpected data counter
//    unexpected_data_count = 0;
//    expected_data_count = 0;
//    k_mutex_unlock(&expected_data_mutex);
//
//    return 0;
//}
//

// start send commands handlers
int cmd_send_individual_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Send individual command handler"); // NOLINT

    int return_code = 1;

    char *path = NULL;
    e2e_byte_array individual_value = { 0 };
    astarte_data_t data = { 0 };
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");
    individual_value = next_alloc_base64_parameter(&argv, &argc);
    CHECK_GOTO(individual_value.len == 0, cleanup, "Invalid individual parameter passed");
    e2e_timestamp_option_t timestamp = next_timestamp_parameter(&argv, &argc);

    CHECK_GOTO(parse_alloc_astarte_invividual(interface, path, &individual_value, &data) != 0,
        cleanup, "Could not parse and allocate astarte individual");

    astarte_result_t res = { 0 };
    if (timestamp.present) {
        res = astarte_device_send_individual(
            device, interface->name, path, data, &timestamp.value);
    } else {
        res = astarte_device_send_individual(device, interface->name, path, data, NULL);
    }
    CHECK_ASTARTE_OK_GOTO(res, cleanup, "Failed to send individual to astarte");

    shell_print(sh, "Sent individual");
    return_code = 0;

cleanup:
    astarte_data_destroy_deserialized(data);
    free(individual_value.buf);
    free(path);

    return return_code;
}

int cmd_send_object_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Send object command handler"); // NOLINT

    int return_code = 1;

    char *path = NULL;
    e2e_byte_array object_bytes = { 0 };
    astarte_object_entry_t *entries = { 0 };
    size_t entries_length = { 0 };
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");
    object_bytes = next_alloc_base64_parameter(&argv, &argc);
    CHECK_GOTO(object_bytes.len == 0, cleanup, "Invalid object parameter passed");
    e2e_timestamp_option_t timestamp = next_timestamp_parameter(&argv, &argc);

    CHECK_GOTO(
        parse_alloc_astarte_object(interface, path, &object_bytes, &entries, &entries_length) != 0,
        cleanup, "Could not parse and allocate astarte object entries");

    astarte_result_t res = { 0 };
    if (timestamp.present) {
        res = astarte_device_send_object(
            device, interface->name, path, entries, entries_length, &timestamp.value);
    } else {
        res = astarte_device_send_object(
            device, interface->name, path, entries, entries_length, NULL);
    }
    CHECK_ASTARTE_OK_GOTO(res, cleanup, "Failed to send object to astarte");

    shell_print(sh, "Sent object");
    return_code = 0;

cleanup:
    astarte_object_entries_destroy_deserialized(entries, entries_length);
    free(object_bytes.buf);
    free(path);

    return return_code;
}

int cmd_send_property_set_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Set property command handler"); // NOLINT

    int return_code = 1;

    char *path = NULL;
    e2e_byte_array property_value = { 0 };
    astarte_data_t data = { 0 };
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");
    property_value = next_alloc_base64_parameter(&argv, &argc);
    CHECK_GOTO(property_value.len == 0, cleanup, "Invalid data parameter passed");

    CHECK_GOTO(parse_alloc_astarte_invividual(interface, path, &property_value, &data) != 0,
        cleanup, "Could not parse and allocate data");

    astarte_result_t res = astarte_device_set_property(device, interface->name, path, data);
    CHECK_ASTARTE_OK_GOTO(res, cleanup, "Failed to send set property to astarte");

    shell_print(sh, "Property set");
    return_code = 0;

cleanup:
    astarte_data_destroy_deserialized(data);
    free(property_value.buf);
    free(path);

    return return_code;
}

int cmd_send_property_unset_handler(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Unset property command handler"); // NOLINT

    int return_code = 1;

    char *path = NULL;
    skip_parameter(&argv, &argc);
    const astarte_interface_t *interface = next_interface_parameter(&argv, &argc);
    CHECK_GOTO(!interface, cleanup, "Invalid interface name passed");
    path = next_alloc_string_parameter(&argv, &argc);
    CHECK_GOTO(!path, cleanup, "Invalid path parameter passed");

    astarte_result_t res = astarte_device_unset_property(device, interface->name, path);
    CHECK_ASTARTE_OK_GOTO(res, cleanup, "Failed to send set property to astarte");

    shell_print(sh, "Property unset");
    return_code = 0;

cleanup:
    free(path);

    return return_code;
}

int cmd_disconnect(const struct shell *sh, size_t argc, char **argv)
{
    LOG_INF("Disconnect command handler"); // NOLINT

    LOG_INF("Stopping and joining the astarte device polling thread."); // NOLINT
    set_termination();

    return 0;
}


static const astarte_interface_t *next_interface_parameter(
    char ***args, size_t *argc)
{
    if (*argc < 1) {
        // no more arguments
        return NULL;
    }

    const char *const interface_name = (*args)[0];
    const astarte_interface_t *interface = idata_get_interface(idata, interface_name);

    if (!interface) {
        // no interface with name specified found
        LOG_ERR("Invalid interface name %s", interface_name); // NOLINT
        return NULL;
    }

    // move to the next parameter for caller
    *args += 1;
    *argc -= 1;
    return interface;
}

// this also implicitly checks that the passed path is valid
static int parse_alloc_astarte_invividual(
    const astarte_interface_t *interface, char *path, e2e_byte_array *buf, astarte_data_t *out_data)
{
    const astarte_mapping_t *mapping = NULL;
    astarte_result_t res = astarte_interface_get_mapping_from_path(interface, path, &mapping);
    CHECK_ASTARTE_OK_RET_1(
        res, "Error while searching for the mapping (%d) %s", res, astarte_result_to_name(res));

    CHECK_RET_1(!astarte_bson_deserializer_check_validity(buf->buf, buf->len),
        "Invalid BSON document in data");
    astarte_bson_document_t full_document = astarte_bson_deserializer_init_doc(buf->buf);
    astarte_bson_element_t v_elem = { 0 };
    CHECK_ASTARTE_OK_RET_1(astarte_bson_deserializer_element_lookup(full_document, "v", &v_elem),
        "Cannot retrieve BSON value from data");

    CHECK_ASTARTE_OK_RET_1(astarte_data_deserialize(v_elem, mapping->type, out_data),
        "Couldn't deserialize received binary data into object entries");

    return 0;
}

// this also implicitly checks that the passed path is valid
static int parse_alloc_astarte_object(const astarte_interface_t *interface, char *path,
    e2e_byte_array *buf, astarte_object_entry_t **out_entries, size_t *out_entries_length)
{
    // Since the function expects a bson element we need to receive a "v" value like it would be
    // sent to astarte
    CHECK_RET_1(!astarte_bson_deserializer_check_validity(buf->buf, buf->len),
        "Invalid BSON document in data");
    astarte_bson_document_t full_document = astarte_bson_deserializer_init_doc(buf->buf);
    astarte_bson_element_t v_elem = { 0 };
    CHECK_ASTARTE_OK_RET_1(astarte_bson_deserializer_element_lookup(full_document, "v", &v_elem),
        "Cannot retrieve BSON value from data");

    CHECK_ASTARTE_OK_RET_1(astarte_object_entries_deserialize(
                               v_elem, interface, path, out_entries, out_entries_length),
        "Couldn't deserialize received binary data into object entries");

    return 0;
}
