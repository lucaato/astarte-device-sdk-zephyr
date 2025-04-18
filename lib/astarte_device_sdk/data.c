/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "astarte_device_sdk/data.h"
#include "data_private.h"

#include <stdlib.h>

#include "bson_types.h"
#include "interface_private.h"
#include "log.h"
#include "mapping_private.h"

ASTARTE_LOG_MODULE_REGISTER(astarte_data, CONFIG_ASTARTE_DEVICE_SDK_DATA_LOG_LEVEL);

/************************************************
 *         Static functions declaration         *
 ***********************************************/

/**
 * @brief Fill an empty array Astarte data of the required type.
 *
 * @param[in] type Mapping type to use for the Astarte data.
 * @param[out] data The Astarte data to fill.
 * @return An Astarte result that may take the following values:
 * @retval ASTARTE_RESULT_OK upon success
 * @retval ASTARTE_RESULT_INTERNAL_ERROR if the input mapping type is not an array.
 */
static astarte_result_t initialize_empty_array(astarte_mapping_type_t type, astarte_data_t *data);

/**
 * @brief Deserialize a scalar bson element.
 *
 * @param[in] bson_elem BSON element to deserialize.
 * @param[in] type The expected type for the Astarte data.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_scalar(
    astarte_bson_element_t bson_elem, astarte_mapping_type_t type, astarte_data_t *data);

/**
 * @brief Deserialize a bson element containing a binaryblob.
 *
 * @note This function will perform dynamic allocation.
 *
 * @param[in] bson_elem BSON element to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_binaryblob(
    astarte_bson_element_t bson_elem, astarte_data_t *data);

/**
 * @brief Deserialize a bson element containing a string.
 *
 * @note This function will perform dynamic allocation.
 *
 * @param[in] bson_elem BSON element to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_string(astarte_bson_element_t bson_elem, astarte_data_t *data);

/**
 * @brief Deserialize a bson element containing an array.
 *
 * @param[in] bson_elem BSON element to deserialize.
 * @param[in] type The expected type for the Astarte data.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array(
    astarte_bson_element_t bson_elem, astarte_mapping_type_t type, astarte_data_t *data);

/**
 * @brief Deserialize a bson element containing an array of doubles.
 *
 * @param[in] bson_doc BSON document containing the array to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @param[in] array_length The number of elements of the BSON array.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array_double(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length);

/**
 * @brief Deserialize a bson element containing an array of strings.
 *
 * @param[in] bson_doc BSON document containing the array to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @param[in] array_length The number of elements of the BSON array.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array_string(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length);

/**
 * @brief Deserialize a bson element containing an array of booleans.
 *
 * @param[in] bson_doc BSON document containing the array to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @param[in] array_length The number of elements of the BSON array.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array_bool(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length);

/**
 * @brief Deserialize a bson element containing an array of datetimes.
 *
 * @param[in] bson_doc BSON document containing the array to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @param[in] array_length The number of elements of the BSON array.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array_datetime(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length);

/**
 * @brief Deserialize a bson element containing an array of integers.
 *
 * @param[in] bson_doc BSON document containing the array to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @param[in] array_length The number of elements of the BSON array.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array_int32(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length);

/**
 * @brief Deserialize a bson element containing an array of long integers.
 *
 * @param[in] bson_doc BSON document containing the array to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @param[in] array_length The number of elements of the BSON array.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array_int64(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length);

/**
 * @brief Deserialize a bson element containing an array of binary blobs.
 *
 * @param[in] bson_doc BSON document containing the array to deserialize.
 * @param[out] data The Astarte data where to store the deserialized data.
 * @param[in] array_length The number of elements of the BSON array.
 * @return ASTARTE_RESULT_OK upon success, an error code otherwise.
 */
static astarte_result_t deserialize_array_binblob(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length);

/**
 * @brief Check if a BSON type is compatible with a mapping type.
 *
 * @param[in] mapping_type Mapping type to evaluate
 * @param[in] bson_type BSON type to evaluate.
 * @return true if the BSON type and mapping type are compatible, false otherwise.
 */
static bool check_if_bson_type_is_mapping_type(
    astarte_mapping_type_t mapping_type, uint8_t bson_type);

/************************************************
 *     Global public functions definitions      *
 ***********************************************/

// clang-format off
#define MAKE_FUNCTION_DATA_FROM(NAME, ENUM, TYPE, PARAM)                                           \
    astarte_data_t astarte_data_from_##NAME(TYPE PARAM)                                            \
    {                                                                                              \
        return (astarte_data_t) {                                                                  \
            .data = {                                                                              \
                .PARAM = (PARAM),                                                                  \
            },                                                                                     \
            .tag = (ENUM),                                                                         \
        };                                                                                         \
    }

#define MAKE_FUNCTION_DATA_FROM_ARRAY(NAME, ENUM, TYPE, PARAM)                                     \
    astarte_data_t astarte_data_from_##NAME(TYPE PARAM, size_t len)                                \
    {                                                                                              \
        return (astarte_data_t) {                                                                  \
            .data = {                                                                              \
                .PARAM = {                                                                         \
                    .buf = (PARAM),                                                                \
                    .len = len,                                                                    \
                },                                                                                 \
            },                                                                                     \
            .tag = (ENUM),                                                                         \
        };                                                                                         \
    }
// clang-format on

MAKE_FUNCTION_DATA_FROM_ARRAY(binaryblob, ASTARTE_MAPPING_TYPE_BINARYBLOB, void *, binaryblob)
MAKE_FUNCTION_DATA_FROM(boolean, ASTARTE_MAPPING_TYPE_BOOLEAN, bool, boolean)
MAKE_FUNCTION_DATA_FROM(datetime, ASTARTE_MAPPING_TYPE_DATETIME, int64_t, datetime)
MAKE_FUNCTION_DATA_FROM(double, ASTARTE_MAPPING_TYPE_DOUBLE, double, dbl)
MAKE_FUNCTION_DATA_FROM(integer, ASTARTE_MAPPING_TYPE_INTEGER, int32_t, integer)
MAKE_FUNCTION_DATA_FROM(longinteger, ASTARTE_MAPPING_TYPE_LONGINTEGER, int64_t, longinteger)
MAKE_FUNCTION_DATA_FROM(string, ASTARTE_MAPPING_TYPE_STRING, const char *, string)

astarte_data_t astarte_data_from_binaryblob_array(const void **blobs, size_t *sizes, size_t count)
{
    return (astarte_data_t) {
        .data = {
            .binaryblob_array = {
                .blobs = blobs,
                .sizes = sizes,
                .count = count,
            },
        },
        .tag = ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY,
    };
}

MAKE_FUNCTION_DATA_FROM_ARRAY(
    boolean_array, ASTARTE_MAPPING_TYPE_BOOLEANARRAY, bool *, boolean_array)
MAKE_FUNCTION_DATA_FROM_ARRAY(
    datetime_array, ASTARTE_MAPPING_TYPE_DATETIMEARRAY, int64_t *, datetime_array)
MAKE_FUNCTION_DATA_FROM_ARRAY(
    double_array, ASTARTE_MAPPING_TYPE_DOUBLEARRAY, double *, double_array)
MAKE_FUNCTION_DATA_FROM_ARRAY(
    integer_array, ASTARTE_MAPPING_TYPE_INTEGERARRAY, int32_t *, integer_array)
MAKE_FUNCTION_DATA_FROM_ARRAY(
    longinteger_array, ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY, int64_t *, longinteger_array)
MAKE_FUNCTION_DATA_FROM_ARRAY(
    string_array, ASTARTE_MAPPING_TYPE_STRINGARRAY, const char **, string_array)

astarte_mapping_type_t astarte_data_get_type(astarte_data_t data)
{
    return data.tag;
}

// clang-format off
// NOLINTBEGIN(bugprone-macro-parentheses)
#define MAKE_FUNCTION_DATA_TO(NAME, ENUM, TYPE, PARAM)                                             \
    astarte_result_t astarte_data_to_##NAME(astarte_data_t data, TYPE *PARAM)                      \
    {                                                                                              \
        if (!(PARAM) || (data.tag != (ENUM))) {                                                    \
            ASTARTE_LOG_ERR("Conversion from Astarte data to %s error.", #NAME);                   \
            return ASTARTE_RESULT_INVALID_PARAM;                                                   \
        }                                                                                          \
        *PARAM = data.data.PARAM;                                                                  \
        return ASTARTE_RESULT_OK;                                                                  \
    }

#define MAKE_FUNCTION_DATA_TO_ARRAY(NAME, ENUM, TYPE, PARAM)                                       \
    astarte_result_t astarte_data_to_##NAME(                                                       \
        astarte_data_t data, TYPE *PARAM, size_t *len)                                             \
    {                                                                                              \
        if (!(PARAM) || !len || (data.tag != (ENUM))) {                                            \
            ASTARTE_LOG_ERR("Conversion from Astarte data to %s error.", #NAME);                   \
            return ASTARTE_RESULT_INVALID_PARAM;                                                   \
        }                                                                                          \
        *PARAM = data.data.PARAM.buf;                                                              \
        *len = data.data.PARAM.len;                                                                \
        return ASTARTE_RESULT_OK;                                                                  \
    }
// NOLINTEND(bugprone-macro-parentheses)
// clang-format on

MAKE_FUNCTION_DATA_TO_ARRAY(binaryblob, ASTARTE_MAPPING_TYPE_BINARYBLOB, void *, binaryblob)
MAKE_FUNCTION_DATA_TO(boolean, ASTARTE_MAPPING_TYPE_BOOLEAN, bool, boolean)
MAKE_FUNCTION_DATA_TO(datetime, ASTARTE_MAPPING_TYPE_DATETIME, int64_t, datetime)
MAKE_FUNCTION_DATA_TO(double, ASTARTE_MAPPING_TYPE_DOUBLE, double, dbl)
MAKE_FUNCTION_DATA_TO(integer, ASTARTE_MAPPING_TYPE_INTEGER, int32_t, integer)
MAKE_FUNCTION_DATA_TO(longinteger, ASTARTE_MAPPING_TYPE_LONGINTEGER, int64_t, longinteger)
MAKE_FUNCTION_DATA_TO(string, ASTARTE_MAPPING_TYPE_STRING, const char *, string)

astarte_result_t astarte_data_to_binaryblob_array(
    astarte_data_t data, const void ***blobs, size_t **sizes, size_t *count)
{
    if (!blobs || !sizes || !count || (data.tag != ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY)) {
        ASTARTE_LOG_ERR("Conversion from Astarte data to binaryblob_array error.");
        return ASTARTE_RESULT_INVALID_PARAM;
    }
    *blobs = data.data.binaryblob_array.blobs;
    *sizes = data.data.binaryblob_array.sizes;
    *count = data.data.binaryblob_array.count;
    return ASTARTE_RESULT_OK;
}

MAKE_FUNCTION_DATA_TO_ARRAY(boolean_array, ASTARTE_MAPPING_TYPE_BOOLEANARRAY, bool *, boolean_array)
MAKE_FUNCTION_DATA_TO_ARRAY(
    datetime_array, ASTARTE_MAPPING_TYPE_DATETIMEARRAY, int64_t *, datetime_array)
MAKE_FUNCTION_DATA_TO_ARRAY(double_array, ASTARTE_MAPPING_TYPE_DOUBLEARRAY, double *, double_array)
MAKE_FUNCTION_DATA_TO_ARRAY(
    integer_array, ASTARTE_MAPPING_TYPE_INTEGERARRAY, int32_t *, integer_array)
MAKE_FUNCTION_DATA_TO_ARRAY(
    longinteger_array, ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY, int64_t *, longinteger_array)
MAKE_FUNCTION_DATA_TO_ARRAY(
    string_array, ASTARTE_MAPPING_TYPE_STRINGARRAY, const char **, string_array)

/************************************************
 *     Global private functions definitions     *
 ***********************************************/

astarte_result_t astarte_data_serialize(
    astarte_bson_serializer_t *bson, const char *key, astarte_data_t data)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;

    switch (data.tag) {
        case ASTARTE_MAPPING_TYPE_INTEGER:
            astarte_bson_serializer_append_int32(bson, key, data.data.integer);
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGER:
            astarte_bson_serializer_append_int64(bson, key, data.data.longinteger);
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLE:
            astarte_bson_serializer_append_double(bson, key, data.data.dbl);
            break;
        case ASTARTE_MAPPING_TYPE_STRING:
            astarte_bson_serializer_append_string(bson, key, data.data.string);
            break;
        case ASTARTE_MAPPING_TYPE_BINARYBLOB: {
            astarte_data_binaryblob_t binaryblob = data.data.binaryblob;
            astarte_bson_serializer_append_binary(bson, key, binaryblob.buf, binaryblob.len);
            break;
        }
        case ASTARTE_MAPPING_TYPE_BOOLEAN:
            astarte_bson_serializer_append_boolean(bson, key, data.data.boolean);
            break;
        case ASTARTE_MAPPING_TYPE_DATETIME:
            astarte_bson_serializer_append_datetime(bson, key, data.data.datetime);
            break;
        case ASTARTE_MAPPING_TYPE_INTEGERARRAY: {
            astarte_data_integerarray_t int32_array = data.data.integer_array;
            ares = astarte_bson_serializer_append_int32_array(
                bson, key, int32_array.buf, (int) int32_array.len);
            break;
        }
        case ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY: {
            astarte_data_longintegerarray_t int64_array = data.data.longinteger_array;
            ares = astarte_bson_serializer_append_int64_array(
                bson, key, int64_array.buf, (int) int64_array.len);
            break;
        }
        case ASTARTE_MAPPING_TYPE_DOUBLEARRAY: {
            astarte_data_doublearray_t double_array = data.data.double_array;
            ares = astarte_bson_serializer_append_double_array(
                bson, key, double_array.buf, (int) double_array.len);
            break;
        }
        case ASTARTE_MAPPING_TYPE_STRINGARRAY: {
            astarte_data_stringarray_t string_array = data.data.string_array;
            ares = astarte_bson_serializer_append_string_array(
                bson, key, string_array.buf, (int) string_array.len);
            break;
        }
        case ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY: {
            astarte_data_binaryblobarray_t binary_arrays = data.data.binaryblob_array;
            ares = astarte_bson_serializer_append_binary_array(
                bson, key, binary_arrays.blobs, binary_arrays.sizes, (int) binary_arrays.count);
            break;
        }
        case ASTARTE_MAPPING_TYPE_BOOLEANARRAY: {
            astarte_data_booleanarray_t bool_array = data.data.boolean_array;
            ares = astarte_bson_serializer_append_boolean_array(
                bson, key, bool_array.buf, (int) bool_array.len);
            break;
        }
        case ASTARTE_MAPPING_TYPE_DATETIMEARRAY: {
            astarte_data_longintegerarray_t dt_array = data.data.datetime_array;
            ares = astarte_bson_serializer_append_datetime_array(
                bson, key, dt_array.buf, (int) dt_array.len);
            break;
        }
        default:
            ares = ASTARTE_RESULT_INVALID_PARAM;
            break;
    }

    return ares;
}

astarte_result_t astarte_data_deserialize(
    astarte_bson_element_t bson_elem, astarte_mapping_type_t type, astarte_data_t *data)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;

    switch (type) {
        case ASTARTE_MAPPING_TYPE_BINARYBLOB:
        case ASTARTE_MAPPING_TYPE_BOOLEAN:
        case ASTARTE_MAPPING_TYPE_DATETIME:
        case ASTARTE_MAPPING_TYPE_DOUBLE:
        case ASTARTE_MAPPING_TYPE_INTEGER:
        case ASTARTE_MAPPING_TYPE_LONGINTEGER:
        case ASTARTE_MAPPING_TYPE_STRING:
            ares = deserialize_scalar(bson_elem, type, data);
            break;
        case ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY:
        case ASTARTE_MAPPING_TYPE_BOOLEANARRAY:
        case ASTARTE_MAPPING_TYPE_DATETIMEARRAY:
        case ASTARTE_MAPPING_TYPE_DOUBLEARRAY:
        case ASTARTE_MAPPING_TYPE_INTEGERARRAY:
        case ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY:
        case ASTARTE_MAPPING_TYPE_STRINGARRAY:
            ares = deserialize_array(bson_elem, type, data);
            break;
        default:
            ASTARTE_LOG_ERR("Unsupported mapping type.");
            ares = ASTARTE_RESULT_INTERNAL_ERROR;
            break;
    }
    return ares;
}

void astarte_data_destroy_deserialized(astarte_data_t data)
{
    switch (data.tag) {
        case ASTARTE_MAPPING_TYPE_BINARYBLOB:
            free(data.data.binaryblob.buf);
            break;
        case ASTARTE_MAPPING_TYPE_STRING:
            free((void *) data.data.string);
            break;
        case ASTARTE_MAPPING_TYPE_INTEGERARRAY:
            free(data.data.integer_array.buf);
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY:
            free(data.data.longinteger_array.buf);
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLEARRAY:
            free(data.data.double_array.buf);
            break;
        case ASTARTE_MAPPING_TYPE_STRINGARRAY:
            for (size_t i = 0; i < data.data.string_array.len; i++) {
                free((void *) data.data.string_array.buf[i]);
            }
            free((void *) data.data.string_array.buf);
            break;
        case ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY:
            for (size_t i = 0; i < data.data.binaryblob_array.count; i++) {
                free((void *) data.data.binaryblob_array.blobs[i]);
            }
            free(data.data.binaryblob_array.sizes);
            free((void *) data.data.binaryblob_array.blobs);
            break;
        case ASTARTE_MAPPING_TYPE_BOOLEANARRAY:
            free(data.data.boolean_array.buf);
            break;
        case ASTARTE_MAPPING_TYPE_DATETIMEARRAY:
            free(data.data.datetime_array.buf);
            break;
        default:
            break;
    }
}

/************************************************
 *         Static functions definitions         *
 ***********************************************/

static astarte_result_t initialize_empty_array(astarte_mapping_type_t type, astarte_data_t *data)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;
    switch (type) {
        case ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY:
            data->tag = type;
            data->data.binaryblob_array.count = 0;
            data->data.binaryblob_array.blobs = NULL;
            data->data.binaryblob_array.sizes = NULL;
            break;
        case ASTARTE_MAPPING_TYPE_BOOLEANARRAY:
            data->tag = type;
            data->data.boolean_array.len = 0;
            data->data.boolean_array.buf = NULL;
            break;
        case ASTARTE_MAPPING_TYPE_DATETIMEARRAY:
            data->tag = type;
            data->data.datetime_array.len = 0;
            data->data.datetime_array.buf = NULL;
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLEARRAY:
            data->tag = type;
            data->data.double_array.len = 0;
            data->data.double_array.buf = NULL;
            break;
        case ASTARTE_MAPPING_TYPE_INTEGERARRAY:
            data->tag = type;
            data->data.integer_array.len = 0;
            data->data.integer_array.buf = NULL;
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY:
            data->tag = type;
            data->data.longinteger_array.len = 0;
            data->data.longinteger_array.buf = NULL;
            break;
        case ASTARTE_MAPPING_TYPE_STRINGARRAY:
            data->tag = type;
            data->data.string_array.len = 0;
            data->data.string_array.buf = NULL;
            break;
        default:
            ASTARTE_LOG_ERR("Creating empty array Astarte data for scalar mapping type.");
            ares = ASTARTE_RESULT_INTERNAL_ERROR;
            break;
    }
    return ares;
}

static astarte_result_t deserialize_scalar(
    astarte_bson_element_t bson_elem, astarte_mapping_type_t type, astarte_data_t *data)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;

    if (!check_if_bson_type_is_mapping_type(type, bson_elem.type)) {
        ASTARTE_LOG_ERR("BSON element is not of the expected type.");
        return ASTARTE_RESULT_BSON_DESERIALIZER_TYPES_ERROR;
    }

    switch (type) {
        case ASTARTE_MAPPING_TYPE_BINARYBLOB:
            ASTARTE_LOG_DBG("Deserializing binary blob data.");
            ares = deserialize_binaryblob(bson_elem, data);
            break;
        case ASTARTE_MAPPING_TYPE_BOOLEAN:
            ASTARTE_LOG_DBG("Deserializing boolean data.");
            bool bool_tmp = astarte_bson_deserializer_element_to_bool(bson_elem);
            *data = astarte_data_from_boolean(bool_tmp);
            break;
        case ASTARTE_MAPPING_TYPE_DATETIME:
            ASTARTE_LOG_DBG("Deserializing datetime data.");
            int64_t datetime_tmp = astarte_bson_deserializer_element_to_datetime(bson_elem);
            *data = astarte_data_from_datetime(datetime_tmp);
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLE:
            ASTARTE_LOG_DBG("Deserializing double data.");
            double double_tmp = astarte_bson_deserializer_element_to_double(bson_elem);
            *data = astarte_data_from_double(double_tmp);
            break;
        case ASTARTE_MAPPING_TYPE_INTEGER:
            ASTARTE_LOG_DBG("Deserializing integer data.");
            int32_t int32_tmp = astarte_bson_deserializer_element_to_int32(bson_elem);
            *data = astarte_data_from_integer(int32_tmp);
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGER:
            ASTARTE_LOG_DBG("Deserializing long integer data.");
            int64_t int64_tmp = 0U;
            if (bson_elem.type == ASTARTE_BSON_TYPE_INT32) {
                int64_tmp = (int64_t) astarte_bson_deserializer_element_to_int32(bson_elem);
            } else {
                int64_tmp = astarte_bson_deserializer_element_to_int64(bson_elem);
            }
            *data = astarte_data_from_longinteger(int64_tmp);
            break;
        case ASTARTE_MAPPING_TYPE_STRING:
            ASTARTE_LOG_DBG("Deserializing string data.");
            ares = deserialize_string(bson_elem, data);
            break;
        default:
            ASTARTE_LOG_ERR("Unsupported mapping type.");
            ares = ASTARTE_RESULT_INTERNAL_ERROR;
            break;
    }
    return ares;
}

static astarte_result_t deserialize_binaryblob(
    astarte_bson_element_t bson_elem, astarte_data_t *data)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;
    uint8_t *dyn_deserialized = NULL;

    uint32_t deserialized_len = 0;
    const uint8_t *deserialized
        = astarte_bson_deserializer_element_to_binary(bson_elem, &deserialized_len);

    dyn_deserialized = calloc(deserialized_len, sizeof(uint8_t));
    if (!dyn_deserialized) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }

    memcpy(dyn_deserialized, deserialized, deserialized_len);
    *data = astarte_data_from_binaryblob((void *) dyn_deserialized, deserialized_len);
    return ares;

failure:
    free(dyn_deserialized);
    return ares;
}

static astarte_result_t deserialize_string(astarte_bson_element_t bson_elem, astarte_data_t *data)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;
    char *dyn_deserialized = NULL;

    uint32_t deserialized_len = 0;
    const char *deserialized
        = astarte_bson_deserializer_element_to_string(bson_elem, &deserialized_len);

    dyn_deserialized = calloc(deserialized_len + 1, sizeof(char));
    if (!dyn_deserialized) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }

    strncpy(dyn_deserialized, deserialized, deserialized_len);
    *data = astarte_data_from_string(dyn_deserialized);
    return ares;

failure:
    free(dyn_deserialized);
    return ares;
}

static astarte_result_t deserialize_array(
    astarte_bson_element_t bson_elem, astarte_mapping_type_t type, astarte_data_t *data)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;

    if (bson_elem.type != ASTARTE_BSON_TYPE_ARRAY) {
        ASTARTE_LOG_ERR("Expected an array but BSON element type is %d.", bson_elem.type);
        return ASTARTE_RESULT_BSON_DESERIALIZER_TYPES_ERROR;
    }

    astarte_bson_document_t bson_doc = astarte_bson_deserializer_element_to_array(bson_elem);

    // Step 1: figure out the size and type of the array
    astarte_bson_element_t inner_elem = { 0 };
    ares = astarte_bson_deserializer_first_element(bson_doc, &inner_elem);
    if (ares != ASTARTE_RESULT_OK) {
        return initialize_empty_array(type, data);
    }
    size_t array_length = 0U;

    astarte_mapping_type_t scalar_type = ASTARTE_MAPPING_TYPE_BINARYBLOB;
    ares = astarte_mapping_array_to_scalar_type(type, &scalar_type);
    if (ares != ASTARTE_RESULT_OK) {
        ASTARTE_LOG_ERR("Non array type passed to deserialize_array.");
        return ares;
    }

    do {
        array_length++;
        if (!check_if_bson_type_is_mapping_type(scalar_type, inner_elem.type)) {
            ASTARTE_LOG_ERR("BSON array element is not of the expected type.");
            return ASTARTE_RESULT_BSON_DESERIALIZER_TYPES_ERROR;
        }
        ares = astarte_bson_deserializer_next_element(bson_doc, inner_elem, &inner_elem);
    } while (ares == ASTARTE_RESULT_OK);

    if (ares != ASTARTE_RESULT_NOT_FOUND) {
        return ares;
    }

    // Step 2: depending on the array type call the appropriate function
    switch (scalar_type) {
        case ASTARTE_MAPPING_TYPE_BINARYBLOB:
            ASTARTE_LOG_DBG("Deserializing array of binary blobs.");
            ares = deserialize_array_binblob(bson_doc, data, array_length);
            break;
        case ASTARTE_MAPPING_TYPE_BOOLEAN:
            ASTARTE_LOG_DBG("Deserializing array of booleans.");
            ares = deserialize_array_bool(bson_doc, data, array_length);
            break;
        case ASTARTE_MAPPING_TYPE_DATETIME:
            ASTARTE_LOG_DBG("Deserializing array of datetimes.");
            ares = deserialize_array_datetime(bson_doc, data, array_length);
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLE:
            ASTARTE_LOG_DBG("Deserializing array of doubles.");
            ares = deserialize_array_double(bson_doc, data, array_length);
            break;
        case ASTARTE_MAPPING_TYPE_INTEGER:
            ASTARTE_LOG_DBG("Deserializing array of integers.");
            ares = deserialize_array_int32(bson_doc, data, array_length);
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGER:
            ASTARTE_LOG_DBG("Deserializing array of long integers.");
            ares = deserialize_array_int64(bson_doc, data, array_length);
            break;
        case ASTARTE_MAPPING_TYPE_STRING:
            ASTARTE_LOG_DBG("Deserializing array of strings.");
            ares = deserialize_array_string(bson_doc, data, array_length);
            break;
        default:
            ASTARTE_LOG_ERR("Unsupported mapping type.");
            ares = ASTARTE_RESULT_INTERNAL_ERROR;
            break;
    }

    return ares;
}

// clang-format off
// NOLINTBEGIN(bugprone-macro-parentheses) Some can't be wrapped in parenthesis
#define MAKE_FUNCTION_DESERIALIZE_ARRAY(NAME, TYPE, TAG, UNION)                                    \
static astarte_result_t deserialize_array_##NAME(                                                  \
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length)                   \
{                                                                                                  \
    astarte_result_t ares = ASTARTE_RESULT_OK;                                                     \
    TYPE *array = calloc(array_length, sizeof(TYPE));                                              \
    if (!array) {                                                                                  \
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);                               \
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;                                                       \
        goto failure;                                                                              \
    }                                                                                              \
                                                                                                   \
    astarte_bson_element_t inner_elem = {0};                                                       \
    ares = astarte_bson_deserializer_first_element(bson_doc, &inner_elem);                         \
    if (ares != ASTARTE_RESULT_OK) {                                                               \
        goto failure;                                                                              \
    }                                                                                              \
    array[0] = astarte_bson_deserializer_element_to_##NAME(inner_elem);                            \
                                                                                                   \
    for (size_t i = 1; i < array_length; i++) {                                                    \
        ares = astarte_bson_deserializer_next_element(bson_doc, inner_elem, &inner_elem);          \
        if (ares != ASTARTE_RESULT_OK) {                                                           \
            goto failure;                                                                          \
        }                                                                                          \
        array[i] = astarte_bson_deserializer_element_to_##NAME(inner_elem);                        \
    }                                                                                              \
                                                                                                   \
    data->tag = (TAG);                                                                             \
    data->data.UNION.len = array_length;                                                           \
    data->data.UNION.buf = array;                                                                  \
    return ASTARTE_RESULT_OK;                                                                      \
                                                                                                   \
failure:                                                                                           \
    free(array);                                                                                   \
    return ares;                                                                                   \
}
// NOLINTEND(bugprone-macro-parentheses)
// clang-format on

MAKE_FUNCTION_DESERIALIZE_ARRAY(double, double, ASTARTE_MAPPING_TYPE_DOUBLEARRAY, double_array)
MAKE_FUNCTION_DESERIALIZE_ARRAY(bool, bool, ASTARTE_MAPPING_TYPE_BOOLEANARRAY, boolean_array)
MAKE_FUNCTION_DESERIALIZE_ARRAY(
    datetime, int64_t, ASTARTE_MAPPING_TYPE_DATETIMEARRAY, datetime_array)
MAKE_FUNCTION_DESERIALIZE_ARRAY(int32, int32_t, ASTARTE_MAPPING_TYPE_INTEGERARRAY, integer_array)

static astarte_result_t deserialize_array_int64(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;
    int64_t *array = calloc(array_length, sizeof(int64_t));
    if (!array) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }

    astarte_bson_element_t inner_elem = { 0 };
    ares = astarte_bson_deserializer_first_element(bson_doc, &inner_elem);
    if (ares != ASTARTE_RESULT_OK) {
        goto failure;
    }

    if (inner_elem.type == ASTARTE_BSON_TYPE_INT32) {
        array[0] = (int64_t) astarte_bson_deserializer_element_to_int32(inner_elem);
    } else {
        array[0] = astarte_bson_deserializer_element_to_int64(inner_elem);
    }

    for (size_t i = 1; i < array_length; i++) {
        ares = astarte_bson_deserializer_next_element(bson_doc, inner_elem, &inner_elem);
        if (ares != ASTARTE_RESULT_OK) {
            goto failure;
        }

        if (inner_elem.type == ASTARTE_BSON_TYPE_INT32) {
            array[i] = (int64_t) astarte_bson_deserializer_element_to_int32(inner_elem);
        } else {
            array[i] = astarte_bson_deserializer_element_to_int64(inner_elem);
        }
    }

    data->tag = ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY;
    data->data.longinteger_array.len = array_length;
    data->data.longinteger_array.buf = array;
    return ASTARTE_RESULT_OK;

failure:
    free(array);
    return ares;
}

static astarte_result_t deserialize_array_string(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;
    // Step 1: allocate enough memory to contain the array from the BSON file
    char **array = (char **) calloc(array_length, sizeof(char *));
    if (!array) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }

    // Step 2: fill in the array with data
    astarte_bson_element_t inner_elem = { 0 };
    ares = astarte_bson_deserializer_first_element(bson_doc, &inner_elem);
    if (ares != ASTARTE_RESULT_OK) {
        goto failure;
    }

    uint32_t deser_len = 0;
    const char *deser = astarte_bson_deserializer_element_to_string(inner_elem, &deser_len);
    array[0] = calloc(deser_len + 1, sizeof(char));
    if (!array[0]) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }
    strncpy(array[0], deser, deser_len);

    for (size_t i = 1; i < array_length; i++) {
        ares = astarte_bson_deserializer_next_element(bson_doc, inner_elem, &inner_elem);
        if (ares != ASTARTE_RESULT_OK) {
            goto failure;
        }

        deser_len = 0;
        const char *deser = astarte_bson_deserializer_element_to_string(inner_elem, &deser_len);
        array[i] = calloc(deser_len + 1, sizeof(char));
        if (!array[i]) {
            ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
            ares = ASTARTE_RESULT_OUT_OF_MEMORY;
            goto failure;
        }
        strncpy(array[i], deser, deser_len);
    }

    // Step 3: Place the generated array in the output struct
    data->tag = ASTARTE_MAPPING_TYPE_STRINGARRAY;
    data->data.string_array.len = array_length;
    data->data.string_array.buf = (const char **) array;

    return ASTARTE_RESULT_OK;

failure:
    if (array) {
        for (size_t i = 0; i < array_length; i++) {
            free(array[i]);
        }
    }
    free((void *) array);
    return ares;
}

static astarte_result_t deserialize_array_binblob(
    astarte_bson_document_t bson_doc, astarte_data_t *data, size_t array_length)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;
    uint8_t **array = NULL;
    size_t *array_sizes = NULL;
    // Step 1: allocate enough memory to contain the array from the BSON file
    array = (uint8_t **) calloc(array_length, sizeof(uint8_t *));
    if (!array) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }
    array_sizes = calloc(array_length, sizeof(size_t));
    if (!array_sizes) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }

    // Step 2: fill in the array with data
    astarte_bson_element_t inner_elem = { 0 };
    ares = astarte_bson_deserializer_first_element(bson_doc, &inner_elem);
    if (ares != ASTARTE_RESULT_OK) {
        goto failure;
    }
    uint32_t deser_size = 0;
    const uint8_t *deser = astarte_bson_deserializer_element_to_binary(inner_elem, &deser_size);
    array[0] = calloc(deser_size, sizeof(uint8_t));
    if (!array[0]) {
        ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
        ares = ASTARTE_RESULT_OUT_OF_MEMORY;
        goto failure;
    }
    memcpy(array[0], deser, deser_size);
    array_sizes[0] = deser_size;

    for (size_t i = 1; i < array_length; i++) {
        ares = astarte_bson_deserializer_next_element(bson_doc, inner_elem, &inner_elem);
        if (ares != ASTARTE_RESULT_OK) {
            goto failure;
        }

        deser_size = 0;
        const uint8_t *deser = astarte_bson_deserializer_element_to_binary(inner_elem, &deser_size);
        array[i] = calloc(deser_size, sizeof(uint8_t));
        if (!array[i]) {
            ASTARTE_LOG_ERR("Out of memory %s: %d", __FILE__, __LINE__);
            ares = ASTARTE_RESULT_OUT_OF_MEMORY;
            goto failure;
        }
        memcpy(array[i], deser, deser_size);
        array_sizes[i] = deser_size;
    }

    // Step 3: Place the generated array in the output struct
    data->tag = ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY;
    data->data.binaryblob_array.count = array_length;
    data->data.binaryblob_array.sizes = array_sizes;
    data->data.binaryblob_array.blobs = (const void **) array;

    return ASTARTE_RESULT_OK;

failure:
    if (array) {
        for (size_t i = 0; i < array_length; i++) {
            free(array[i]);
        }
    }
    free((void *) array);
    free(array_sizes);
    return ares;
}

static bool check_if_bson_type_is_mapping_type(
    astarte_mapping_type_t mapping_type, uint8_t bson_type)
{
    uint8_t expected_bson_type = '\x00';
    switch (mapping_type) {
        case ASTARTE_MAPPING_TYPE_BINARYBLOB:
            expected_bson_type = ASTARTE_BSON_TYPE_BINARY;
            break;
        case ASTARTE_MAPPING_TYPE_BOOLEAN:
            expected_bson_type = ASTARTE_BSON_TYPE_BOOLEAN;
            break;
        case ASTARTE_MAPPING_TYPE_DATETIME:
            expected_bson_type = ASTARTE_BSON_TYPE_DATETIME;
            break;
        case ASTARTE_MAPPING_TYPE_DOUBLE:
            expected_bson_type = ASTARTE_BSON_TYPE_DOUBLE;
            break;
        case ASTARTE_MAPPING_TYPE_INTEGER:
            expected_bson_type = ASTARTE_BSON_TYPE_INT32;
            break;
        case ASTARTE_MAPPING_TYPE_LONGINTEGER:
            expected_bson_type = ASTARTE_BSON_TYPE_INT64;
            break;
        case ASTARTE_MAPPING_TYPE_STRING:
            expected_bson_type = ASTARTE_BSON_TYPE_STRING;
            break;
        case ASTARTE_MAPPING_TYPE_BINARYBLOBARRAY:
        case ASTARTE_MAPPING_TYPE_BOOLEANARRAY:
        case ASTARTE_MAPPING_TYPE_DATETIMEARRAY:
        case ASTARTE_MAPPING_TYPE_DOUBLEARRAY:
        case ASTARTE_MAPPING_TYPE_INTEGERARRAY:
        case ASTARTE_MAPPING_TYPE_LONGINTEGERARRAY:
        case ASTARTE_MAPPING_TYPE_STRINGARRAY:
            expected_bson_type = ASTARTE_BSON_TYPE_ARRAY;
            break;
        default:
            ASTARTE_LOG_ERR("Invalid mapping type (%d).", mapping_type);
            return false;
    }

    if ((expected_bson_type == ASTARTE_BSON_TYPE_INT64) && (bson_type == ASTARTE_BSON_TYPE_INT32)) {
        return true;
    }

    if (bson_type != expected_bson_type) {
        ASTARTE_LOG_ERR(
            "Mapping type (%d) and BSON type (0x%x) do not match.", mapping_type, bson_type);
        return false;
    }

    return true;
}
