/*
 * (C) Copyright 2024, SECO Mind Srl
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "uuid.h"

#include <ctype.h>
#include <stdio.h>

#include <zephyr/posix/arpa/inet.h>
#include <zephyr/random/random.h>
#include <zephyr/sys/base64.h>

#include <mbedtls/md.h>

#include "log.h"

ASTARTE_LOG_MODULE_REGISTER(astarte_uuid, CONFIG_ASTARTE_DEVICE_SDK_UUID_LOG_LEVEL);

// All the macros below follow the standard for the Universally Unique Identifier as defined
// by the IETF in the RFC 4122.
// The full specification can be found here: https://datatracker.ietf.org/doc/html/rfc4122

// Lengths of each component of the UUID in bytes
#define UUID_LEN_TIME_LOW (4)
#define UUID_LEN_TIME_MID (2)
#define UUID_LEN_TIME_HIGH_AND_VERSION (2)
#define UUID_LEN_CLOCK_SEQ_AND_RESERVED (1)
#define UUID_LEN_CLOCK_SEQ_LOW (1)
#define UUID_LEN_NODE (6)

// Offset of each component of the UUID in bytes
#define UUID_OFFSET_TIME_LOW (0)
#define UUID_OFFSET_TIME_MID (4)
#define UUID_OFFSET_TIME_HIGH_AND_VERSION (6)
#define UUID_OFFSET_CLOCK_SEQ_AND_RESERVED (8)
#define UUID_OFFSET_CLOCK_SEQ_LOW (9)
#define UUID_OFFSET_NODE (10)

// Offset of each component of the UUID in string format
#define UUID_STR_OFFSET_TIME_LOW (0)
#define UUID_STR_OFFSET_TIME_MID (9)
#define UUID_STR_OFFSET_TIME_HIGH_AND_VERSION (14)
#define UUID_STR_OFFSET_CLOCK_SEQ_AND_RESERVED (19)
#define UUID_STR_OFFSET_CLOCK_SEQ_LOW (21)
#define UUID_STR_OFFSET_NODE (24)

// Position of the hyphens
#define UUID_STR_POSITION_FIRST_HYPHEN (UUID_STR_OFFSET_TIME_MID - 1)
#define UUID_STR_POSITION_SECOND_HYPHEN (UUID_STR_OFFSET_TIME_HIGH_AND_VERSION - 1)
#define UUID_STR_POSITION_THIRD_HYPHEN (UUID_STR_OFFSET_CLOCK_SEQ_AND_RESERVED - 1)
#define UUID_STR_POSITION_FOURTH_HYPHEN (UUID_STR_OFFSET_NODE - 1)

// Internal element's masks and offsets
#define UUID_TIME_HI_AND_VERSION_MASK_TIME (0x0FFFU)

#define UUID_TIME_HI_AND_VERSION_OFFSET_VERSION (12U)

#define UUID_CLOCK_SEQ_AND_RESERVED_MASK_1 (0x3FU)
#define UUID_CLOCK_SEQ_AND_RESERVED_MASK_2 (0x80U)

/** @brief Helper struct to make it easier to access UUID fields. */
struct uuid
{
    /** @brief The low field of the timestamp. */
    uint32_t time_low;
    /** @brief The middle field of the timestamp. */
    uint16_t time_mid;
    /** @brief The high field of the timestamp multiplexed with the version number. */
    uint16_t time_hi_and_version;
    /** @brief The high field of the clock sequence multiplexed with the variant. */
    uint8_t clock_seq_hi_res;
    /** @brief The low field of the clock sequence. */
    uint8_t clock_seq_low;
    /** @brief The spatially unique node identifier. */
    uint8_t node[UUID_LEN_NODE];
};

static void uuid_from_struct(const struct uuid *input, uuid_t out)
{
    uint32_t tmp32 = 0U;
    uint16_t tmp16 = 0U;
    uint8_t *out_p = out;

    tmp32 = htonl(input->time_low); // NOLINT(hicpp-signed-bitwise)
    memcpy(out_p + UUID_OFFSET_TIME_LOW, &tmp32, UUID_LEN_TIME_LOW);

    tmp16 = htons(input->time_mid); // NOLINT(hicpp-signed-bitwise)
    memcpy(out_p + UUID_OFFSET_TIME_MID, &tmp16, UUID_LEN_TIME_MID);

    tmp16 = htons(input->time_hi_and_version); // NOLINT(hicpp-signed-bitwise)
    memcpy(out_p + UUID_OFFSET_TIME_HIGH_AND_VERSION, &tmp16, UUID_LEN_TIME_HIGH_AND_VERSION);

    out_p[UUID_OFFSET_CLOCK_SEQ_AND_RESERVED] = input->clock_seq_hi_res;
    out_p[UUID_OFFSET_CLOCK_SEQ_LOW] = input->clock_seq_low;

    memcpy(out_p + UUID_OFFSET_NODE, input->node, UUID_LEN_NODE);
}

static void uuid_to_struct(const uuid_t input, struct uuid *out)
{
    uint32_t tmp32 = 0U;
    uint16_t tmp16 = 0U;
    const uint8_t *in_p = input;

    memcpy(&tmp32, in_p + UUID_OFFSET_TIME_LOW, UUID_LEN_TIME_LOW);
    out->time_low = ntohl(tmp32); // NOLINT(hicpp-signed-bitwise)

    memcpy(&tmp16, in_p + UUID_OFFSET_TIME_MID, UUID_LEN_TIME_MID);
    out->time_mid = ntohs(tmp16); // NOLINT(hicpp-signed-bitwise)

    memcpy(&tmp16, in_p + UUID_OFFSET_TIME_HIGH_AND_VERSION, UUID_LEN_TIME_HIGH_AND_VERSION);
    out->time_hi_and_version = ntohs(tmp16); // NOLINT(hicpp-signed-bitwise)

    out->clock_seq_hi_res = in_p[UUID_OFFSET_CLOCK_SEQ_AND_RESERVED];
    out->clock_seq_low = in_p[UUID_OFFSET_CLOCK_SEQ_LOW];

    memcpy(out->node, in_p + UUID_OFFSET_NODE, UUID_LEN_NODE);
}

void uuid_generate_v4(uuid_t out)
{
    uint8_t random_result[UUID_SIZE];
    sys_rand_get(random_result, UUID_SIZE);

    struct uuid random_uuid_struct = { 0 };
    uuid_to_struct(random_result, &random_uuid_struct);

    const unsigned int version = 4U;
    random_uuid_struct.time_hi_and_version &= UUID_TIME_HI_AND_VERSION_MASK_TIME;
    random_uuid_struct.time_hi_and_version |= (version << UUID_TIME_HI_AND_VERSION_OFFSET_VERSION);

    uuid_from_struct(&random_uuid_struct, out);
}

astarte_result_t uuid_generate_v5(
    const uuid_t namespace, const void *data, size_t data_size, uuid_t out)
{
    const size_t sha_256_bytes = 32;
    uint8_t sha_result[sha_256_bytes];

    mbedtls_md_context_t ctx = { 0 };
    const mbedtls_md_info_t *md_info = mbedtls_md_info_from_type(MBEDTLS_MD_SHA1);

    mbedtls_md_init(&ctx);
    int mbedtls_err = mbedtls_md_setup(&ctx, md_info, 0);
    // NOLINTBEGIN(hicpp-signed-bitwise) Only using the mbedtls_err to check if zero
    mbedtls_err |= mbedtls_md_starts(&ctx);
    mbedtls_err |= mbedtls_md_update(&ctx, namespace, UUID_SIZE);
    mbedtls_err |= mbedtls_md_update(&ctx, data, data_size);
    mbedtls_err |= mbedtls_md_finish(&ctx, sha_result);
    // NOLINTEND(hicpp-signed-bitwise)
    mbedtls_md_free(&ctx);
    if (mbedtls_err != 0) {
        ASTARTE_LOG_ERR("UUID V5 generation failed.");
        return ASTARTE_RESULT_INTERNAL_ERROR;
    }

    struct uuid sha_uuid_struct = { 0 };
    // This will use the first 16 bytes of the SHA-256
    uuid_to_struct(sha_result, &sha_uuid_struct);

    const unsigned int version = 5U;
    sha_uuid_struct.time_hi_and_version &= UUID_TIME_HI_AND_VERSION_MASK_TIME;
    sha_uuid_struct.time_hi_and_version |= (version << UUID_TIME_HI_AND_VERSION_OFFSET_VERSION);
    sha_uuid_struct.clock_seq_hi_res &= UUID_CLOCK_SEQ_AND_RESERVED_MASK_1;
    sha_uuid_struct.clock_seq_hi_res |= UUID_CLOCK_SEQ_AND_RESERVED_MASK_2;

    uuid_from_struct(&sha_uuid_struct, out);

    return ASTARTE_RESULT_OK;
}

astarte_result_t uuid_generate_v5_to_base64url(
    const uuid_t namespace, const void *data, size_t data_size, char *out, size_t out_size)
{
    astarte_result_t ares = ASTARTE_RESULT_OK;
    uuid_t uuid = { 0 };

    ares = uuid_generate_v5(namespace, data, data_size, uuid);
    if (ares != ASTARTE_RESULT_OK) {
        ASTARTE_LOG_ERR("UUID V5 generation failed: %s", astarte_result_to_name(ares));
        return ares;
    }

    return uuid_to_base64url(uuid, out, out_size);
}

astarte_result_t uuid_from_string(const char *input, uuid_t out)
{
    // Length check
    if (strlen(input) != UUID_STR_LEN) {
        return ASTARTE_RESULT_INTERNAL_ERROR;
    }

    // Sanity check
    for (int i = 0; i < UUID_STR_LEN; i++) {
        char char_i = input[i];
        // Check that hyphens are in the right place
        if ((i == UUID_STR_POSITION_FIRST_HYPHEN) || (i == UUID_STR_POSITION_SECOND_HYPHEN)
            || (i == UUID_STR_POSITION_THIRD_HYPHEN) || (i == UUID_STR_POSITION_FOURTH_HYPHEN)) {
            if (char_i != '-') {
                ASTARTE_LOG_WRN("Found invalid character %c in hyphen position %d", char_i, i);
                return ASTARTE_RESULT_INTERNAL_ERROR;
            }
            continue;
        }

        // checking is the given input is not hexadecimal
        if (!isxdigit(char_i)) {
            ASTARTE_LOG_WRN("Found invalid character %c in position %d", char_i, i);
            return ASTARTE_RESULT_INTERNAL_ERROR;
        }
    }

    // Will be used to contain the string representation of a uint16_t (plus the NULL terminator)
    char tmp[sizeof(uint16_t) + sizeof(uint8_t)] = { 0 };
    struct uuid uuid_struct = { 0 };
    const int strtoul_base = 16;

    uuid_struct.time_low = strtoul(input + UUID_STR_OFFSET_TIME_LOW, NULL, strtoul_base);
    uuid_struct.time_mid = strtoul(input + UUID_STR_OFFSET_TIME_MID, NULL, strtoul_base);
    uuid_struct.time_hi_and_version
        = strtoul(input + UUID_STR_OFFSET_TIME_HIGH_AND_VERSION, NULL, strtoul_base);

    tmp[0] = input[UUID_STR_OFFSET_CLOCK_SEQ_AND_RESERVED];
    tmp[1] = input[UUID_STR_OFFSET_CLOCK_SEQ_AND_RESERVED + 1];
    uuid_struct.clock_seq_hi_res = strtoul(tmp, NULL, strtoul_base);

    tmp[0] = input[UUID_STR_OFFSET_CLOCK_SEQ_LOW];
    tmp[1] = input[UUID_STR_OFFSET_CLOCK_SEQ_LOW + 1];
    uuid_struct.clock_seq_low = strtoul(tmp, NULL, strtoul_base);

    for (int i = 0; i < UUID_LEN_NODE; i++) {
        tmp[0] = input[UUID_STR_OFFSET_NODE + i * sizeof(uint16_t)];
        tmp[1] = input[UUID_STR_OFFSET_NODE + i * sizeof(uint16_t) + 1];
        uuid_struct.node[i] = strtoul(tmp, NULL, strtoul_base);
    }

    uuid_from_struct(&uuid_struct, out);
    return ASTARTE_RESULT_OK;
}

astarte_result_t uuid_to_string(const uuid_t uuid, char *out, size_t out_size)
{
    size_t min_out_size = UUID_STR_LEN + 1;
    if (out_size < min_out_size) {
        ASTARTE_LOG_ERR("Output buffer should be at least %zu bytes long", min_out_size);
        return ASTARTE_RESULT_INVALID_PARAM;
    }

    struct uuid uuid_struct = { 0 };

    uuid_to_struct(uuid, &uuid_struct);

    // NOLINTBEGIN(readability-magic-numbers, cppcoreguidelines-avoid-magic-numbers)
    int res = snprintf(out, min_out_size,
        "%08" PRIx32 "-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x", uuid_struct.time_low,
        uuid_struct.time_mid, uuid_struct.time_hi_and_version, uuid_struct.clock_seq_hi_res,
        uuid_struct.clock_seq_low, uuid_struct.node[0], uuid_struct.node[1], uuid_struct.node[2],
        uuid_struct.node[3], uuid_struct.node[4], uuid_struct.node[5]);
    // NOLINTEND(readability-magic-numbers, cppcoreguidelines-avoid-magic-numbers)
    if ((res < 0) || (res >= min_out_size)) {
        ASTARTE_LOG_ERR("Error converting UUID to string.");
        return ASTARTE_RESULT_INTERNAL_ERROR;
    }
    return ASTARTE_RESULT_OK;
}

astarte_result_t uuid_to_base64(const uuid_t uuid, char *out, size_t out_size)
{
    size_t min_out_size = UUID_BASE64_LEN + 1;
    if (out_size < min_out_size) {
        ASTARTE_LOG_ERR("Output buffer should be at least %zu bytes long", min_out_size);
        return ASTARTE_RESULT_INVALID_PARAM;
    }

    size_t olen = 0;
    int res = base64_encode(out, out_size, &olen, uuid, UUID_SIZE);
    if (res != 0) {
        ASTARTE_LOG_ERR("Error converting UUID to base 64 string, rc: %d.", res);
        return ASTARTE_RESULT_INTERNAL_ERROR;
    }

    return ASTARTE_RESULT_OK;
}

astarte_result_t uuid_to_base64url(const uuid_t uuid, char *out, size_t out_size)
{
    size_t min_out_size = UUID_BASE64URL_LEN + 1;
    if (out_size < min_out_size) {
        ASTARTE_LOG_ERR("Output buffer should be at least %zu bytes long", min_out_size);
        return ASTARTE_RESULT_INVALID_PARAM;
    }

    // Convert UUID to RFC 3548/4648 base 64 notation
    size_t olen = 0;
    char uuid_base64[UUID_BASE64_LEN + 1] = { 0 };
    int res = base64_encode(uuid_base64, UUID_BASE64_LEN + 1, &olen, uuid, UUID_SIZE);
    if (res != 0) {
        ASTARTE_LOG_ERR("Error converting UUID to base 64 string, rc: %d.", res);
        return ASTARTE_RESULT_INTERNAL_ERROR;
    }

    // Convert UUID to RFC 4648 sec. 5 URL and filename safe base 64 notation
    for (size_t i = 0; i < UUID_BASE64URL_LEN; i++) {
        if (uuid_base64[i] == '+') {
            uuid_base64[i] = '-';
        }
        if (uuid_base64[i] == '/') {
            uuid_base64[i] = '_';
        }
    }

    memcpy(out, uuid_base64, UUID_BASE64URL_LEN);
    out[UUID_BASE64URL_LEN] = 0;

    return ASTARTE_RESULT_OK;
}
