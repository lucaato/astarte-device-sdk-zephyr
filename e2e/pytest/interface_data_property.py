# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from west import log

from conftest import TestcaseHelper
from interface_data import InterfaceData, Ownership
from http_requests import (
    prepare_transmit_data,
    post_server_data,
    decode_value,
    get_server_data,
    unset_server_property,
)
from test_utilities import compare_data, encode_shell_bson


class Property:
    def __init__(self, path: str, value: Any):
        self.path = path
        self.value = value


class PropertySet(InterfaceData[Property]):
    def __init__(self, interface: str, ownership: Ownership, properties: list[Property]):
        super().__init__(interface, ownership)
        self.properties = properties

    def _send_server_data(self, helper: TestcaseHelper, data: Property):
        value = prepare_transmit_data(data.path.split("/")[-1], data.value)
        post_server_data(helper.astarte_cfg, self.interface, data.path, value)

    def _check_server_received_data(self, helper: TestcaseHelper, data: Property):
        received_data = get_server_data(helper.astarte_cfg, self.interface)
        log.inf(f"Server property data {received_data}")

        # Retrieve and check properties
        try:
            parameter = data.path.split("/")[1]
            last_path_segment = data.path.split("/")[-1]
            expected_value = data.value
            got_value = decode_value(received_data[parameter][last_path_segment], last_path_segment)
        except KeyError as e:
            log.err(f"KeyError while accessing the property {e}")
            return False

        log.inf(f"Endpoint {data.path}: expected {expected_value} got {got_value}")
        return compare_data(got_value, expected_value)

    def _get_device_shell_commands(self, base_command: str, data: Property) -> str:
        path = data.path
        bson_base64 = encode_shell_bson(data.value)
        return f"{base_command} property set {self.interface} {path} {bson_base64}"

    def _get_single_send_elements(self) -> list[Property]:
        return self.properties


class PropertyUnset(InterfaceData[str]):
    def __init__(self, interface: str, ownership: Ownership, unset: list[str]):
        super().__init__(interface, ownership)
        self.unset = unset

    def _send_server_data(self, helper: TestcaseHelper, data: str):
        unset_server_property(helper.astarte_cfg, self.interface, data)

    def _check_server_received_data(self, helper: TestcaseHelper, data: str) -> bool:
        received_data = get_server_data(helper.astarte_cfg, self.interface)
        log.inf(f"Server property data {received_data}")

        paramter = data.split("/")[1]
        endpoint = data.split("/")[-1]
        try:
            parameter_object = received_data[paramter][endpoint]
            got_value = decode_value(parameter_object, endpoint)
        except KeyError as e:
            # handled because completety unset interfaces do not return anything
            log.inf(f"The key is not accessible as expected: {e}")
            got_value = {}

        expected_value = {}
        log.inf(f"Endpoint {data}: expected {expected_value} got {got_value}")
        return got_value == expected_value

    def _get_device_shell_commands(self, base_command: str, data: str) -> str:
        return f"{base_command} property unset {self.interface} {data}"

    def _get_single_send_elements(self) -> list[str]:
        return self.unset
