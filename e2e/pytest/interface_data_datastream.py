# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Any

from west import log

from conftest import TestcaseHelper
from interface_data import InterfaceData, Ownership
from http_requests import prepare_transmit_data, post_server_data, decode_value, get_server_data
from test_utilities import compare_data, encode_shell_bson


class DatastreamMapping:
    def __init__(self, path: str, value: Any, timestamp: datetime | None = None):
        self.path = path
        self.timestamp = timestamp
        self.value = value


class Datastream(InterfaceData[DatastreamMapping]):
    def __init__(self, interface: str, ownership: Ownership, mappings: list[DatastreamMapping]):
        super().__init__(interface, ownership)
        self.mappings = mappings

    def _send_server_data(self, helper: TestcaseHelper, data: DatastreamMapping):
        path = data.path
        value = prepare_transmit_data(path.split("/")[-1], data.value)
        post_server_data(helper.astarte_cfg, self.interface, path, value)

    def _check_server_received_data(self, helper: TestcaseHelper, data: DatastreamMapping) -> bool:
        received_data = get_server_data(helper.astarte_cfg, self.interface, limit=1)
        log.inf(f"Server individual data {received_data}")

        last_path_segment = data.path.split("/")[-1]
        expected_value = data.value

        try:
            got_value = decode_value(received_data[last_path_segment]["value"], last_path_segment)
        except KeyError as e:
            log.err(f"KeyError while accessing the individual {e}")
            return False

        endpoint = self.interface + data.path
        log.inf(f"Endpoint {endpoint}: expected {expected_value} got {got_value}")

        return compare_data(got_value, expected_value)

    def _get_device_shell_commands(self, base_command: str, data: DatastreamMapping) -> str:
        command = ""

        path = data.path
        bson_base64 = encode_shell_bson(data.value)

        if data.timestamp:
            unix_t = int(data.timestamp.timestamp() * 1000)
            command = f"{base_command} individual {self.interface} {path} {bson_base64} {unix_t}"
        else:
            command = f"{base_command} individual {self.interface} {path} {bson_base64}"

        return command

    def _get_single_send_elements(self) -> list[DatastreamMapping]:
        return self.mappings
