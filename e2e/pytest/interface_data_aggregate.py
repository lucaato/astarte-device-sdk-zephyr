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


class Aggregate(InterfaceData[dict[str, Any]]):
    def __init__(
        self,
        interface: str,
        ownership: Ownership,
        path: str,
        entries: dict[str, Any],
        timestamp: datetime | None = None,
    ):
        super().__init__(interface, ownership)
        self.path = path
        self.entries = entries
        self.timestamp = timestamp

    def _send_server_data(self, helper: TestcaseHelper, data: dict[str, Any]):
        data = {}
        for key, value in data.items():
            data[key] = prepare_transmit_data(key, value)
        post_server_data(helper.astarte_cfg, self.interface, self.path, data)

    def _check_server_received_data(self, helper: TestcaseHelper, data: dict[str, Any]) -> bool:
        received_data = get_server_data(helper.astarte_cfg, self.interface, limit=1)

        log.inf(f"Server aggregate data {received_data}")

        try:
            for key, expected_value in data.items():
                parameter = self.path.split("/")[1]
                got_value = decode_value(received_data[parameter][0][key], key)

                log.inf(f"Endpoint {key}: expected {expected_value} got {got_value}")
                if not compare_data(got_value, expected_value):
                    return False
        except KeyError as e:
            log.err(f"KeyError while accessing the object {e}")
            return False

        return True

    def _get_device_shell_commands(self, base_command: str, data: dict[str, Any]) -> str:
        path = self.path
        bson_base64 = encode_shell_bson(data)

        if self.timestamp:
            unix_t = int(self.timestamp.timestamp() * 1000)
            command = f"{base_command} object {self.interface} {path} {bson_base64} {unix_t}"
        else:
            command = f"{base_command} object {self.interface} {path} {bson_base64}"

        return command

    def _get_single_send_elements(self) -> list[dict[str, Any]]:
        return [self.entries]
