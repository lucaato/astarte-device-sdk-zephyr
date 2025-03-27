# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
import time

from west import log

from conftest import TestcaseHelper


class Ownership(Enum):
    DEVICE = 1
    SERVER = 2


class InterfaceData(ABC):
    """
    Base interface data class, defines a generic test method that handles
    sending and receiving data for both the server and the client.
    Implementors should redefine the "private" methods
    """

    def __init__(self, interface: str, ownership: Ownership):
        self.interface = interface
        self.ownership = ownership

    @abstractmethod
    def _send_server_data(self, helper: TestcaseHelper):
        """
        Abstract method that handles sending the data from the server to the device
        """

    @abstractmethod
    def _check_server_received_data(self, helper: TestcaseHelper, send_start: datetime):
        """
        Abstract method that checks the data received by the server.
        This is the data that was sent the device using a "send" shell command
        """

    @abstractmethod
    def _get_device_shell_commands(self, base_command: str) -> list[str]:
        """
        Gets the command that encodes all the data about this interface.
        This comes in the form of '{base_command} <interface_name> <path> <bson_base64_data> <timestamp>'
        """

    def test(self, helper: TestcaseHelper):
        """
        Test reception and transmission of this interface
        """

        EXPECT_VERIFY_COMMAND = "expect verify"
        SEND_BASE_COMMAND = "send"
        EXPECT_BASE_COMMAND = "expect"

        if self.ownership == Ownership.SERVER:
            helper.exec_commands(self._get_device_shell_commands(EXPECT_BASE_COMMAND))
            time.sleep(2)
            # TODO should also test that the command gets executed and chec the presense of the confirmation string like "Property set"
            self._send_server_data(helper)
            # TODO Could add a command that waits for all message to be sent ?
            time.sleep(2)
            helper.exec_command(EXPECT_VERIFY_COMMAND)
            helper.dut.readlines_until(regex="All expected data received$", timeout=10)
        else:
            send_start = datetime.now(tz=timezone.utc)
            helper.exec_commands(self._get_device_shell_commands(SEND_BASE_COMMAND))
            time.sleep(2)

            # retry two times
            for i in range(0, 2):
                try:
                    self._check_server_received_data(helper, send_start)
                except (KeyError, ValueError) as e:
                    log.inf(f"Missing key in server data {e}, retrying...")

                time.sleep(2)

            self._check_server_received_data(helper, send_start)
