# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from datetime import datetime, timezone
from enum import Enum
import time

from west import log
from conftest import TestcaseHelper


class Ownership(Enum):
    DEVICE = 1
    SERVER = 2

T = TypeVar("T", covariant=False)

class InterfaceData(ABC, Generic[T]):
    """
    Base interface data class, defines a generic test method that handles
    sending and receiving data for both the server and the client.
    Implementors should redefine the "private" methods
    """

    def __init__(self, interface: str, ownership: Ownership):
        self.interface = interface
        self.ownership = ownership

    @abstractmethod
    def _send_server_data(self, helper: TestcaseHelper, data: T):
        """
        Abstract method that handles sending the data passed from the server to the device
        """

    @abstractmethod
    def _check_server_received_data(self, helper: TestcaseHelper, send_start: datetime, data: T):
        """
        Abstract method that checks the data received by the server.
        This is the data that was sent the device using a "send" shell command
        """

    @abstractmethod
    def _get_device_shell_commands(self, base_command: str, data: T) -> str:
        """
        Gets the command that encodes the send/set or unset of this interface and the passed data.
        This comes in the form of '{base_command} <interface_name> <path> <bson_base64_data> <timestamp>'
        """

    @abstractmethod
    def _get_single_send_elements(self) -> list[T]:
       """
       Get a list of element each corresponding to a single astarte send/set or unset command.
       """

    def test(self, helper: TestcaseHelper):
        """
        Test reception and transmission of this interface
        """

        EXPECT_VERIFY_COMMAND = "expect verify"
        SEND_BASE_COMMAND = "send"
        EXPECT_BASE_COMMAND = "expect"

        for send in self._get_single_send_elements():
            if self.ownership == Ownership.SERVER:
                helper.exec_command(self._get_device_shell_commands(EXPECT_BASE_COMMAND, send))
                time.sleep(2)
                self._send_server_data(helper, send)
                # TODO Could add a command that waits for all message to be sent ?
                time.sleep(2)
            else:
                send_start = datetime.now(tz=timezone.utc)
                helper.exec_command(self._get_device_shell_commands(SEND_BASE_COMMAND, send))
                time.sleep(2)

                # retry two times
                for i in range(0, 2):
                    try:
                        self._check_server_received_data(helper, send_start, send)
                    except (KeyError, ValueError) as e:
                        log.inf(f"Missing key in server data {e}, retrying...")

                    time.sleep(2)

                self._check_server_received_data(helper, send_start, send)
