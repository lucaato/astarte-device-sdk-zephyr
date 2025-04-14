# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

from cfgvalues import CfgValues

import pytest
from twister_harness import DeviceAdapter, Shell
from west import log


class TestcaseHelper:
    def __init__(self, astarte_cfg: CfgValues, dut: DeviceAdapter, shell: Shell):
        self.astarte_cfg = astarte_cfg
        self.dut = dut
        self.shell = shell

    # The shell.exec_command function has some quirks that i can't be bothered to work out right now
    # - one is that the length of the shell in the CONFIG_SHELL_DEFAULT_TERMINAL_WIDTH changes the way the command gets printed out
    #   so i do not think the regex can match the command so i've set CONFIG_SHELL_DEFAULT_TERMINAL_WIDTH to a higher value
    # - the other is that the regex still timeouts but this need more investigating
    # this function does not perform the same actions as shell.exec_command it just write a commands and wait for an available prompt
    def exec_command(self, command: str, timeout: float | None = None):
        self.dut.clear_buffer()
        log.inf(f"Executing command on device shell >>> {command}")
        self.dut.write(f"{command}\n\n".encode())
        self.shell.wait_for_prompt(timeout=timeout)


@pytest.fixture(scope="session")
def astarte_cfg(dut: DeviceAdapter):
    # Load kconfig configured settings from the build directory
    CONFIG_FILE = dut.device_config.build_dir.joinpath("zephyr", ".config")

    return CfgValues(CONFIG_FILE)


@pytest.fixture(scope="session")
def testcase_helper(astarte_cfg: CfgValues, dut: DeviceAdapter, shell: Shell):
    return TestcaseHelper(astarte_cfg, dut, shell)
