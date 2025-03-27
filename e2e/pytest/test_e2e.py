# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

from west import log

from conftest import TestcaseHelper
from data import data


def test_device(testcase_helper: TestcaseHelper):
    log.inf("Launching the device")

    testcase_helper.dut.launch()
    testcase_helper.dut.readlines_until(regex="Device shell ready$", timeout=15)

    for interface_data in data:
        interface_data.test(testcase_helper)

    testcase_helper.shell.exec_command("disconnect")
    # FIXME the disconnect still does not work i don't know why it seems to be working when i run the binary manually
