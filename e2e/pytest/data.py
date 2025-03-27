# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from interface_data import InterfaceData, Ownership
from interface_data_aggregate import Aggregate
from interface_data_datastream import Datastream, DatastreamMapping
from interface_data_property import PropertySet, PropertyUnset, Property

data: list[InterfaceData] = [
    PropertySet(
        interface="org.astarte-platform.zephyr.e2etest.DeviceProperty",
        ownership=Ownership.DEVICE,
        properties=[
            Property(path="/sensor36/binaryblob_endpoint", value=b"SGVsbG8="),
            Property(path="/sensor36/binaryblobarray_endpoint", value=[b"SGVsbG8=", b"dDk5Yg=="]),
            Property(path="/sensor36/boolean_endpoint", value=True),
            Property(path="/sensor36/booleanarray_endpoint", value=[True, False, True]),
            Property(
                path="/sensor36/datetime_endpoint",
                value=datetime.fromtimestamp(1710940988, tz=timezone.utc),
            ),
            Property(
                path="/sensor36/datetimearray_endpoint",
                value=[
                    datetime.fromtimestamp(1710940988, tz=timezone.utc),
                    datetime.fromtimestamp(17109409814, tz=timezone.utc),
                ],
            ),
            Property(path="/sensor36/double_endpoint", value=15.42),
            Property(path="/sensor36/doublearray_endpoint", value=[1542.25, 88852.6]),
            Property(path="/sensor36/integer_endpoint", value=42),
            Property(path="/sensor36/integerarray_endpoint", value=[4525, 0, 11]),
            Property(path="/sensor36/longinteger_endpoint", value=8589934592),
            Property(
                path="/sensor36/longintegerarray_endpoint", value=[8589930067, 42, 8589934592]
            ),
            Property(path="/sensor36/string_endpoint", value="Hello world!"),
            Property(path="/sensor36/stringarray_endpoint", value=["Hello ", "world!"]),
        ],
    ),
    PropertySet(
        interface="org.astarte-platform.zephyr.e2etest.ServerProperty",
        ownership=Ownership.SERVER,
        properties=[
            Property(path="/path84/binaryblob_endpoint", value=b"SGVsbG8="),
            Property(path="/path84/binaryblobarray_endpoint", value=[b"SGVsbG8=", b"dDk5Yg=="]),
            Property(path="/path84/boolean_endpoint", value=True),
            Property(path="/path84/booleanarray_endpoint", value=[True, False, True]),
            Property(
                path="/path84/datetime_endpoint",
                value=datetime.fromtimestamp(1710940988, tz=timezone.utc),
            ),
            Property(
                path="/path84/datetimearray_endpoint",
                value=[
                    datetime.fromtimestamp(1710940988, tz=timezone.utc),
                    datetime.fromtimestamp(17109409814, tz=timezone.utc),
                ],
            ),
            Property(path="/path84/double_endpoint", value=15.42),
            Property(path="/path84/doublearray_endpoint", value=[1542.25, 88852.6]),
            Property(path="/path84/integer_endpoint", value=42),
            Property(path="/path84/integerarray_endpoint", value=[4525, 0, 11]),
            Property(path="/path84/longinteger_endpoint", value=8589934592),
            Property(path="/path84/longintegerarray_endpoint", value=[8589930067, 42, 8589934592]),
            Property(path="/path84/string_endpoint", value="Hello world!"),
            Property(path="/path84/stringarray_endpoint", value=["Hello ", "world!"]),
        ],
    ),
    Aggregate(
        interface="org.astarte-platform.zephyr.e2etest.DeviceAggregate",
        ownership=Ownership.DEVICE,
        path="/sensor42",
        entries={
            "binaryblob_endpoint": b"SGVsbG8=",
            "binaryblobarray_endpoint": [b"SGVsbG8=", b"dDk5Yg=="],
            "boolean_endpoint": True,
            "booleanarray_endpoint": [True, False, True],
            "datetime_endpoint": datetime.fromtimestamp(1710940988, tz=timezone.utc),
            "datetimearray_endpoint": [
                datetime.fromtimestamp(17109409814, tz=timezone.utc),
                datetime.fromtimestamp(1710940988, tz=timezone.utc),
            ],
            "double_endpoint": 15.42,
            "doublearray_endpoint": [1542.25, 88852.6],
            "integer_endpoint": 42,
            "integerarray_endpoint": [4525, 0, 11],
            "longinteger_endpoint": 8589934592,
            "longintegerarray_endpoint": [8589930067, 42, 8589934592],
            "string_endpoint": "Hello world!",
            "stringarray_endpoint": ["Hello ", "world!"],
        },
    ),
    Aggregate(
        interface="org.astarte-platform.zephyr.e2etest.ServerAggregate",
        ownership=Ownership.SERVER,
        path="/path37",
        timestamp=datetime.now(tz=timezone.utc),
        entries={
            "binaryblob_endpoint": b"SGVsbG8=",
            "binaryblobarray_endpoint": [b"SGVsbG8=", b"dDk5Yg=="],
            "boolean_endpoint": True,
            "booleanarray_endpoint": [True, False, True],
            "datetime_endpoint": datetime.fromtimestamp(1710940988, tz=timezone.utc),
            "datetimearray_endpoint": [
                datetime.fromtimestamp(17109409814, tz=timezone.utc),
                datetime.fromtimestamp(1710940988, tz=timezone.utc),
            ],
            "double_endpoint": 15.42,
            "doublearray_endpoint": [1542.25, 88852.6],
            "integer_endpoint": 42,
            "integerarray_endpoint": [4525, 0, 11],
            "longinteger_endpoint": 8589934592,
            "longintegerarray_endpoint": [8589930067, 42, 8589934592],
            "string_endpoint": "Hello world!",
            "stringarray_endpoint": ["Hello ", "world!"],
        },
    ),
    Datastream(
        interface="org.astarte-platform.zephyr.e2etest.DeviceDatastream",
        ownership=Ownership.DEVICE,
        mappings=[
            DatastreamMapping(
                path="/binaryblob_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=b"SGVsbG8=",
            ),
            DatastreamMapping(
                path="/binaryblobarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[b"SGVsbG8=", b"dDk5Yg=="],
            ),
            DatastreamMapping(
                path="/boolean_endpoint", timestamp=datetime.now(tz=timezone.utc), value=True
            ),
            DatastreamMapping(
                path="/booleanarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[True, False, True],
            ),
            DatastreamMapping(
                path="/datetime_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=datetime.fromtimestamp(1710940988, tz=timezone.utc),
            ),
            DatastreamMapping(
                path="/datetimearray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[
                    datetime.fromtimestamp(17109409814, tz=timezone.utc),
                    datetime.fromtimestamp(1710940988, tz=timezone.utc),
                ],
            ),
            DatastreamMapping(
                path="/double_endpoint", timestamp=datetime.now(tz=timezone.utc), value=15.42
            ),
            DatastreamMapping(
                path="/doublearray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[1542.25, 88852.6],
            ),
            DatastreamMapping(
                path="/integer_endpoint", timestamp=datetime.now(tz=timezone.utc), value=42
            ),
            DatastreamMapping(
                path="/integerarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[4525, 0, 11],
            ),
            DatastreamMapping(
                path="/longinteger_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=8589934592,
            ),
            DatastreamMapping(
                path="/longintegerarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[8589930067, 42, 8589934592],
            ),
            DatastreamMapping(
                path="/string_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value="Hello world!",
            ),
            DatastreamMapping(
                path="/stringarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=["Hello ", "world!"],
            ),
        ],
    ),
    Datastream(
        interface="org.astarte-platform.zephyr.e2etest.ServerDatastream",
        ownership=Ownership.SERVER,
        mappings=[
            DatastreamMapping(
                path="/binaryblob_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=b"SGVsbG8=",
            ),
            DatastreamMapping(
                path="/binaryblobarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[b"SGVsbG8=", b"dDk5Yg=="],
            ),
            DatastreamMapping(
                path="/boolean_endpoint", timestamp=datetime.now(tz=timezone.utc), value=True
            ),
            DatastreamMapping(
                path="/booleanarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[True, False, True],
            ),
            DatastreamMapping(
                path="/datetime_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=datetime.fromtimestamp(1710940988, tz=timezone.utc),
            ),
            DatastreamMapping(
                path="/datetimearray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[
                    datetime.fromtimestamp(17109409814, tz=timezone.utc),
                    datetime.fromtimestamp(1710940988, tz=timezone.utc),
                ],
            ),
            DatastreamMapping(
                path="/double_endpoint", timestamp=datetime.now(tz=timezone.utc), value=15.42
            ),
            DatastreamMapping(
                path="/doublearray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[1542.25, 88852.6],
            ),
            DatastreamMapping(
                path="/integer_endpoint", timestamp=datetime.now(tz=timezone.utc), value=42
            ),
            DatastreamMapping(
                path="/integerarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[4525, 0, 11],
            ),
            DatastreamMapping(
                path="/longinteger_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=8589934592,
            ),
            DatastreamMapping(
                path="/longintegerarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=[8589930067, 42, 8589934592],
            ),
            DatastreamMapping(
                path="/string_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value="Hello world!",
            ),
            DatastreamMapping(
                path="/stringarray_endpoint",
                timestamp=datetime.now(tz=timezone.utc),
                value=["Hello ", "world!"],
            ),
        ],
    ),
    PropertyUnset(
        interface="org.astarte-platform.zephyr.e2etest.DeviceProperty",
        ownership=Ownership.DEVICE,
        unset=[
            "/sensor36/binaryblob_endpoint",
            "/sensor36/binaryblobarray_endpoint",
            "/sensor36/boolean_endpoint",
            "/sensor36/booleanarray_endpoint",
            "/sensor36/datetime_endpoint",
            "/sensor36/datetimearray_endpoint",
            "/sensor36/double_endpoint",
            "/sensor36/doublearray_endpoint",
            "/sensor36/integer_endpoint",
            "/sensor36/integerarray_endpoint",
            "/sensor36/longinteger_endpoint",
            "/sensor36/longintegerarray_endpoint",
            "/sensor36/string_endpoint",
            "/sensor36/stringarray_endpoint",
        ],
    ),
    PropertyUnset(
        interface="org.astarte-platform.zephyr.e2etest.ServerProperty",
        ownership=Ownership.SERVER,
        unset=[
            "/path84/binaryblob_endpoint",
            "/path84/binaryblobarray_endpoint",
            "/path84/boolean_endpoint",
            "/path84/booleanarray_endpoint",
            "/path84/datetime_endpoint",
            "/path84/datetimearray_endpoint",
            "/path84/double_endpoint",
            "/path84/doublearray_endpoint",
            "/path84/integer_endpoint",
            "/path84/integerarray_endpoint",
            "/path84/longinteger_endpoint",
            "/path84/longintegerarray_endpoint",
            "/path84/string_endpoint",
            "/path84/stringarray_endpoint",
        ],
    ),
]
