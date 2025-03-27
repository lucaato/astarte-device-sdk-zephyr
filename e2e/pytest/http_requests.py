# (C) Copyright 2024, SECO Mind Srl
#
# SPDX-License-Identifier: Apache-2.0

"""
Contains useful wrappers for HTTPS requests.
"""
import base64
import json
from typing import Any

import curlify
import requests
from cfgvalues import CfgValues
from dateutil import parser

from west import log
from datetime import datetime, timezone


def get_server_data(
    test_cfg: CfgValues,
    interface: str,
    path: str | None = None,
    quiet: bool = False,
    limit: int | None = None,
    since_after: datetime | None = None,
    to: datetime | None = None,
) -> object:
    """
    Wrapper for a GET request for the server returning the specified interface data.
    """
    url = (
        test_cfg.appengine_url
        + "/v1/"
        + test_cfg.realm
        + "/devices/"
        + test_cfg.device_id
        + "/interfaces/"
        + interface
    )

    if not path is None:
        url += path

    headers = {"Authorization": "Bearer " + test_cfg.appengine_token}

    params: dict[str, str] = {}

    if not since_after is None:
        params["since_after"] = since_after.isoformat()

    if not limit is None:
        params["limit"] = str(limit)

    if not to is None:
        params["to"] = to.isoformat()

    res = requests.get(
        url, headers=headers, params=params, timeout=5, verify=test_cfg.appengine_cert
    )
    log.inf(curlify.to_curl(res.request))
    if res.status_code != 200:
        if not quiet:
            log.err(res.text)
        raise requests.HTTPError(f"GET request failed. response {res}")

    return res.json().get("data", {})


def post_server_data(
    test_cfg: CfgValues, interface: str, endpoint: str, data: dict, quiet: bool = False
):
    """
    Wrapper for a POST request for the server, uploading new values to an interface.
    """
    url = (
        test_cfg.appengine_url
        + "/v1/"
        + test_cfg.realm
        + "/devices/"
        + test_cfg.device_id
        + "/interfaces/"
        + interface
        + endpoint
    )
    json_data = json.dumps({"data": data}, default=str)
    headers = {
        "Authorization": "Bearer " + test_cfg.appengine_token,
        "Content-Type": "application/json",
    }
    res = requests.post(
        url=url, data=json_data, headers=headers, timeout=5, verify=test_cfg.appengine_cert
    )
    log.inf(curlify.to_curl(res.request))
    if res.status_code != 200:
        if not quiet:
            log.err(res.text)
        raise requests.HTTPError(f"POST request failed. response {res}")


def unset_server_property(test_cfg: CfgValues, interface: str, endpoint: str, quiet: bool = False):
    """
    Wrapper for a DELETE request for the server, deleting an endpoint.
    """
    url = (
        test_cfg.appengine_url
        + "/v1/"
        + test_cfg.realm
        + "/devices/"
        + test_cfg.device_id
        + "/interfaces/"
        + interface
        + endpoint
    )
    headers = {
        "Authorization": "Bearer " + test_cfg.appengine_token,
        "Content-Type": "application/json",
    }
    res = requests.delete(url, headers=headers, timeout=5, verify=test_cfg.appengine_cert)
    log.inf(curlify.to_curl(res.request))
    if res.status_code != 204:
        if not quiet:
            log.err(res.text)
        raise requests.HTTPError("DELETE request failed.")


# FIXME we should read the interface file to have access to the fields types instead of encoding based on field name
def prepare_transmit_data(key, value) -> Any:
    """
    Some data to be transmitted should be encoded to an appropriate type.
    """
    if key == "binaryblob_endpoint":
        return base64.b64encode(value).decode("utf-8")

    if key == "binaryblobarray_endpoint":
        return [base64.b64encode(v).decode("utf-8") for v in value]
    return value


# FIXME we should read the interface file to have access to the fields types instead of decoding based on field name
def decode_value(recv: Any, prop: str):
    """
    Decode a value read from astarte and map it approprietly to a python value
    """
    e: Any = recv

    if (isinstance(prop, str) and prop.endswith("array_endpoint")) and (e == None):
        return []

    # Parse longinteger from string to number (only necessary for aggregates)
    if "longinteger_endpoint" == prop:
        return int(e)
    if "longintegerarray_endpoint" == prop:
        return [int(dt) for dt in e]

    # Parse datetime from string to datetime
    if "datetime_endpoint" == prop:
        return parser.parse(e)
    if "datetimearray_endpoint" == prop:
        return [parser.parse(dt) for dt in e]

    # Decode binary blob from base64
    if "binaryblob_endpoint" == prop:
        return base64.b64decode(e)
    if "binaryblobarray_endpoint" == prop:
        return [base64.b64decode(dt) for dt in e]

    return e
