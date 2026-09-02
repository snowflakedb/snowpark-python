#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""mitmdump addon that records every request mitmproxy sees.

Loaded via `mitmdump -s tests/mitmproxy_addon.py`. Appends one JSON line per
request to the file at MITMPROXY_CAPTURE_OUTPUT_PATH; MitmproxyClient reads
that file to answer get_requests()/wait_for_requests(). Traffic is otherwise
untouched -- this addon never returns a response, so requests continue to
their real destination.
"""
import json
import os


def request(flow) -> None:
    output_path = os.environ["MITMPROXY_CAPTURE_OUTPUT_PATH"]
    record = {
        "method": flow.request.method,
        "url": flow.request.pretty_url,
        "headers": dict(flow.request.headers),
        "body": flow.request.text,
    }
    with open(output_path, "a") as f:
        f.write(json.dumps(record) + "\n")
