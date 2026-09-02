#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Self-test proving the mitmproxy capture fixture works end-to-end.

Exists so a break in the fixture itself (proxy routing, CA trust, capture)
fails loudly here instead of silently as an empty-results false pass in the
telemetry tests that depend on it.
"""


def test_mitmproxy_captures_login_request(mitmproxy_session, mitmproxy):
    # mitmproxy_session already completed login by the time this test body
    # runs, so the request should already be captured.
    requests = mitmproxy.wait_for_requests(
        r"/session/v1/login-request", min_count=1, timeout=10.0
    )
    assert (
        len(requests) >= 1
    ), "Expected the session's login request to be captured by the proxy"
