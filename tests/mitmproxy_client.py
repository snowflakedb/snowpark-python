#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Passive HTTPS capture for integration tests, via mitmproxy.

Unlike a stub server, this never fabricates a response: it runs `mitmdump` as
a real forward proxy so login/query/telemetry traffic reaches its actual
destination untouched, while tests/mitmproxy_addon.py records every request
mitmproxy decrypts to a JSON-lines file this client reads.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional  # noqa: F401


class MitmproxyClient:
    _ADDON_PATH = Path(__file__).parent / "mitmproxy_addon.py"

    def __init__(self) -> None:
        self.host = "127.0.0.1"
        self.port: Optional[int] = None
        self._process: Optional[subprocess.Popen] = None
        fd, path = tempfile.mkstemp(prefix="mitmproxy_capture_", suffix=".jsonl")
        os.close(fd)
        self._output_path = Path(path)

    def start(self) -> "MitmproxyClient":
        if shutil.which("mitmdump") is None:
            raise RuntimeError(
                "mitmdump not found on PATH. Install with: pip install mitmproxy"
            )
        self.port = self._find_free_port()
        env = {**os.environ, "MITMPROXY_CAPTURE_OUTPUT_PATH": str(self._output_path)}
        self._process = subprocess.Popen(
            [
                "mitmdump",
                "--listen-host",
                self.host,
                "--listen-port",
                str(self.port),
                "-s",
                str(self._ADDON_PATH),
            ],
            env=env,
            # Redirect to DEVNULL: an unread stdout/stderr pipe can deadlock
            # mitmdump's logging thread once its buffer fills.
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._wait_for_port()
        self._wait_for_ca_cert()
        return self

    @staticmethod
    def _find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _wait_for_port(self, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"mitmdump exited early with code {self._process.returncode}"
                )
            try:
                with socket.create_connection((self.host, self.port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError("mitmdump did not become ready in time")

    def _wait_for_ca_cert(self, timeout: float = 10.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.ca_cert_path.exists():
                return
            time.sleep(0.2)
        raise RuntimeError("mitmproxy CA cert was not generated in time")

    @property
    def ca_cert_path(self) -> Path:
        return Path.home() / ".mitmproxy" / "mitmproxy-ca-cert.pem"

    @property
    def proxy_host(self) -> str:
        return self.host

    @property
    def proxy_port(self) -> int:
        return self.port

    def reset(self) -> None:
        self._output_path.write_text("")

    def get_requests(self, url_path_pattern: str) -> List[Dict[str, Any]]:
        if not self._output_path.exists():
            return []
        matches = []
        for line in self._output_path.read_text().splitlines():
            if not line:
                continue
            entry = json.loads(line)
            if re.search(url_path_pattern, entry["url"]):
                matches.append(entry)
        return matches

    def wait_for_requests(
        self,
        url_path_pattern: str,
        min_count: int = 1,
        timeout: float = 2.0,
        poll_interval: float = 0.1,
    ) -> List[Dict[str, Any]]:
        """Poll until at least `min_count` requests matching the pattern arrive.

        Useful for asserting on requests sent asynchronously (e.g. telemetry).
        """
        deadline = time.time() + timeout
        result: List[Dict[str, Any]] = []
        while time.time() < deadline:
            result = self.get_requests(url_path_pattern)
            if len(result) >= min_count:
                return result
            time.sleep(poll_interval)
        return result

    def stop(self) -> None:
        if self._process is not None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
            self._process = None
        if self._output_path.exists():
            self._output_path.unlink()

    def __enter__(self) -> "MitmproxyClient":
        return self.start()

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()
