#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.

import types

from snowflake.snowpark._internal import event_table_telemetry as ett


def test_import_or_missing_opentelemetry_imports_resources(monkeypatch):
    # Build a fake opentelemetry module tree
    opentelemetry = types.ModuleType("opentelemetry")

    sdk = types.ModuleType("opentelemetry.sdk")
    sdk_logs = types.ModuleType("opentelemetry.sdk._logs")
    sdk_resources = types.ModuleType("opentelemetry.sdk.resources")

    class Resource:
        @classmethod
        def create(cls, attrs):
            return ("resource", attrs)

    sdk_resources.Resource = Resource

    exporter = types.ModuleType("opentelemetry.exporter")
    exporter_otlp = types.ModuleType("opentelemetry.exporter.otlp")
    exporter_otlp_proto = types.ModuleType("opentelemetry.exporter.otlp.proto")
    exporter_otlp_proto_http = types.ModuleType(
        "opentelemetry.exporter.otlp.proto.http"
    )
    trace_exporter = types.ModuleType(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter"
    )
    log_exporter = types.ModuleType(
        "opentelemetry.exporter.otlp.proto.http._log_exporter"
    )

    logs_api = types.ModuleType("opentelemetry._logs")

    registry = {
        "opentelemetry": opentelemetry,
        "opentelemetry.sdk": sdk,
        "opentelemetry.sdk._logs": sdk_logs,
        "opentelemetry.sdk.resources": sdk_resources,
        "opentelemetry.exporter": exporter,
        "opentelemetry.exporter.otlp": exporter_otlp,
        "opentelemetry.exporter.otlp.proto": exporter_otlp_proto,
        "opentelemetry.exporter.otlp.proto.http": exporter_otlp_proto_http,
        "opentelemetry.exporter.otlp.proto.http.trace_exporter": trace_exporter,
        "opentelemetry.exporter.otlp.proto.http._log_exporter": log_exporter,
        "opentelemetry._logs": logs_api,
    }

    def fake_import_module(name):
        # Simulate ImportError if an expected dependency isn't present
        if name not in registry:
            raise ImportError(name)

        mod = registry[name]

        # Emulate Python import behavior: after importing X.Y, X gets attribute Y.
        if "." in name:
            parent_name, child = name.rsplit(".", 1)
            setattr(registry[parent_name], child, mod)

        return mod

    monkeypatch.setattr(ett.importlib, "import_module", fake_import_module)

    otel, installed = ett._import_or_missing_opentelemetry()

    assert installed is True
    assert otel is opentelemetry

    # Key regression check: no AttributeError when accessing otel.sdk.resources
    assert hasattr(otel, "sdk")
    assert hasattr(otel.sdk, "resources")
    assert otel.sdk.resources is registry["opentelemetry.sdk.resources"]
    assert otel.sdk.resources.Resource is Resource

    # Also verify the "import path" works (in our mocked import system)
    m = ett.importlib.import_module("opentelemetry.sdk.resources")
    assert m is otel.sdk.resources

    # Sanity: Resource.create callable
    assert otel.sdk.resources.Resource.create({"service.name": "X"}) == (
        "resource",
        {"service.name": "X"},
    )


def test_import_or_missing_opentelemetry_returns_missing_on_importerror(monkeypatch):
    def always_fail(_name):
        raise ImportError("missing")

    monkeypatch.setattr(ett.importlib, "import_module", always_fail)

    otel, installed = ett._import_or_missing_opentelemetry()

    assert installed is False
    assert isinstance(otel, ett.MissingOpenTelemetry)
