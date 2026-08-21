#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Masks secrets that might otherwise leak via out-of-band telemetry.

Ported from the Universal Driver's ``connector._common.secret_detector.SecretDetector``
backward-compat shim -- only the masking behavior LocalTestOOBTelemetryService actually
calls, not the full legacy surface (NamedTuple return, logging.Formatter integration,
backward-compat shims, classmethod workaround). Every masking regex and the order
they're applied in is kept verbatim from the original -- this is a security control,
not a cosmetic detail.
"""
import re
from typing import Optional

_MASKING_FAILURE_SENTINEL = "****"

_AWS_KEY_PATTERN = re.compile(
    r"(aws_key_id|aws_secret_key|access_key_id|secret_access_key)\s*=\s*'([^']+)'",
    flags=re.IGNORECASE,
)
_AWS_TOKEN_PATTERN = re.compile(
    r'(accessToken|tempToken|keySecret)"\s*:\s*"([a-z0-9/+]{32,}={0,2})"',
    flags=re.IGNORECASE,
)
# Detects OAuth access/refresh tokens in serialized JSON (e.g. the OAuth
# token-exchange response), matching legacy JDBC's OAUTH_JSON_PATTERN.
_OAUTH_TOKEN_PATTERN = re.compile(
    r'(access_token|refresh_token)"\s*:\s*"([a-z0-9!"#\$%&\'\(\)\*\+\,\-\./:;<=>\?\@\[\]\^_`\{\|\}~]{3,})"',
    flags=re.IGNORECASE,
)
_SAS_TOKEN_PATTERN = re.compile(
    r"(sig|signature|AWSAccessKeyId|password|passcode)=(?P<secret>[a-z0-9%/+]{16,})",
    flags=re.IGNORECASE,
)
_PRIVATE_KEY_PATTERN = re.compile(
    r"-{3,}BEGIN [A-Z ]*PRIVATE KEY-{3,}\n([\s\S]*?)\n-{3,}END [A-Z ]*PRIVATE KEY-{3,}",
    flags=re.MULTILINE | re.IGNORECASE,
)
_PRIVATE_KEY_DATA_PATTERN = re.compile(
    r'"privateKeyData": "([a-z0-9/+=\\n]{10,})"',
    flags=re.MULTILINE | re.IGNORECASE,
)
# ':' and '%' are in the value class so a version/hint-prefixed session token
# (e.g. "token=ver:1-hint:1036-<value>", Snowflake's actual wire format) masks
# in full instead of stopping at the first ':' -- matches legacy Node.js's fix.
_CONNECTION_TOKEN_PATTERN = re.compile(
    r"(token|assertion content)" r"([\'\"\s:=]+)" r"([a-z0-9=/_\-\+\.:%]{8,})",
    flags=re.IGNORECASE,
)
# Matches legacy Node.js's OAUTH_CLIENT_SECRET_PATTERN.
_OAUTH_CLIENT_SECRET_PATTERN = re.compile(
    r"(oauthClientId|oauthClientSecret|clientSecret)"
    r"([\'\"\s:=]+)"
    r"([a-z0-9!\"#\$%&\\\'\(\)\*\+\,-\./:;<=>\?\@\[\]\^_`\{\|\}~]{8,})",
    flags=re.IGNORECASE,
)
# Matches legacy Node.js's PASSCODE_PATTERN.
_PASSCODE_PATTERN = re.compile(
    r"(passcode|otp|pin|otac)\s*([:=])\s*([0-9]{4,6})",
    flags=re.IGNORECASE,
)
# Value quantifier is {6,} (not {1,}) so short common words after a bare
# "password"/"pwd" keyword -- e.g. "...no ID password was not given" -- are
# not mistaken for the secret value itself; matches legacy .NET/JDBC's floor.
_PASSWORD_PATTERN = re.compile(
    r"(password"
    r"|pwd)"
    r"([\'\"\s:=]+)"
    r"([a-z0-9!\"#\$%&\\\'\(\)\*\+\,-\./:;<=>\?\@\[\]\^_`\{\|\}~]{6,})",
    flags=re.IGNORECASE,
)

_SECRET_STARRED_MASK_STR = "****"


def _mask_aws_keys(text: str) -> str:
    return _AWS_KEY_PATTERN.sub(r"\1=" + f"'{_SECRET_STARRED_MASK_STR}'", text)


def _mask_sas_tokens(text: str) -> str:
    return _SAS_TOKEN_PATTERN.sub(r"\1=" + _SECRET_STARRED_MASK_STR, text)


def _mask_aws_tokens(text: str) -> str:
    return _AWS_TOKEN_PATTERN.sub(r'\1":"XXXX"', text)


def _mask_oauth_tokens(text: str) -> str:
    return _OAUTH_TOKEN_PATTERN.sub(r'\1":"XXXX"', text)


def _mask_private_key(text: str) -> str:
    return _PRIVATE_KEY_PATTERN.sub(
        "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----", text
    )


def _mask_private_key_data(text: str) -> str:
    return _PRIVATE_KEY_DATA_PATTERN.sub('"privateKeyData": "XXXX"', text)


def _mask_oauth_client_secrets(text: str) -> str:
    return _OAUTH_CLIENT_SECRET_PATTERN.sub(r"\1\2" + _SECRET_STARRED_MASK_STR, text)


def _mask_passcodes(text: str) -> str:
    return _PASSCODE_PATTERN.sub(r"\1\2" + _SECRET_STARRED_MASK_STR, text)


def _mask_password(text: str) -> str:
    return _PASSWORD_PATTERN.sub(r"\1\2" + _SECRET_STARRED_MASK_STR, text)


def _mask_connection_token(text: str) -> str:
    return _CONNECTION_TOKEN_PATTERN.sub(r"\1\2" + _SECRET_STARRED_MASK_STR, text)


def mask_secrets(text: Optional[str]) -> Optional[str]:
    """Return text with any detected secrets masked. On masking failure,
    returns a static placeholder rather than the exception message, to
    avoid reflecting secret-containing input back into the caller."""
    if text is None:
        return None
    try:
        masked_text = _mask_aws_keys(text)
        masked_text = _mask_sas_tokens(masked_text)
        masked_text = _mask_aws_tokens(masked_text)
        masked_text = _mask_oauth_tokens(masked_text)
        masked_text = _mask_private_key(masked_text)
        masked_text = _mask_private_key_data(masked_text)
        masked_text = _mask_oauth_client_secrets(masked_text)
        masked_text = _mask_passcodes(masked_text)
        masked_text = _mask_password(masked_text)
        masked_text = _mask_connection_token(masked_text)
        return masked_text
    except Exception:
        return _MASKING_FAILURE_SENTINEL
