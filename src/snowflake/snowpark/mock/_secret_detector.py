#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""The secret detector detects sensitive information.

It masks secrets that might be leaked from two potential avenues
    1. Out of Band Telemetry
    2. Logging

Ported from snowflake-connector-python's legacy SecretDetector via the
Universal Driver's backward-compat shim (drivers#598).
"""
from __future__ import annotations

import logging
import os
import re

from typing import NamedTuple

MIN_TOKEN_LEN = os.getenv("MIN_TOKEN_LEN", 32)
MIN_PWD_LEN = os.getenv("MIN_PWD_LEN", 8)


class MaskedMessageData(NamedTuple):
    is_masked: bool = False
    masked_text: str | None = None
    error_str: str | None = None


class SecretDetector(logging.Formatter):
    AWS_KEY_PATTERN = re.compile(
        r"(aws_key_id|aws_secret_key|access_key_id|secret_access_key)\s*=\s*'([^']+)'",
        flags=re.IGNORECASE,
    )
    AWS_TOKEN_PATTERN = re.compile(
        r'(accessToken|tempToken|keySecret)"\s*:\s*"([a-z0-9/+]{32,}={0,2})"',
        flags=re.IGNORECASE,
    )
    # Detects OAuth access/refresh tokens in serialized JSON (e.g. the OAuth
    # token-exchange response), matching legacy JDBC's OAUTH_JSON_PATTERN.
    OAUTH_TOKEN_PATTERN = re.compile(
        r'(access_token|refresh_token)"\s*:\s*"([a-z0-9!"#\$%&\'\(\)\*\+\,\-\./:;<=>\?\@\[\]\^_`\{\|\}~]{3,})"',
        flags=re.IGNORECASE,
    )
    SAS_TOKEN_PATTERN = re.compile(
        r"(sig|signature|AWSAccessKeyId|password|passcode)=(?P<secret>[a-z0-9%/+]{16,})",
        flags=re.IGNORECASE,
    )
    PRIVATE_KEY_PATTERN = re.compile(
        r"-{3,}BEGIN [A-Z ]*PRIVATE KEY-{3,}\n([\s\S]*?)\n-{3,}END [A-Z ]*PRIVATE KEY-{3,}",
        flags=re.MULTILINE | re.IGNORECASE,
    )
    PRIVATE_KEY_DATA_PATTERN = re.compile(
        r'"privateKeyData": "([a-z0-9/+=\\n]{10,})"', flags=re.MULTILINE | re.IGNORECASE
    )
    # ':' and '%' are in the value class so a version/hint-prefixed session token
    # (e.g. "token=ver:1-hint:1036-<value>", Snowflake's actual wire format) masks
    # in full instead of stopping at the first ':' -- matches legacy Node.js's fix.
    CONNECTION_TOKEN_PATTERN = re.compile(
        r"(token|assertion content)" r"([\'\"\s:=]+)" r"([a-z0-9=/_\-\+\.:%]{8,})",
        flags=re.IGNORECASE,
    )
    # Matches legacy Node.js's OAUTH_CLIENT_SECRET_PATTERN.
    OAUTH_CLIENT_SECRET_PATTERN = re.compile(
        r"(oauthClientId|oauthClientSecret|clientSecret)"
        r"([\'\"\s:=]+)"
        r"([a-z0-9!\"#\$%&\\\'\(\)\*\+\,-\./:;<=>\?\@\[\]\^_`\{\|\}~]{8,})",
        flags=re.IGNORECASE,
    )
    # Matches legacy Node.js's PASSCODE_PATTERN.
    PASSCODE_PATTERN = re.compile(
        r"(passcode|otp|pin|otac)\s*([:=])\s*([0-9]{4,6})",
        flags=re.IGNORECASE,
    )

    # Value quantifier is {6,} (not {1,}) so short common words after a bare
    # "password"/"pwd" keyword -- e.g. "...no ID password was not given" -- are
    # not mistaken for the secret value itself; matches legacy .NET/JDBC's floor.
    PASSWORD_PATTERN = re.compile(
        r"(password"
        r"|pwd)"
        r"([\'\"\s:=]+)"
        r"([a-z0-9!\"#\$%&\\\'\(\)\*\+\,-\./:;<=>\?\@\[\]\^_`\{\|\}~]{6,})",
        flags=re.IGNORECASE,
    )

    SECRET_STARRED_MASK_STR = "****"

    @classmethod
    def mask_connection_token(cls, text: str) -> str:
        return cls.CONNECTION_TOKEN_PATTERN.sub(
            r"\1\2" + f"{cls.SECRET_STARRED_MASK_STR}", text
        )

    @classmethod
    def mask_password(cls, text: str) -> str:
        return cls.PASSWORD_PATTERN.sub(
            r"\1\2" + f"{cls.SECRET_STARRED_MASK_STR}", text
        )

    @classmethod
    def mask_aws_keys(cls, text: str) -> str:
        return cls.AWS_KEY_PATTERN.sub(
            r"\1=" + f"'{cls.SECRET_STARRED_MASK_STR}'", text
        )

    @classmethod
    def mask_sas_tokens(cls, text: str) -> str:
        return cls.SAS_TOKEN_PATTERN.sub(
            r"\1=" + f"{cls.SECRET_STARRED_MASK_STR}", text
        )

    @classmethod
    def mask_aws_tokens(cls, text: str) -> str:
        return cls.AWS_TOKEN_PATTERN.sub(r'\1":"XXXX"', text)

    @classmethod
    def mask_oauth_tokens(cls, text: str) -> str:
        return cls.OAUTH_TOKEN_PATTERN.sub(r'\1":"XXXX"', text)

    @classmethod
    def mask_oauth_client_secrets(cls, text: str) -> str:
        return cls.OAUTH_CLIENT_SECRET_PATTERN.sub(
            r"\1\2" + f"{cls.SECRET_STARRED_MASK_STR}", text
        )

    @classmethod
    def mask_passcodes(cls, text: str) -> str:
        return cls.PASSCODE_PATTERN.sub(
            r"\1\2" + f"{cls.SECRET_STARRED_MASK_STR}", text
        )

    @classmethod
    def mask_private_key(cls, text: str) -> str:
        return cls.PRIVATE_KEY_PATTERN.sub(
            "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----", text
        )

    @classmethod
    def mask_private_key_data(cls, text: str) -> str:
        return cls.PRIVATE_KEY_DATA_PATTERN.sub('"privateKeyData": "XXXX"', text)

    @classmethod
    def mask_secrets(cls, text: str | None) -> MaskedMessageData:
        """Return ``text`` with any detected secrets masked (the public entry point).

        These maskers are ``classmethod``s (not the legacy ``staticmethod``s) so
        their ``cls.`` self-references resolve even though
        ``@backward_compatibility`` stashes ``SecretDetector`` out of the module
        globals; a bare ``SecretDetector.`` reference would raise ``NameError``.
        """
        if text is None:
            return MaskedMessageData()

        masked = False
        err_str = None
        try:
            masked_text = cls.mask_aws_keys(text)
            masked_text = cls.mask_sas_tokens(masked_text)
            masked_text = cls.mask_aws_tokens(masked_text)
            masked_text = cls.mask_oauth_tokens(masked_text)
            masked_text = cls.mask_private_key(masked_text)
            masked_text = cls.mask_private_key_data(masked_text)
            masked_text = cls.mask_oauth_client_secrets(masked_text)
            masked_text = cls.mask_passcodes(masked_text)
            masked_text = cls.mask_password(masked_text)
            masked_text = cls.mask_connection_token(masked_text)
            if masked_text != text:
                masked = True
        except Exception as ex:
            # We'll assume that the exception was raised during masking
            # to be safe consider that the log has sensitive information
            # and do not raise an exception.
            masked = True
            masked_text = str(ex)
            err_str = str(ex)

        return MaskedMessageData(masked, masked_text, err_str)

    @staticmethod
    def create_formatting_error_log(
        original_record: logging.LogRecord, error_message: str
    ) -> str:
        return "{} - {} {} - {} - {} - {}".format(
            original_record.asctime,
            original_record.threadName,
            "secret_detector.py",
            "sanitize_log_str",
            original_record.levelname,
            error_message,
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format ``record`` via ``logging.Formatter``, masking any secrets.

        Ensures the formatted message is free from sensitive credentials before
        it reaches a log handler.
        """
        try:
            unsanitized_log = super().format(record)
            masked, optional_sanitized_log, err_str = type(self).mask_secrets(
                unsanitized_log
            )
            # Added to comply with type hints (Optional[str] is not accepted for str)
            sanitized_log = optional_sanitized_log or ""

            if masked and err_str is not None:
                sanitized_log = self.create_formatting_error_log(record, err_str)

        except Exception as ex:
            sanitized_log = self.create_formatting_error_log(
                record, "EXCEPTION - " + str(ex)
            )

        return sanitized_log
