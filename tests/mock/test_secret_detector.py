"""Behavioral tests for the ``SecretDetector`` shim, ported from the Universal
Driver's backward-compat test suite (drivers#598), which itself ports the full
legacy ``snowflake-connector-python`` masking-behavior suite
(``test_log_secret_detector.py`` and ``test_oob_secret_detector.py``) verbatim,
plus cases ported from other legacy drivers for masking gaps that legacy
Python itself never covered: AWS-token masking (from .NET's ``TestAWSTokens``),
OAuth-token JSON masking (from JDBC's ``OAUTH_JSON_PATTERN``),
OAuth-client-secret and passcode/OTP/PIN masking (from Node.js), the
version/hint-prefixed session-token wire format, and the ``PASSWORD_PATTERN``
false-positive fix (from .NET/JDBC's ``{6,}`` floor).
"""

import logging
import random
import string

from unittest import mock

import pytest

from snowflake.snowpark.mock._secret_detector import MaskedMessageData, SecretDetector


def _assert_not_masked(text):
    masked, masked_text, err_str = SecretDetector.mask_secrets(text)
    assert not masked
    assert err_str is None
    assert masked_text == text


class TestMaskSecrets:
    def test_clean_text_is_not_masked(self):
        _assert_not_masked("select 1 from dual")

    def test_none_text_returns_empty_result(self):
        result = SecretDetector.mask_secrets(None)
        assert result == MaskedMessageData()

    def test_empty_string_is_not_masked(self):
        _assert_not_masked("")

    def test_password_is_masked(self):
        random_password = "Fh[+2J~AcqeqW%?"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "password:" + random_password
        )
        assert masked
        assert err_str is None
        assert masked_text == "password:****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "PASSWORD:" + random_password
        )
        assert masked
        assert err_str is None
        assert masked_text == "PASSWORD:****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "PassWorD:" + random_password
        )
        assert masked
        assert err_str is None
        assert masked_text == "PassWorD:****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "password = " + random_password
        )
        assert masked
        assert err_str is None
        assert masked_text == "password = ****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "pwd:" + random_password
        )
        assert masked
        assert err_str is None
        assert masked_text == "pwd:****"

        masked, masked_text, err_str = SecretDetector.mask_secrets("password=Afs...")
        assert masked
        assert err_str is None
        assert masked_text == "password=****"

    @pytest.mark.parametrize(
        "text",
        [
            "2020-04-30 23:06:04,069 - MainThread auth.py:397"
            " - write_temporary_credential() - DEBUG - no ID password was not given",
            "2020-04-30 23:06:04,069 - MainThread auth.py:397"
            " - write_temporary_credential() - DEBUG - no ID proxyPassword was not given",
        ],
    )
    def test_password_false_positives_are_not_masked(self, text):
        """``PASSWORD_PATTERN``'s value floor is 6 chars (not 1) so a short common word
        right after a bare ``password``/``pwd`` keyword isn't mistaken for the secret
        itself; matches legacy .NET/JDBC's floor and ``TestPasswordFalsePositive``."""
        _assert_not_masked(text)

    def test_aws_key_is_masked(self):
        sql = (
            "copy into 's3://xxxx/test' from \n"
            "(select seq1(), random()\n"
            ", random(), random(), random(), random()\n"
            ", random(), random(), random(), random()\n"
            ", random() , random(), random(), random()\n"
            "\tfrom table(generator(rowcount => 10000)))\n"
            "credentials=(\n"
            "  aws_key_id='xxdsdfsafds'\n"
            "  aws_secret_key='safas+asfsad+safasf'\n"
            "  )\n"
            "OVERWRITE = TRUE \n"
            "MAX_FILE_SIZE = 500000000 \n"
            "HEADER = TRUE \n"
            "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n"
            ";"
        )
        correct = (
            "copy into 's3://xxxx/test' from \n"
            "(select seq1(), random()\n"
            ", random(), random(), random(), random()\n"
            ", random(), random(), random(), random()\n"
            ", random() , random(), random(), random()\n"
            "\tfrom table(generator(rowcount => 10000)))\n"
            "credentials=(\n"
            "  aws_key_id='****'\n"
            "  aws_secret_key='****'\n"
            "  )\n"
            "OVERWRITE = TRUE \n"
            "MAX_FILE_SIZE = 500000000 \n"
            "HEADER = TRUE \n"
            "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n"
            ";"
        )
        _, masked_text, _ = SecretDetector.mask_secrets(sql)
        assert masked_text == correct

    @pytest.mark.parametrize(
        "text,expected",
        [
            ('accessToken":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"', 'accessToken":"XXXX"'),
            ('tempToken":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"', 'tempToken":"XXXX"'),
            ('keySecret":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"', 'keySecret":"XXXX"'),
            (
                'accessToken"  :  "aB1aaaaaaaaaaZaaaaaaaaaaa9aaaaaaa="',
                'accessToken":"XXXX"',
            ),
            (
                'accessToken"  :  "aB1aaaaaaaaaaZaaaaaaaa56aaaaaaaaaa=="',
                'accessToken":"XXXX"',
            ),
        ],
    )
    def test_aws_tokens_are_masked(self, text, expected):
        """``AWS_TOKEN_PATTERN`` (``accessToken``/``tempToken``/``keySecret``) has no legacy
        Python test at all; ported from the legacy .NET driver's ``TestAWSTokens``."""
        masked, masked_text, err_str = SecretDetector.mask_secrets(text)
        assert masked
        assert err_str is None
        assert masked_text == expected

    def test_sas_tokens_are_masked(self):
        azure_sas_token = (
            "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-"
            "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;"
            "sig=iCvQmdZngZNW%2F4vw43j6%2BVz6fndHF5LI639QJba4r8o%3D&amp;"
            "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;"
            "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl"
        )
        masked_azure_sas_token = (
            "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-"
            "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;"
            "sig=****&amp;"
            "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;"
            "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl"
        )
        s3_sas_token = (
            "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3"
            "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?"
            "x-amz-server-side-encryption-customer-algorithm=AES256&"
            "response-content-encoding=gzip&AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"
            "&Expires=1555481960&Signature=zFiRkdB9RtRRYomppVes4fQ%2ByWw%3D"
        )
        masked_s3_sas_token = (
            "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3"
            "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?"
            "x-amz-server-side-encryption-customer-algorithm=AES256&"
            "response-content-encoding=gzip&AWSAccessKeyId=****"
            "&Expires=1555481960&Signature=****"
        )

        _, masked_text, _ = SecretDetector.mask_secrets(azure_sas_token)
        assert masked_text == masked_azure_sas_token

        _, masked_text, _ = SecretDetector.mask_secrets(s3_sas_token)
        assert masked_text == masked_s3_sas_token

        text = "".join(random.choice(string.ascii_lowercase) for _ in range(200))
        _, masked_text, _ = SecretDetector.mask_secrets(text)
        assert masked_text == text

        _, masked_text, _ = SecretDetector.mask_secrets(
            azure_sas_token + "\n" + azure_sas_token
        )
        assert masked_text == masked_azure_sas_token + "\n" + masked_azure_sas_token

        _, masked_text, _ = SecretDetector.mask_secrets(
            s3_sas_token + "\n" + s3_sas_token
        )
        assert masked_text == masked_s3_sas_token + "\n" + masked_s3_sas_token

        _, masked_text, _ = SecretDetector.mask_secrets(
            azure_sas_token + "\n" + s3_sas_token
        )
        assert masked_text == masked_azure_sas_token + "\n" + masked_s3_sas_token

    def test_combined_secrets_in_create_stage_sql(self):
        sql = (
            "create stage mystage "
            "URL = 's3://mybucket/mypath/' "
            "credentials = (aws_key_id = 'AKIAIOSFODNN7EXAMPLE' "
            "aws_secret_key = 'frJIUN8DYpKDtOLCwo//yllqDzg='); "
            "create stage mystage2 "
            "URL = 'azure//mystorage.blob.core.windows.net/cont' "
            "credentials = (azure_sas_token = "
            "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&"
            "st=2017-06-27T02:05:50Z&spr=https,http&"
            "sig=bgqQwoXwxzuD2GJfagRg7VOS8hzNr3QLT7rhS8OFRLQ%3D')"
        )
        masked_sql = (
            "create stage mystage "
            "URL = 's3://mybucket/mypath/' "
            "credentials = (aws_key_id='****' "
            "aws_secret_key='****'); "
            "create stage mystage2 "
            "URL = 'azure//mystorage.blob.core.windows.net/cont' "
            "credentials = (azure_sas_token = "
            "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&"
            "st=2017-06-27T02:05:50Z&spr=https,http&"
            "sig=****')"
        )
        _, masked_text, _ = SecretDetector.mask_secrets(sql)
        assert masked_text == masked_sql

        text = "".join(random.choice(string.ascii_lowercase) for _ in range(500))
        _, masked_text, _ = SecretDetector.mask_secrets(text)
        assert masked_text == text

    def test_connection_token_is_masked(self):
        long_token = (
            "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U"
            "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj"
            "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F"
            "mClCMBSissVsU3Ei590FP0lPQQhcSGcD"
            "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU="
            "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj"
            "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR"
            "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5"
            "XdJYuI8vhg=f0bKSq7AhQ2Bh"
        )
        json_token = (
            "{'TOKEN': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFt"
            "ZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c'}"
        )

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "Token =" + long_token
        )
        assert masked
        assert err_str is None
        assert masked_text == "Token =****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "idToken : " + long_token
        )
        assert masked
        assert err_str is None
        assert masked_text == "idToken : ****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "sessionToken : " + long_token
        )
        assert masked
        assert err_str is None
        assert masked_text == "sessionToken : ****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "masterToken : " + long_token
        )
        assert masked
        assert err_str is None
        assert masked_text == "masterToken : ****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "assertion content:" + long_token
        )
        assert masked
        assert err_str is None
        assert masked_text == "assertion content:****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(json_token)
        assert masked
        assert err_str is None
        assert masked_text == "{'TOKEN': '****'}"

    def test_token_false_positives(self):
        false_positive_token_str = (
            "2020-04-30 23:06:04,069 - MainThread auth.py:397"
            " - write_temporary_credential() - DEBUG - no ID "
            "token is given when try to store temporary credential"
        )
        _assert_not_masked(false_positive_token_str)

    def test_multiple_secrets_are_all_masked(self):
        long_token = (
            "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U"
            "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj"
            "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F"
            "mClCMBSissVsU3Ei590FP0lPQQhcSGcD"
            "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU="
            "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj"
            "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR"
            "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5"
            "XdJYuI8vhg=f0bKSq7AhQ2Bh"
        )
        long_token2 = (
            "ktL57KJemuq4-M+Q0pdRjCIMcf1mzcr"
            "MwKteDS5DRE/Pb+5MzvWjDH7LFPV5b_"
            "/tX/yoLG3b4TuC6Q5qNzsARPPn_zs/j"
            "BbDOEg1-IfPpdsbwX6ETeEnhxkHIL4H"
            "sP-V"
        )
        random_pwd = "Fh[+2J~AcqeqW%?"
        random_pwd2 = random_pwd + "vdkav13"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "token=" + long_token + " random giberish " + "password:" + random_pwd
        )
        assert masked
        assert err_str is None
        assert masked_text == "token=****" + " random giberish " + "password:****"

        # order reversed
        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "password:" + random_pwd + " random giberish " + "token=" + long_token
        )
        assert masked
        assert err_str is None
        assert masked_text == "password:****" + " random giberish " + "token=****"

        # multiple tokens and password
        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "token="
            + long_token
            + " random giberish "
            + "password:"
            + random_pwd
            + " random giberish "
            + "idToken:"
            + long_token2
        )
        assert masked
        assert err_str is None
        assert (
            masked_text
            == "token=****"
            + " random giberish "
            + "password:****"
            + " random giberish "
            + "idToken:****"
        )

        # multiple passwords
        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "password=" + random_pwd + " random giberish " + "pwd:" + random_pwd2
        )
        assert masked
        assert err_str is None
        assert masked_text == "password=****" + " random giberish " + "pwd:****"

        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "password="
            + random_pwd
            + " random giberish "
            + "password="
            + random_pwd2
            + " random giberish "
            + "password="
            + random_pwd
        )
        assert masked
        assert err_str is None
        assert (
            masked_text
            == "password=****"
            + " random giberish "
            + "password=****"
            + " random giberish "
            + "password=****"
        )

    def test_private_key_body_is_masked(self):
        rsa_key = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEA0pCa0rw1n4GBjylx\n"
            "sBJPVCrsKO7SowkgJ52Lc8K3hMHNKXvYiqwgizbXFBQA27kvpEVSeRQVC3FAPRU5\n"
            "gjtLRwIDAQABAkBHZbz5o9PS6AjUUEs6VpsLgRpersxBeACtLiBw+h9cJfUerR//\n"
            "tTmNsQ9LlamMu2lOlfbO3R2J45ybF7z94A+hAiEA8piucvAlo9YJ4VViQGRTVvr+\n"
            "xZKekSEYRJBn2czeP+kCIQDeMt1PVk/p0NEcNvQMbO0vJ3+U+lITJRwmtJ9Fs1Lj\n"
            "rwIgJeTdkwyaBI6BepY4w7AoKHUKaNgvNqJBxSv9XNMYgEkCIG2rl1YgWOMkAQI3\n"
            "EW/Ml6jtiugiQT5X07Q69F33q5LbAiEArZM7htafpt0RVia+nC9aY+73wpW0Be9e\n"
            "pDz0yVv8s/Q=\n"
            "-----END RSA PRIVATE KEY-----\n"
        )
        masked, masked_text, err_str = SecretDetector.mask_secrets(rsa_key)
        assert masked
        assert err_str is None
        assert (
            masked_text
            == "-----BEGIN PRIVATE KEY-----\\nXXXX\\n-----END PRIVATE KEY-----\n"
        )

    def test_private_key_data_is_masked(self):
        text = '"privateKeyData": "aslkjdflasjf"'
        filtered_text = '"privateKeyData": "XXXX"'
        _, result, _ = SecretDetector.mask_secrets(text)
        assert result == filtered_text

    def test_session_token_wire_format_is_masked(self):
        """``CONNECTION_TOKEN_PATTERN``'s value class includes ':' and '%' so a
        version/hint-prefixed session token -- Snowflake's actual wire format --
        masks in full instead of stopping at the first ':'; matches legacy
        Node.js's fix for the same gap."""
        masked, masked_text, err_str = SecretDetector.mask_secrets(
            "token=ver:1-hint:1036-abcd1234efgh5678"
        )
        assert masked
        assert err_str is None
        assert masked_text == "token=****"

    @pytest.mark.parametrize(
        "text,expected",
        [
            ('"access_token" : "some:FAKE_token123"', '"access_token":"XXXX"'),
            ('"refresh_token" : "some:FAKE_token123"', '"refresh_token":"XXXX"'),
        ],
    )
    def test_oauth_tokens_are_masked(self, text, expected):
        """``OAUTH_TOKEN_PATTERN`` has no legacy Python equivalent; ported from
        legacy JDBC's ``OAUTH_JSON_PATTERN`` / ``testMaskOAuthSecrets``."""
        masked, masked_text, err_str = SecretDetector.mask_secrets(text)
        assert masked
        assert err_str is None
        assert masked_text == expected

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("oauthClientSecret: aVeryLongSecretValue123", "oauthClientSecret: ****"),
            ("clientSecret=anotherLongSecretValue456", "clientSecret=****"),
        ],
    )
    def test_oauth_client_secrets_are_masked(self, text, expected):
        """``OAUTH_CLIENT_SECRET_PATTERN`` has no legacy Python equivalent; ported
        from legacy Node.js's ``OAUTH_CLIENT_SECRET_PATTERN``."""
        masked, masked_text, err_str = SecretDetector.mask_secrets(text)
        assert masked
        assert err_str is None
        assert masked_text == expected

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("passcode: 123456", "passcode:****"),
            ("otp=987654", "otp=****"),
            ("pin = 4321", "pin=****"),
        ],
    )
    def test_passcodes_are_masked(self, text, expected):
        """``PASSCODE_PATTERN`` has no legacy Python equivalent; ported from legacy
        Node.js's ``PASSCODE_PATTERN`` (covers passcode/otp/pin/otac, 4-6 digits)."""
        masked, masked_text, err_str = SecretDetector.mask_secrets(text)
        assert masked
        assert err_str is None
        assert masked_text == expected


class TestMaskSecretsExceptionHandling:
    """Masking failures must fail closed: the original (unmasked) text is never
    returned, even if a masker raises."""

    @mock.patch.object(
        SecretDetector,
        "mask_connection_token",
        mock.Mock(side_effect=Exception("Test exception")),
    )
    def test_exception_in_masking_is_fail_closed(self):
        test_str = "This string will raise an exception"
        masked, masked_text, err_str = SecretDetector.mask_secrets(test_str)
        assert masked
        assert err_str == "Test exception"
        assert masked_text == "Test exception"

    @staticmethod
    def _format_with_masking_exception():
        test_str = "This string will raise an exception"
        log_record = logging.LogRecord(
            SecretDetector.__name__,
            logging.DEBUG,
            "test_secret_detector.py",
            45,
            test_str,
            [],
            None,
        )
        log_record.asctime = "2003-07-08 16:49:45,896"
        sanitized_log = SecretDetector().format(log_record)
        assert "Test exception" in sanitized_log
        assert "secret_detector.py" in sanitized_log
        assert "sanitize_log_str" in sanitized_log
        assert test_str not in sanitized_log

    @mock.patch.object(
        SecretDetector,
        "mask_connection_token",
        mock.Mock(side_effect=Exception("Test exception")),
    )
    def test_exception_in_secret_detector_while_log_masking(self):
        self._format_with_masking_exception()

    @mock.patch.object(
        SecretDetector,
        "mask_secrets",
        mock.Mock(side_effect=Exception("Test exception")),
    )
    def test_exception_while_log_masking(self):
        self._format_with_masking_exception()


class TestFormatter:
    def test_format_sanitizes_log_record(self):
        formatter = SecretDetector("%(message)s")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="connecting with password=hunter2",
            args=None,
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "hunter2" not in formatted
        assert SecretDetector.SECRET_STARRED_MASK_STR in formatted

    def test_is_a_logging_formatter(self):
        assert issubclass(SecretDetector, logging.Formatter)
