#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Behavioral tests for mock/_secret_detector.py, ported from the Universal
Driver's SecretDetector backward-compat shim test suite -- masking-behavior
assertions only, not the logging.Formatter/backward-compat-specific tests
that don't apply here (mask_secrets is a plain function, not a class).
"""
import random
import string
from unittest import mock

import pytest

from snowflake.snowpark.mock import _secret_detector
from snowflake.snowpark.mock._secret_detector import mask_secrets


def test_clean_text_is_not_masked():
    text = "select 1 from dual"
    assert mask_secrets(text) == text


def test_none_text_returns_none():
    assert mask_secrets(None) is None


def test_empty_string_is_not_masked():
    assert mask_secrets("") == ""


def test_password_is_masked():
    random_password = "Fh[+2J~AcqeqW%?"

    assert mask_secrets("password:" + random_password) == "password:****"
    assert mask_secrets("PASSWORD:" + random_password) == "PASSWORD:****"
    assert mask_secrets("PassWorD:" + random_password) == "PassWorD:****"
    assert mask_secrets("password = " + random_password) == "password = ****"
    assert mask_secrets("pwd:" + random_password) == "pwd:****"
    assert mask_secrets("password=Afs...") == "password=****"


@pytest.mark.parametrize(
    "text",
    [
        "2020-04-30 23:06:04,069 - MainThread auth.py:397"
        " - write_temporary_credential() - DEBUG - no ID password was not given",
        "2020-04-30 23:06:04,069 - MainThread auth.py:397"
        " - write_temporary_credential() - DEBUG - no ID proxyPassword was not given",
    ],
)
def test_password_false_positives_are_not_masked(text):
    """PASSWORD_PATTERN's value floor is 6 chars (not 1) so a short common word
    right after a bare password/pwd keyword isn't mistaken for the secret
    itself."""
    assert mask_secrets(text) == text


def test_aws_key_is_masked():
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
    assert mask_secrets(sql) == correct


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
def test_aws_tokens_are_masked(text, expected):
    assert mask_secrets(text) == expected


def test_sas_tokens_are_masked():
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

    assert mask_secrets(azure_sas_token) == masked_azure_sas_token
    assert mask_secrets(s3_sas_token) == masked_s3_sas_token

    text = "".join(random.choice(string.ascii_lowercase) for _ in range(200))
    assert mask_secrets(text) == text

    assert (
        mask_secrets(azure_sas_token + "\n" + azure_sas_token)
        == masked_azure_sas_token + "\n" + masked_azure_sas_token
    )
    assert (
        mask_secrets(s3_sas_token + "\n" + s3_sas_token)
        == masked_s3_sas_token + "\n" + masked_s3_sas_token
    )
    assert (
        mask_secrets(azure_sas_token + "\n" + s3_sas_token)
        == masked_azure_sas_token + "\n" + masked_s3_sas_token
    )


def test_combined_secrets_in_create_stage_sql():
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
    assert mask_secrets(sql) == masked_sql

    text = "".join(random.choice(string.ascii_lowercase) for _ in range(500))
    assert mask_secrets(text) == text


def test_connection_token_is_masked():
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

    assert mask_secrets("Token =" + long_token) == "Token =****"
    assert mask_secrets("idToken : " + long_token) == "idToken : ****"
    assert mask_secrets("sessionToken : " + long_token) == "sessionToken : ****"
    assert mask_secrets("masterToken : " + long_token) == "masterToken : ****"
    assert mask_secrets("assertion content:" + long_token) == "assertion content:****"
    assert mask_secrets(json_token) == "{'TOKEN': '****'}"


def test_token_false_positives():
    false_positive_token_str = (
        "2020-04-30 23:06:04,069 - MainThread auth.py:397"
        " - write_temporary_credential() - DEBUG - no ID "
        "token is given when try to store temporary credential"
    )
    assert mask_secrets(false_positive_token_str) == false_positive_token_str


def test_multiple_secrets_are_all_masked():
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

    assert (
        mask_secrets(
            "token=" + long_token + " random giberish " + "password:" + random_pwd
        )
        == "token=****" + " random giberish " + "password:****"
    )

    # order reversed
    assert (
        mask_secrets(
            "password:" + random_pwd + " random giberish " + "token=" + long_token
        )
        == "password:****" + " random giberish " + "token=****"
    )

    # multiple tokens and password
    assert (
        mask_secrets(
            "token="
            + long_token
            + " random giberish "
            + "password:"
            + random_pwd
            + " random giberish "
            + "idToken:"
            + long_token2
        )
        == "token=****"
        + " random giberish "
        + "password:****"
        + " random giberish "
        + "idToken:****"
    )

    # multiple passwords
    assert (
        mask_secrets(
            "password=" + random_pwd + " random giberish " + "pwd:" + random_pwd2
        )
        == "password=****" + " random giberish " + "pwd:****"
    )

    assert (
        mask_secrets(
            "password="
            + random_pwd
            + " random giberish "
            + "password="
            + random_pwd2
            + " random giberish "
            + "password="
            + random_pwd
        )
        == "password=****"
        + " random giberish "
        + "password=****"
        + " random giberish "
        + "password=****"
    )


def test_private_key_body_is_masked():
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
    assert (
        mask_secrets(rsa_key)
        == "-----BEGIN PRIVATE KEY-----\\nXXXX\\n-----END PRIVATE KEY-----\n"
    )


def test_private_key_data_is_masked():
    text = '"privateKeyData": "aslkjdflasjf"'
    filtered_text = '"privateKeyData": "XXXX"'
    assert mask_secrets(text) == filtered_text


def test_session_token_wire_format_is_masked():
    """CONNECTION_TOKEN_PATTERN's value class includes ':' and '%' so a
    version/hint-prefixed session token -- Snowflake's actual wire format --
    masks in full instead of stopping at the first ':'."""
    assert mask_secrets("token=ver:1-hint:1036-abcd1234efgh5678") == "token=****"


@pytest.mark.parametrize(
    "text,expected",
    [
        ('"access_token" : "some:FAKE_token123"', '"access_token":"XXXX"'),
        ('"refresh_token" : "some:FAKE_token123"', '"refresh_token":"XXXX"'),
    ],
)
def test_oauth_tokens_are_masked(text, expected):
    assert mask_secrets(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("oauthClientSecret: aVeryLongSecretValue123", "oauthClientSecret: ****"),
        ("clientSecret=anotherLongSecretValue456", "clientSecret=****"),
    ],
)
def test_oauth_client_secrets_are_masked(text, expected):
    assert mask_secrets(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("passcode: 123456", "passcode:****"),
        ("otp=987654", "otp=****"),
        ("pin = 4321", "pin=****"),
    ],
)
def test_passcodes_are_masked(text, expected):
    assert mask_secrets(text) == expected


def test_masking_failure_returns_static_sentinel_not_exception_text():
    """Masking failures must fail closed: the exception message (which may
    itself reflect secret-containing input) is never returned -- only the
    static sentinel."""
    with mock.patch.object(
        _secret_detector,
        "_mask_connection_token",
        side_effect=Exception("Test exception, would leak: password=hunter2"),
    ):
        result = mask_secrets("some text")
    assert result == "****"
    assert "hunter2" not in result
    assert "Test exception" not in result
