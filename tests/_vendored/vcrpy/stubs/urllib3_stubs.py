"""Stubs for urllib3"""

# Urllib3 1.25.9 renamed VerifiedHTTPSConnection to HTTPSConnection
from urllib3._version import __version__ as urllib3_version

def urllib3_renamed_version() -> bool:
    """Helper function to patch code."""
    urllib3_maj, urllib3_min, urllib3_micro = tuple([int(part) for part in urllib3_version.split('.')])
    print(f'urllib3 version: {urllib3_version}')
    return (urllib3_min == 25 and urllib3_micro >= 9) or urllib3_min > 25


if urllib3_renamed_version():
    from urllib3.connectionpool import HTTPConnection
    from urllib3.connectionpool import HTTPSConnection as VerifiedHTTPSConnection
else:
    from urllib3.connectionpool import HTTPConnection, VerifiedHTTPSConnection

from ..stubs import VCRHTTPConnection, VCRHTTPSConnection

# urllib3 defines its own HTTPConnection classes. It includes some polyfills
# for newer features missing in older pythons.


class VCRRequestsHTTPConnection(VCRHTTPConnection, HTTPConnection):
    _baseclass = HTTPConnection


class VCRRequestsHTTPSConnection(VCRHTTPSConnection, VerifiedHTTPSConnection):
    _baseclass = VerifiedHTTPSConnection
