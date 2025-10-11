import functools
import pytest
from modin.config import AutoSwitchBackend

def hybrid_xskip(
    throws:type,
    reason:str=None
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if AutoSwitchBackend.get():
                skip_reason = f"Skipping when running under hybrid mode: {throws}: {reason}"
                pytest.mark.skip(reason=skip_reason)
            return func(*args, **kwargs)
            
        return wrapper
    return decorator