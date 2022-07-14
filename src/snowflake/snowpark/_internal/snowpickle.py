#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import io
import sys
from _thread import LockType
from pickle import UnpicklingError
from typing import Any
from weakref import ReferenceType

import cloudpickle
from cloudpickle import CloudPickler

# IMPORTANT: this allows cloudpickle to pickle the internal method by value
cloudpickle.register_pickle_by_value(sys.modules[__name__])


class SnowPickler(CloudPickler):
    @staticmethod
    def _create_lock(locked, *args):
        from threading import Lock

        lock = Lock()
        if locked and not lock.acquire(False):
            raise UnpicklingError("Cannot acquire lock")
        return lock

    @staticmethod
    def _create_weakref(obj, *args):
        from weakref import ref

        if obj is None:
            from collections import UserDict

            return ref(UserDict(), *args)
        return ref(obj, *args)

    @staticmethod
    def _reduce_lock(create_lock_method, locked_status):
        return create_lock_method(locked_status)

    @staticmethod
    def _reduce_weakref(create_weakref_method, obj, *args):
        return create_weakref_method(obj, *args)

    def reducer_override(self, obj: Any) -> Any:
        if issubclass(type(obj), LockType):
            return SnowPickler._reduce_lock, (
                SnowPickler._create_lock,
                obj.locked(),
            )
        elif issubclass(type(obj), ReferenceType):
            return SnowPickler._reduce_weakref, (
                SnowPickler._create_weakref,
                obj(),
            )
        else:
            # fallback to CloudPickler
            return super().reducer_override(obj)


def dumps(obj, protocol=None):
    with io.BytesIO() as file:
        cp = SnowPickler(file, protocol=protocol)
        cp.dump(obj)
        return file.getvalue()
