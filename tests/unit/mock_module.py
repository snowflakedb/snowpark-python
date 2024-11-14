#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


class Parent:
    def bar(self):
        return 1


class Child(Parent):
    def bar(self):
        return 1


def module_method(a, b, c, **kwargs):
    return a + b + c


module_var = 123
