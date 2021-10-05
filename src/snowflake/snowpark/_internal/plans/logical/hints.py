#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
# Hints,
# https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/hints.scala
class JoinHint:
    def __init__(self, left_hint, right_hint):
        self.left_hint = left_hint
        self.right_hint = right_hint

    @classmethod
    def none(cls):
        return cls(None, None)
