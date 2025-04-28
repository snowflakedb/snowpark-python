#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.ast.batch import AstBatch


@pytest.fixture
def fake_session():
    yield mock.create_autospec(Session)


class TestAstBatch:
    def test_id_gen_variant(self, fake_session):
        ast_batch = AstBatch(fake_session)
        # Simulate usage that changes __id_gen
        bind1 = ast_batch.bind()
        bind2 = ast_batch.bind()
        assert bind2.uid == bind1.uid + 1
        ast_batch.flush()
        bind3 = ast_batch.bind()
        assert bind3.uid == bind1.uid + 2

    def test_bind(self, fake_session):
        ast_batch = AstBatch(fake_session)
        stmt = ast_batch.bind()
        assert stmt is not None

    def test_eval(self, fake_session):
        # Smoke test for AstBatch.eval()
        ast_batch = AstBatch(fake_session)
        ast_batch.eval(ast_batch.bind("foo"))

    def test_flush(self, fake_session):
        ast_batch = AstBatch(fake_session)

        # Call bind() and eval() a few times
        assign_stmt1 = ast_batch.bind("symbol1")
        assign_stmt2 = ast_batch.bind("symbol2")
        ast_batch.eval(assign_stmt1)
        ast_batch.eval(assign_stmt2)

        # Call flush() and test the outcome
        request_id, batch = ast_batch.flush()

        # Ensure the batch is not empty
        assert batch is not None
        assert request_id is not None

        # Ensure the batch specific attributes are reset
        assert ast_batch._cur_request_bind_id_q == []
        assert ast_batch._cur_request_bind_ids == set()
        assert ast_batch._eval_ids == set()
