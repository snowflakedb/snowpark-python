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
    def test_reset_id_gen_variant(self, fake_session):
        ast_batch = AstBatch(fake_session)
        # Simulate usage that changes _id_gen
        ast_batch.bind()
        ast_batch.bind()
        assert next(ast_batch._id_gen) != 1  # Ensure _id_gen has changed

        # Reset _id_gen and verify it is zero
        ast_batch.reset_id_gen()
        assert next(ast_batch._id_gen) == 1

    def test_assign(self, fake_session):
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

        # Ensure the statements list is reset
        assert ast_batch._request.body == []
