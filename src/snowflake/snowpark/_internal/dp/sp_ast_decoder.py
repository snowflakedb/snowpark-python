from typing import Any

import snowflake
import logging
import snowflake.snowpark._internal.proto.generated.ast_pb2 as ast
logger = logging.getLogger(__name__)


def column_expr(e):
    variant = e.WhichOneof('variant')
    if variant == 'sp_column_sql_expr':
        e = e.sp_column_sql_expr
        return e.sql
    else:
        logger.warning('Unexpected column expr %s', str(e))
        return None


class SnowparkAstDecoder:
    def __init__(self, session: snowflake.snowpark.Session):
        self._session = session
        self._session.ast_enabled = True
        self.bindings = {}

    def request(self, req: ast.Request) -> ast.Response:
        resp = ast.Response()

        for stmt in req.body:
            variant = stmt.WhichOneof('variant')
            if variant == 'assign':
                self.assign(stmt.assign)
            elif variant == 'eval':
                ans = self.eval(stmt.eval)

                # Fill into response evaluation result, this allows on the client-side to reconstruct
                # values with results.
                ok_result = ast.EvalOk(uid=stmt.eval.uid, var_id=stmt.eval.var_id, data=ans)
                res = ast.Result()
                res.eval_ok.CopyFrom(ok_result)
                resp.body.add().CopyFrom(res)
            else:
                logger.warning('Unexpected statement %s', str(stmt))
        logger.info('Session bindings %s', str(self.bindings))
        return resp

    def get_binding(self, var_id):
        # TODO: check if valid.
        return self.bindings[var_id.bitfield1]

    def assign(self, assign) -> None:
        val = self.expr(assign.expr)
        self.bindings[assign.var_id.bitfield1] = val

    def eval(self, eval) -> Any:
        res = self.get_binding(eval.var_id)
        logger.info('Return atom %s := %s', eval.var_id.bitfield1, str(res))
        return res

    def expr(self, e):
        variant = e.WhichOneof('variant')
        if variant == 'sp_dataframe_ref':
            e = e.sp_dataframe_ref
            return self.get_binding(e.id)
        elif variant == 'sp_sql':
            e = e.sp_sql
            return self._session.sql(e.query)
        elif variant == 'sp_table':
            e = e.sp_table
            return self._session.table(e.table)
        elif variant == 'sp_dataframe_filter':
            e = e.sp_dataframe_filter
            df = self.expr(e.df)
            condition = column_expr(e.condition)
            return df.filter(condition)
        elif variant == 'sp_dataframe_show':
            e = e.sp_dataframe_show
            df = self.get_binding(e.id)
            job = df.collect_nowait()
            return job.query_id
        elif variant == 'sp_dataframe_collect':
            e = e.sp_dataframe_collect
            df = self.get_binding(e.id)
            job = df.collect_nowait()
            return ast.EvalResult(sf_query_result=ast.SfQueryResult(uuid=job.query_id))
        else:
            logger.warning('Unexpected expr %s', str(e))
            return None

