#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""Modular houses default to pandas implementation with snowflake stored procedure"""

import logging
import os
from collections import namedtuple
from enum import Enum
from typing import Any, Callable, Union

import cloudpickle as pickle
import pandas as native_pd

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    is_in_stored_procedure,
    random_name_for_temp_object,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    get_default_snowpark_pandas_statement_params,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.session import Session
from snowflake.snowpark.udf import UserDefinedFunction

_logger = logging.getLogger(__name__)

# Because Snowpark pandas API is not public and not part of Snowpark, it has to
# be uploaded to a stage as the dependency of fallback stored procedure
# We don't need it once Snowpark pandas API is merged to snowpark
# current path of this file:
# `src/snowflake/snowpark/modin/plugin/default2pandas/stored_procedure_utils.py`
# So we need to be back to `snowflake/snowpark`
# We can't go back to `snowflake/` directly, because `snowflake/` directory also contain
# `connector` directory, which can't be used inside stored proc (inside the stored proc, we
# use stored proc connector)
SNOWPARK_PANDAS_IMPORT: Union[str, tuple[str, str]] = (
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "snowflake.snowpark",
)
# currently users can only upload a zip file to use Snowpark pandas inside the stored proc
# so the current path of this file:
# snowflake.zip/snowflake/snowpark/modin/plugin/default2pandas/stored_procedure_utils.py
# However, `snowflake.zip/snowflake` doesn't exist because Python UDFs use zipimport to import
# the dependency so this file is not unzipped. Therefore, we can use `snowflake.zip` directly
if is_in_stored_procedure():
    SNOWPARK_PANDAS_IMPORT = os.path.dirname(
        os.path.dirname(SNOWPARK_PANDAS_IMPORT[0])
    )  # pragma: no cover

# this requirement comes from modin:
# https://github.com/modin-project/modin/blob/7c835a2761ede41d402c1febd29826c1d0b9512f/requirements/requirements-no-engine.yml#L10
# As of 2024/03/19, the Snowflake conda channel doesn't have the latest version
# of packaging, 24.0, but Modin 0.26.1, the version of Modin we are trying to
# integrate with, specifies packaging>=21.0 with no upper bound. Users are
# likely to install packaging 24.0, which would not be available when running
# the stored procedure. Instead of using exactly the version of packaging that
# we have locally, use the latest compatible version that's available in the
# Snowflake conda channel.
# TODO(SNOW-1254730): Once we finish integrating with Snowpark,
# the only dependency we will need is snowpark python with the
# Snowpark pandas extra, i.e. something like
# snowflake-snowpark-python[snowpark-pandas]. That dependency will depend on
# modin, and modin will transitively depend on third party pacakages like
# `packaging`.
PACKAGING_REQUIREMENT = "packaging>=21.0"

MODIN_REQUIREMENT = "modin==0.28.1"

FALLBACK_TAG = "FALLBACK"


class SnowparkPandasObjectType(Enum):
    """
    Enum for supported Snowpark pandas object types.

    Those are Snowpark pandas object that can be used as caller, args, keywords for
    the pandas operation. The caller is typically SnowflakeQueryCompiler; the
    Snowpark pandas object that can occur in args or keywords can be SnowflakeQueryCompiler,
    Snowpark pandas DataFrame, and Snowpark pandas Series. There is currently no need to support other
    Snowpark pandas object like DataFrameGroupBy etc.
    """

    QUERY_COMPILER = 1  # SnowflakeQueryCompiler
    DATAFRAME = 2  # Snowpark pandas DataFrame
    SERIES = 3  # Snowpark pandas Series


# namedtuple for the stored procedure pickle data for Snowpark pandas object, which is used to
# restore the Snowpark pandas object within or out of stored procedure
SnowparkPandasObjectPickleData = namedtuple(
    "SnowparkPandasObjectPickleData",
    [
        "object_type",  # type name for the Snowpark pandas object
        "table_name",  # the temp table created out of the snowpark dataframe for the Snowpark pandas object
        # internal frame properties
        "data_column_pandas_labels",
        "data_column_pandas_index_names",
        "data_column_snowflake_quoted_identifiers",
        "index_column_pandas_labels",
        "index_column_snowflake_quoted_identifiers",
        "ordering_columns",
        "row_position_snowflake_quoted_identifier",
    ],
)


class StoredProcedureDefault:
    @classmethod
    def _stage_and_extract_pickle_data(
        cls, object_type: SnowparkPandasObjectType, internal_frame: InternalFrame
    ) -> SnowparkPandasObjectPickleData:
        """
        Extract the pickle data for the internal frame and save the underlying Snowpark dataframe into
        a temporary table. The pickle data contains the temporary table name, object type name and
        properties of the internal frame.

        Args:
            object_type: SnowparkPandasObjectType. The object type for the Snowpark pandas object type.
            internal_frame: InternalFrame. The internal frame representation of the Snowpark pandas object.

        Returns:
            SnowparkPandasObjectPickleData, the pickle data for the Snowpark pandas object.
        """
        # save the current snowpark df as a snowflake temporary table
        temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
        internal_frame.ordered_dataframe.write.save_as_table(
            temp_table_name,
            table_type="temporary",
            statement_params=get_default_snowpark_pandas_statement_params(),
        )

        # data structure used to pass data in or out of stored proc
        pickle_data = SnowparkPandasObjectPickleData(
            object_type=object_type.name,
            table_name=temp_table_name,
            data_column_pandas_labels=internal_frame.data_column_pandas_labels,
            data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=internal_frame.data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=internal_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
            ordering_columns=internal_frame.ordering_columns,
            row_position_snowflake_quoted_identifier=internal_frame.row_position_snowflake_quoted_identifier,
        )
        return pickle_data

    @classmethod
    def _recover_snowpark_pandas_object(
        cls, session: Session, pickle_data: SnowparkPandasObjectPickleData
    ) -> Union["DataFrame", "Series", "SnowflakeQueryCompiler"]:  # type: ignore[name-defined] # noqa: F821
        """
        Recover the Snowpark pandas object based on the SnowparkPandasObjectPickleData.

        Returns:
            Recovered Snowpark pandas object, it can be a Snowpark pandas DataFrame, Series
            or SnowflakeQueryCompiler based on the object type.
        """
        from snowflake.snowpark.modin.pandas import DataFrame, Series
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        # create snowpark dataframe from a snowflake table
        ordered_dataframe = OrderedDataFrame(
            DataFrameReference(session.table(pickle_data.table_name)),
            ordering_columns=pickle_data.ordering_columns,
            row_position_snowflake_quoted_identifier=pickle_data.row_position_snowflake_quoted_identifier,
        )
        # create the internal frame representation based on the snowpark dataframe and pickle data
        internal_frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            data_column_pandas_labels=pickle_data.data_column_pandas_labels,
            data_column_pandas_index_names=pickle_data.data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=pickle_data.data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=pickle_data.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=pickle_data.index_column_snowflake_quoted_identifiers,
        )
        # create a snowflake query compiler
        query_compiler = SnowflakeQueryCompiler(internal_frame)

        # recreate the Snowpark pandas object based on the object type
        if pickle_data.object_type == SnowparkPandasObjectType.DATAFRAME.name:
            result = DataFrame(query_compiler=query_compiler)
        elif pickle_data.object_type == SnowparkPandasObjectType.SERIES.name:
            result = Series(query_compiler=query_compiler)
        else:
            result = query_compiler

        return result

    @classmethod
    def _try_pickle_snowpark_pandas_objects(
        cls,
        obj: Any,
    ) -> tuple[Any, dict[str, SnowparkPandasObjectPickleData]]:
        """
        Try to extract Snowpark pandas pickle data from `obj` and all nested objects, and replace
        the object with the name of the temporary table created for the Snowpark pandas object.

        If the object is not a Snowpark pandas object, return the original `obj`.

        Args:
            obj: object for extracting the pickle data.

        Returns:
            the new object converted after pickle data is extracted
            mapping between the temp table name and the corresponding pickle data

        """
        from snowflake.snowpark.modin.pandas import DataFrame, Series
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        if isinstance(obj, SnowflakeQueryCompiler):
            # the frontend typically process the Snowpark pandas object in arguments to
            # SnowflakeQueryCompiler. For example: df1.add(df2), the df2 will be processed
            # to df2._query_compiler when calling into SnowflakeQueryCompiler
            pickle_data = cls._stage_and_extract_pickle_data(
                SnowparkPandasObjectType.QUERY_COMPILER, obj._modin_frame
            )
            return pickle_data.table_name, {pickle_data.table_name: pickle_data}
        if isinstance(obj, DataFrame):
            pickle_data = cls._stage_and_extract_pickle_data(
                SnowparkPandasObjectType.DATAFRAME,
                obj._query_compiler._modin_frame,
            )
            return pickle_data.table_name, {pickle_data.table_name: pickle_data}
        if isinstance(obj, Series):
            pickle_data = cls._stage_and_extract_pickle_data(
                SnowparkPandasObjectType.SERIES, obj._query_compiler._modin_frame
            )
            return pickle_data.table_name, {pickle_data.table_name: pickle_data}
        if isinstance(obj, (list, tuple)):
            obj_list = []
            pickle_data_dict = {}
            for o in obj:  # process each element of the list or tuple
                (new_obj, pickle_datas) = cls._try_pickle_snowpark_pandas_objects(o)
                obj_list.append(new_obj)
                pickle_data_dict.update(pickle_datas)

            return type(obj)(obj_list), pickle_data_dict
        if isinstance(obj, dict):
            key_dict = {}
            pickle_data_dict = {}
            for k, v in obj.items():  # process each entry in the dict
                (new_obj, pickle_datas) = cls._try_pickle_snowpark_pandas_objects(v)
                key_dict[k] = new_obj
                pickle_data_dict.update(pickle_datas)
            return key_dict, pickle_data_dict

        return obj, {}

    @classmethod
    def _try_recover_snowpark_pandas_objects(
        cls,
        session: Session,
        obj: Any,
        pickle_data_dict: dict[str, SnowparkPandasObjectPickleData],
    ) -> Any:
        """
        Try to recover the Snowpark pandas object from `obj` and all nested objects based on the
        pickle data mapping. The object is replaced with the recovered Snowpark pandas object, if
        there is no corresponding pickle data, the original object remains.

        Args:
            session: snowflake session used to connect with snowflake server.
            obj: the obj we want to perform the recover process on.
            pickle_data_dict: mapping between the object name and corresponding pickle data

        returns:
            new object after the recover process
        """
        if isinstance(obj, str):
            # if the object have a corresponding entry in the pickle map, recovers
            # the Snowpark pandas object, otherwise, return the original object
            if obj in pickle_data_dict:
                return cls._recover_snowpark_pandas_object(
                    session, pickle_data_dict[obj]
                )
            else:
                return obj
        if isinstance(obj, (list, tuple)):
            obj_list = []
            for o in obj:
                new_obj = cls._try_recover_snowpark_pandas_objects(
                    session, o, pickle_data_dict
                )
                obj_list.append(new_obj)

            return type(obj)(obj_list)
        if isinstance(obj, dict):
            key_dict = {}
            for k, v in obj.items():
                new_obj = cls._try_recover_snowpark_pandas_objects(
                    session, v, pickle_data_dict
                )
                key_dict[k] = new_obj
            return key_dict

        return obj

    @classmethod
    def register(
        cls,
        frame: InternalFrame,
        pandas_op: Callable,
        args: Any,
        kwargs: Any,
    ) -> "SnowflakeQueryCompiler":  # type: ignore[name-defined]  # noqa: F821
        """
        Register and run pandas operation using stored procedure. Proper pre-processing and post-processing
        of Snowpark pandas object is applied on the caller object, input arguments and keyword mapping.

        Returns:
            SnowflakeQueryCompiler that is created out of the operation result. Note that all result of the
            pandas operation will be converted to dataframe format.

        """
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        def args_kwargs_str(args: Any, kwargs: Any) -> str:
            """
            Convert args and kwargs to a string representation.
            For a Snowpark pandas DataFrame/Series, we just retrieve its type name and
            avoid calling `str()` (which triggers query execution) directly.
            """

            def arg_str(arg: Any) -> str:
                if isinstance(arg, (list, tuple)):
                    return ",".join([arg_str(e) for e in arg])
                elif isinstance(arg, dict):
                    return ",".join(f"{k}: {arg_str(v)}" for k, v in arg.items())
                elif isinstance(arg, (pd.DataFrame, pd.Series)):
                    return str(type(arg))
                return str(arg)

            return f"args=({arg_str(args)}) and kwargs={{{arg_str(kwargs)}}}"

        _logger.debug(
            f"Default to (native) pandas fallback using stored_procedure for {pandas_op.__name__} "
            f"with {args_kwargs_str(args, kwargs)}"
        )
        WarningMessage.single_warning(
            f"Falling back to native pandas with a stored procedure for {pandas_op.__name__}. "
            + "Execution of this method could be slow. Please contact snowflake for complete "
            + "support of this feature."
        )

        # process caller object, caller object is always going to be snowflake query compiler
        caller_data = cls._stage_and_extract_pickle_data(
            SnowparkPandasObjectType.QUERY_COMPILER, internal_frame=frame
        )

        session: Session = pd.session

        # For DataFrame.apply, the function may be given as a Snowpark UserDefinedFunction. Extract
        # packages from it, and add to registration below.
        udf_packages = []
        if pandas_op.__name__ == "<function DataFrame.apply>":
            func = kwargs["func"]
            if isinstance(func, UserDefinedFunction):
                udf_packages = func._packages
                # Use underlying, non-decorated function within stored proc.
                kwargs["func"] = func.func

        (processed_args, args_pickle_data) = cls._try_pickle_snowpark_pandas_objects(
            args
        )
        (
            processed_kwargs,
            kwargs_pickle_data,
        ) = cls._try_pickle_snowpark_pandas_objects(kwargs)

        # default_to_pandas is a stored procedure that is executed within snowflake, which can not
        # be recognized by code coverage, mark all code of this function with pragma: no cover to
        # skip the code coverage check.
        def default_to_pandas(session: Session) -> bytes:
            # post process caller object
            caller_compiler = cls._recover_snowpark_pandas_object(
                session, caller_data
            )  # pragma: no cover
            # create pandas df from the compiler
            native_df = caller_compiler.to_pandas()  # pragma: no cover

            post_args = cls._try_recover_snowpark_pandas_objects(  # pragma: no cover
                session, processed_args, args_pickle_data
            )
            post_kwargs = cls._try_recover_snowpark_pandas_objects(  # pragma: no cover
                session, processed_kwargs, kwargs_pickle_data
            )

            # perform unsupported pandas.DataFrame operation
            df = pandas_op(native_df, *post_args, **post_kwargs)  # pragma: no cover

            # create a snowflake from a pandas df
            sf_compiler = SnowflakeQueryCompiler.from_pandas(df)  # pragma: no cover
            result_pickle_data = cls._stage_and_extract_pickle_data(  # pragma: no cover
                SnowparkPandasObjectType.QUERY_COMPILER, sf_compiler._modin_frame
            )

            return pickle.dumps(result_pickle_data)  # pragma: no cover

        sp_name = (
            f"{random_name_for_temp_object(TempObjectType.PROCEDURE)}{FALLBACK_TAG}"
        )

        if "snowflake-snowpark-python" not in session.get_packages():
            # need snowflake-snowpark-python to install required dependencies
            # e.g., pandas, stored procedure connector
            # we only need to add package once to skip the validation on the client side
            session.add_packages("snowflake-snowpark-python")

        packages = list(session.get_packages().values()) + udf_packages
        if "pandas" not in packages:
            # Use the current pandas version to ensure the behavior consistency
            packages = [native_pd] + packages

        if "packages" not in packages:
            packages.append(PACKAGING_REQUIREMENT)

        if "modin" not in packages:
            packages.append(MODIN_REQUIREMENT)

        # register stored procedure
        default_to_pandas_sp = session.sproc.register(
            default_to_pandas,
            name=sp_name,
            imports=[SNOWPARK_PANDAS_IMPORT],
            source_code_display=False,
            packages=packages,
            # TODO: SNOW-940730 Use anonymous stored procedure to avoid procedure creation
            #  once the server side bug is fixed
            statement_params=get_default_snowpark_pandas_statement_params(),
        )

        # call stored proc
        encoded_pickle_data = default_to_pandas_sp(
            statement_params=get_default_snowpark_pandas_statement_params()
        )
        pickle_data = pickle.loads(encoded_pickle_data)

        # clean up temp tables and stored proc
        table_to_drop = (
            [caller_data.table_name]
            + list(args_pickle_data.keys())
            + list(kwargs_pickle_data.keys())
        )
        for table_name in table_to_drop:
            session.sql(f"drop table if exists {quote_name(table_name)}").collect(
                statement_params=get_default_snowpark_pandas_statement_params()
            )
        session.sql(f"drop procedure if exists {sp_name}()").collect(
            statement_params=get_default_snowpark_pandas_statement_params()
        )

        return cls._recover_snowpark_pandas_object(session, pickle_data)
