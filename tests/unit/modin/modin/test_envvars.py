#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

import os
import sys
import unittest.mock
import warnings

import modin.pandas as pd
import pandas
import pytest
from packaging import version

import snowflake.snowpark.modin.config as cfg
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.config import DocModule
from snowflake.snowpark.modin.config.envvars import _check_vars
from snowflake.snowpark.modin.config.pubsub import _UNSET, ExactStr


def reset_vars(*vars: tuple[cfg.Parameter]):
    """
    Reset value for the passed parameters.

    Parameters
    ----------
    *vars : tuple[Parameter]
    """
    for var in vars:
        var._value = _UNSET
        _ = os.environ.pop(var.varname, None)


@pytest.fixture
def make_unknown_env():
    varname = "MODIN_UNKNOWN"
    os.environ[varname] = "foo"
    yield varname
    del os.environ[varname]


@pytest.fixture(params=[str, ExactStr])
def make_custom_envvar(request):
    class CustomVar(cfg.EnvironmentVariable, type=request.param):
        """custom var"""

        default = 10
        varname = "MODIN_CUSTOM"
        choices = (1, 5, 10)

    return CustomVar


@pytest.fixture
def set_custom_envvar(make_custom_envvar):
    os.environ[make_custom_envvar.varname] = "  custom  "
    yield "Custom" if make_custom_envvar.type is str else "  custom  "
    del os.environ[make_custom_envvar.varname]


def test_unknown(make_unknown_env):
    with pytest.warns(UserWarning, match=f"Found unknown .*{make_unknown_env}.*"):
        _check_vars()


def test_custom_default(make_custom_envvar):
    assert make_custom_envvar.get() == 10


def test_custom_set(make_custom_envvar, set_custom_envvar):
    assert make_custom_envvar.get() == set_custom_envvar


def test_custom_help(make_custom_envvar):
    assert "MODIN_CUSTOM" in make_custom_envvar.get_help()
    assert "custom var" in make_custom_envvar.get_help()


def _init_doc_module():
    # Put the docs_module on the path
    sys.path.append(f"{os.path.dirname(__file__)}")
    # We use base.py from upstream modin, so we need to initialize its doc module
    # However, since using the environment variable causes an importlib.reload call,
    # we need to manually call _inherit_docstrings (https://github.com/modin-project/modin/issues/7138)
    from .docs_module import classes

    # As a workaround for upstream modin bugs, we use our own _inherit_docstrings instead of the upstream
    # function. We accordingly need to clear the docstring dictionary in testing because
    # we manually called the annotation on initializing snowflake.snowpark.modin.plugin.
    # snowflake.snowpark.modin.utils._attributes_with_docstrings_replaced.clear()
    # TODO: once modin 0.31.0 is available, use the actual modin DocModule class
    snowflake.snowpark.modin.utils._inherit_docstrings(
        classes.BasePandasDataset,
        overwrite_existing=True,
    )(pd.base.BasePandasDataset)
    DocModule.put("docs_module")


DOC_OVERRIDE_XFAIL_REASON = (
    "test docstring overrides currently cannot override real docstring overrides until "
    "modin 0.31.0 is available"
)


class TestDocModule:
    """
    Test using a module to replace default docstrings.

    TODO: this class tests fixes for
    https://github.com/modin-project/modin/issues/7138,
    which we need to fix in upstream modin.
    """

    @pytest.mark.xfail(strict=True, reason=DOC_OVERRIDE_XFAIL_REASON)
    def test_overrides(self):
        _init_doc_module()
        # Test for override
        # TODO(https://github.com/modin-project/modin/issues/7134): Upstream
        # the BasePandasDataset tests to modin.
        assert pd.base.BasePandasDataset.__doc__ == (
            "This is a test of the documentation module for BasePandasDataSet."
        )
        assert (
            pd.DataFrame.apply.__doc__
            == "This is a test of the documentation module for DataFrame."
        )
        assert pd.base.BasePandasDataset.apply.__doc__ == (
            "This is a test of the documentation module for BasePandasDataSet.apply."
        )
        # Test scenario 2 from https://github.com/modin-project/modin/issues/7113:
        # We can correctly override the docstring for BasePandasDataset.astype,
        # which is the same method as Series.astype.
        # TODO(https://github.com/modin-project/modin/issues/7113): Upstream
        # this change to modin.
        assert (
            pd.Series.astype.__wrapped__ is pd.base.BasePandasDataset.astype.__wrapped__
        )
        assert pd.base.BasePandasDataset.astype.__doc__ == (
            "This is a test of the documentation module for BasePandasDataSet.astype."
        )
        # Test for pandas doc when method is not defined on the plugin module
        assert pandas.DataFrame.isna.__doc__ in pd.DataFrame.isna.__doc__
        assert pandas.DataFrame.isnull.__doc__ in pd.DataFrame.isnull.__doc__
        # Test for override
        assert (
            pd.Series.isna.__doc__
            == "This is a test of the documentation module for Series."
        )
        # Test for pandas doc when method is not defined on the plugin module
        assert pandas.Series.isnull.__doc__ in pd.Series.isnull.__doc__
        assert pandas.Series.apply.__doc__ in pd.Series.apply.__doc__
        # Test for override
        assert pd.read_csv.__doc__ == "Test override for functions on the module."
        # Test for pandas doc when function is not defined on module.
        assert pandas.read_table.__doc__ in pd.read_table.__doc__

    @pytest.mark.xfail(strict=True, reason=DOC_OVERRIDE_XFAIL_REASON)
    def test_not_redefining_classes_modin_issue_7138(self):
        original_dataframe_class = pd.DataFrame
        _init_doc_module()
        # Test for override
        assert (
            pd.DataFrame.apply.__doc__
            == "This is a test of the documentation module for DataFrame."
        )

        assert pd.DataFrame is original_dataframe_class

    @pytest.mark.xfail(strict=True, reason=DOC_OVERRIDE_XFAIL_REASON)
    def test_base_docstring_override_with_no_dataframe_or_series_class_modin_issue_7113(
        self,
    ):
        # TODO(https://github.com/modin-project/modin/issues/7113): Upstream
        # this test case to Modin. This test case tests scenario 1 from issue 7113.
        _init_doc_module()
        assert pd.base.BasePandasDataset.astype.__doc__ == (
            "This is a test of the documentation module for BasePandasDataSet.astype."
        )

    @pytest.mark.xfail(strict=True, reason=DOC_OVERRIDE_XFAIL_REASON)
    def test_base_property_not_overridden_in_either_subclass_modin_issue_7113(self):
        _init_doc_module()
        assert (
            pd.base.BasePandasDataset.loc.__doc__
            == "This is a test of the documentation module for BasePandasDataset.loc."
        )
        assert pd.DataFrame.loc.__doc__ == pd.base.BasePandasDataset.loc.__doc__
        assert pd.Series.loc.__doc__ == pd.base.BasePandasDataset.loc.__doc__


@pytest.mark.skip(reason="Not testing HDK")
def test_hdk_envvar():
    try:
        import pyhdk

        defaults = cfg.HdkLaunchParameters.get()
        assert defaults["enable_union"] == 1
        if version.parse(pyhdk.__version__) >= version.parse("0.6.1"):
            assert defaults["log_dir"] == "pyhdk_log"
        del cfg.HdkLaunchParameters._value
    except ImportError:
        # This test is intended to check pyhdk internals. If pyhdk is not available, skip the version check test.
        pass

    os.environ[cfg.HdkLaunchParameters.varname] = "enable_union=2,enable_thrift_logs=3"
    params = cfg.HdkLaunchParameters.get()
    assert params["enable_union"] == 2
    assert params["enable_thrift_logs"] == 3

    os.environ[cfg.HdkLaunchParameters.varname] = "unsupported=X"
    del cfg.HdkLaunchParameters._value
    params = cfg.HdkLaunchParameters.get()
    assert params["unsupported"] == "X"
    try:
        import pyhdk

        pyhdk.buildConfig(**cfg.HdkLaunchParameters.get())
    except RuntimeError as e:
        assert str(e) == "unrecognised option '--unsupported'"
    except ImportError:
        # This test is intended to check pyhdk internals. If pyhdk is not available, skip the version check test.
        pass

    os.environ[
        cfg.HdkLaunchParameters.varname
    ] = "enable_union=4,enable_thrift_logs=5,enable_lazy_dict_materialization=6"
    del cfg.HdkLaunchParameters._value
    params = cfg.HdkLaunchParameters.get()
    assert params["enable_union"] == 4
    assert params["enable_thrift_logs"] == 5
    assert params["enable_lazy_dict_materialization"] == 6


@pytest.mark.parametrize(
    "deprecated_var, new_var",
    [
        (cfg.ExperimentalGroupbyImpl, cfg.RangePartitioningGroupby),
        (cfg.ExperimentalNumPyAPI, cfg.ModinNumpy),
    ],
)
def test_deprecated_bool_vars_warnings(deprecated_var, new_var):
    """Test that deprecated parameters are raising `FutureWarnings` and their replacements don't."""
    old_depr_val = deprecated_var.get()
    old_new_var = new_var.get()

    try:
        reset_vars(deprecated_var, new_var)
        with pytest.warns(FutureWarning):
            deprecated_var.get()

        with pytest.warns(FutureWarning):
            deprecated_var.put(False)

        with unittest.mock.patch.dict(os.environ, {deprecated_var.varname: "1"}):
            with pytest.warns(FutureWarning):
                _check_vars()

        # check that the new var doesn't raise any warnings
        reset_vars(deprecated_var, new_var)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            new_var.get()
            new_var.put(False)
            with unittest.mock.patch.dict(os.environ, {new_var.varname: "1"}):
                _check_vars()
    finally:
        deprecated_var.put(old_depr_val)
        new_var.put(old_new_var)


@pytest.mark.parametrize(
    "deprecated_var, new_var",
    [
        (cfg.ExperimentalGroupbyImpl, cfg.RangePartitioningGroupby),
        (cfg.ExperimentalNumPyAPI, cfg.ModinNumpy),
    ],
)
@pytest.mark.parametrize("get_depr_first", [True, False])
def test_deprecated_bool_vars_equals(deprecated_var, new_var, get_depr_first):
    """
    Test that deprecated parameters always have values equal to the new replacement parameters.

    Parameters
    ----------
    deprecated_var : EnvironmentVariable
    new_var : EnvironmentVariable
    get_depr_first : bool
        Defines an order in which the ``.get()`` method should be called when comparing values.
        If ``True``: get deprecated value at first ``deprecated_var.get() == new_var.get() == value``.
        If ``False``: get new value at first ``new_var.get() == deprecated_var.get() == value``.
        The logic of the ``.get()`` method depends on which parameter was initialized first,
        that's why it's worth testing both cases.
    """
    old_depr_val = deprecated_var.get()
    old_new_var = new_var.get()

    def get_values():
        return (
            (deprecated_var.get(), new_var.get())
            if get_depr_first
            else (new_var.get(), deprecated_var.get())
        )

    try:
        # case1: initializing the value using 'deprecated_var'
        reset_vars(deprecated_var, new_var)
        deprecated_var.put(True)
        val1, val2 = get_values()
        assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

        new_var.put(False)
        val1, val2 = get_values()
        assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)

        new_var.put(True)
        val1, val2 = get_values()
        assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

        deprecated_var.put(False)
        val1, val2 = get_values()
        assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)

        # case2: initializing the value using 'new_var'
        reset_vars(deprecated_var, new_var)
        new_var.put(True)
        val1, val2 = get_values()
        assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

        deprecated_var.put(False)
        val1, val2 = get_values()
        assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)

        deprecated_var.put(True)
        val1, val2 = get_values()
        assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

        new_var.put(False)
        val1, val2 = get_values()
        assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)

        # case3: initializing the value using 'deprecated_var' with env variable
        reset_vars(deprecated_var, new_var)
        with unittest.mock.patch.dict(os.environ, {deprecated_var.varname: "True"}):
            val1, val2 = get_values()
            assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

            new_var.put(False)
            val1, val2 = get_values()
            assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)

            new_var.put(True)
            val1, val2 = get_values()
            assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

            deprecated_var.put(False)
            val1, val2 = get_values()
            assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)

        # case4: initializing the value using 'new_var' with env variable
        reset_vars(deprecated_var, new_var)
        with unittest.mock.patch.dict(os.environ, {new_var.varname: "True"}):
            val1, val2 = get_values()
            assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

            deprecated_var.put(False)
            val1, val2 = get_values()
            assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)

            deprecated_var.put(True)
            val1, val2 = get_values()
            assert val1 == val2 == True  # noqa: E712 ('obj == True' comparison)

            new_var.put(False)
            val1, val2 = get_values()
            assert val1 == val2 == False  # noqa: E712 ('obj == False' comparison)
    finally:
        deprecated_var.put(old_depr_val)
        new_var.put(old_new_var)
