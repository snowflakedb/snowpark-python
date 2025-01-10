#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin.extensions.series_overrides import CategoryMethods


@pytest.mark.parametrize(
    "func",
    [
        lambda s: s.cat.categories(),
        lambda s: s.cat.categories(native_pd.Categorical([1, 2, 3, 3, 1])),
        lambda s: s.cat.ordered(),
        lambda s: s.cat.codes(),
        lambda s: s.cat.rename_categories(native_pd.Categorical([1, 2, 3, 3, 1])),
        lambda s: s.cat.rename_categories(
            native_pd.Categorical([1, 2, 3, 3, 1]), inplace=True
        ),
        lambda s: s.cat.reorder_categories(native_pd.Categorical([1, 2, 3, 3, 1])),
        lambda s: s.cat.reorder_categories(
            native_pd.Categorical([1, 2, 3, 3, 1]), ordered=[2, 1, 3]
        ),
        lambda s: s.cat.reorder_categories(
            native_pd.Categorical([1, 2, 3, 3, 1]), ordered=[2, 1, 3], inplace=True
        ),
        lambda s: s.cat.add_categories(native_pd.Categorical([1, 2, 3, 3, 1])),
        lambda s: s.cat.add_categories(
            native_pd.Categorical([1, 2, 3, 3, 1]), inplace=True
        ),
        lambda s: s.cat.remove_categories("A"),
        lambda s: s.cat.remove_categories("A", inplace=True),
        lambda s: s.cat.remove_unused_categories(),
        lambda s: s.cat.remove_unused_categories(inplace=True),
        lambda s: s.cat.set_categories(
            native_pd.Categorical([1, 2, 3, 3, 1]), ordered=[2, 1, 3]
        ),
        lambda s: s.cat.set_categories(
            native_pd.Categorical([1, 2, 3, 3, 1]),
            ordered=[2, 1, 3],
            rename=True,
            inplace=True,
        ),
        lambda s: s.cat.as_ordered(inplace=True),
        lambda s: s.cat.as_unordered(),
    ],
)
def test_cat_methods_raises(mock_series, func) -> None:
    with pytest.raises(
        NotImplementedError, match=CategoryMethods.category_not_supported_message
    ):
        func(mock_series)
