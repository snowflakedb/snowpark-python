NumPy Interoperability
======================

Snowpark pandas provides limited interoperability with NumPy functions through the NumPy
NEP18 and NEP13 specifications defined by `__array_ufunc__` and `__array_function__`. 
A discrete number of NumPy APIs are translated to distributed snowpark pandas functions.

+-----------------------------+----------------------------------------------------+
| NumPy method                | Notes for current implementation                   |
+-----------------------------+----------------------------------------------------+
| ``np.where``                | Mapped to np.where(cond, x, y) to x.where(cond, y) |
|                             | cond, x, and y should have the same shapes or be   |
|                             | scalars. The result is always a Snowpark pandas    |
|                             | DataFrame.                                         |
|                             |                                                    |
|                             | Since this function maps to df.where the           |
|                             | column and index labels are considered, as opposed |
|                             | strict positional indexing in NumPy.               |
|                             |                                                    |
|                             | cond, x, and y can either be all non-scalars or a  |
|                             | mix of scalars and non-scalars, such that          |
|                             | non-scalars have the same shape. (If cond, x, and  |
|                             | y are all scalars, NumPy will not call the         |
|                             | dispatcher at all, and the normal NumPy behavior   |
|                             | will occur.)                                       |
+-----------------------------+----------------------------------------------------+
| ``np.add``                  | Mapped to df.__add__(df2)                          |
+-----------------------------+----------------------------------------------------+
| ``np.logical_and``          | Mapped to df.__and__(df2)                          |
+-----------------------------+----------------------------------------------------+
| ``np.logical_or``           | Mapped to df.__or__(df2)                           |
+-----------------------------+----------------------------------------------------+
| ``np.logical_xor``          | Mapped to df.__xor__(df2)                          |
+-----------------------------+----------------------------------------------------+
| ``np.logical_not``          | Mapped to ~df.astype(bool)                         |
+-----------------------------+----------------------------------------------------+

NEP18 Implementation Details
----------------------------
NumPy differs from pandas and Snowflake pandas in several key respects. It is
important to understand that the interoperability provided is to support
common pandas use-cases, rather than matrix or linear algebra operations. NumPy
functions are mapped, with some transformation, to their pandas analogues.

Return Value
--------------------
NEP18 does not specify the return value when implementing a function like np.where,
but they suggest that the return value should match the input types. We follow
that suggestion here and return a Snowpark pandas DataFrame.

Broadcasting
------------
NumPy will "broadcast" all arguments into the same array shape so operations
can be vectorized on the CPU. Snowpark pandas should not do this because all
execution runs within Snowflake. All input DataFrames or Series should be of
the same shape and will not be broadcast. Scalar values can also be used as
an input.

Positional Operations
---------------------
NumPy always performs positional operations on input datatypes, assuming they
are similarly shaped and meaningful arrays. Pandas can have DataFrames which
represent the same data but with different column ordering. Even when a numpy
method is called on a Snow pandas DataFrame we continue to consider the labels
while performing the operation.

