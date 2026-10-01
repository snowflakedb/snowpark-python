===========================================
Interoperability with third party libraries
===========================================

Many third party libraries are interoperable with pandas, for example by accepting pandas dataframes objects as function
inputs. Here we have a non-exhaustive list of third party library use cases with pandas and note whether each method
works in Snowpark pandas as well.

Snowpark pandas supports the `dataframe interchange protocol <https://data-apis.org/dataframe-protocol/latest/>`_, which
some libraries use to interoperate with Snowpark pandas to the same level of support as pandas.

Scope of this reference
=======================

The tables below describe specific Plotly and scikit-learn operations, not a
complete compatibility certification for every version or API of a library.
Accepting a native pandas object does not by itself establish that an API accepts
a Snowpark pandas object or executes its work in Snowflake.

For a library or operation not covered here, such as an Altair, Seaborn,
Matplotlib, XGBoost, NLTK, or Streamlit operation, check that library's accepted
input types. You can explicitly convert a Snowpark pandas DataFrame or Series
with ``to_pandas()`` and then use APIs that accept native pandas objects.
Conversion materializes the data on the client; ensure the result fits in
memory. This is a local-pandas path, not a claim of direct Snowpark pandas
interoperability. For NumPy-specific behavior, see :doc:`numpy`.

plotly.express
==============

For each of the following methods in the ``plotly.express`` module, we validate that passing in Snowpark pandas
dataframes or series as the data inputs behaves equivalently to passing in pandas dataframes or series.

.. note::
    Currently only plotly versions <6.0.0 are supported through the dataframe interchange protocol.

+-------------------------+
| Method name             |
+-------------------------+
| ``scatter``             |
+-------------------------+
| ``line``                |
+-------------------------+
| ``area``                |
+-------------------------+
| ``timeline``            |
+-------------------------+
| ``violin``              |
+-------------------------+
| ``bar``                 |
+-------------------------+
| ``histogram``           |
+-------------------------+
| ``pie``                 |
+-------------------------+
| ``treemap``             |
+-------------------------+
| ``sunburst``            |
+-------------------------+
| ``icicle``              |
+-------------------------+
| ``scatter_matrix``      |
+-------------------------+
| ``funnel``              |
+-------------------------+
| ``density_heatmap``     |
+-------------------------+
| ``boxplot``             |
+-------------------------+
| ``imshow``              |
+-------------------------+


scikit-learn
============

We break down scikit-learn interoperability by categories of scikit-learn
operations.

For each category, we provide scikit-learn operations that may include
multiple method calls. For each of these methods, we validate that passing in Snowpark pandas objects behaves
equivalently to passing in pandas objects.


.. note::
    While some scikit-learn methods accept Snowpark pandas inputs, their
    performance with Snowpark pandas inputs is often much worse than their
    performance with native pandas inputs. Generally we recommend converting
    Snowpark pandas inputs to pandas with ``to_pandas()`` before passing them
    to scikit-learn.


Classification
--------------

+--------------------------------------------+
| Operation                                  |
+--------------------------------------------+
| Fitting a ``LinearDiscriminantAnalysis``   |
| classifier with the ``fit()`` method and   |
| classifying data with the ``predict()``    |
| method                                     |
+--------------------------------------------+


Regression
----------

+--------------------------------------------+
| Operation                                  |
+--------------------------------------------+
| Fitting a ``LogisticRegression``  model    |
| with the ``fit()`` method and predicting   |
| results with the ``predict()`` method      |
+--------------------------------------------+

Clustering
----------

+--------------------------------------------+
| Clustering method                          |
+--------------------------------------------+
| ``KMeans.fit()``                           |
+--------------------------------------------+


Dimensionality reduction
------------------------

+--------------------------------------------+
| Operation                                  |
+--------------------------------------------+
| Getting the principal components of a      |
| numerical dataset with ``PCA.fit()``       |
+--------------------------------------------+


Model selection
------------------------

+-------------------------------------------------+
| Operation                                       |
+-------------------------------------------------+
| Choosing parameters for a                       |
| ``LogisticRegression`` model with               |
| ``RandomizedSearchCV.fit()``                    |
+-------------------------------------------------+

.. note::
    ``RandomizedSearchCV`` causes Snowpark pandas to issue many queries. We strongly
    recommend converting Snowpark pandas inputs to pandas before using ``RandomizedSearchCV``.

Preprocessing
-------------

+--------------------------------------------+
| Operation                                  |
+--------------------------------------------+
| Scaling training data with                 |
| ``MaxAbsScaler.fit_transform()``           |
+--------------------------------------------+
