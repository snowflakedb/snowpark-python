===========================================
Interoperability with third party libraries
===========================================

Many third party libraries are interoperable with pandas, for example by accepting pandas dataframes objects as function
inputs. Here we have a non-exhaustive list of third party library use cases with pandas and note whether each method
works in Snowpark pandas as well.

Snowpark pandas supports the `dataframe interchange protocol <https://data-apis.org/dataframe-protocol/latest/>`_, which
some libraries use to interoperate with Snowpark pandas to the same level of support as pandas.

plotly.express
==============

The following table is structured as follows: The first column contains the name of a method in the ``plotly.express`` module.
The second column is a flag for whether or not interoperability is guaranteed with Snowpark pandas. For each of these
operations, we validate that passing in Snowpark pandas dataframes or series as the data inputs behaves equivalently
to passing in pandas dataframes or series.

.. note::
    ``Y`` stands for yes, i.e., interoperability is guaranteed with this method, and ``N`` stands for no.


.. note::
    Currently only plotly versions <6.0.0 are supported through the dataframe interchange protocol.

+-------------------------+---------------------------------------------+--------------------------------------------+
| Method name             | Interoperable with Snowpark pandas? (Y/N)   | Notes for current implementation           |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``scatter``             | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``line``                | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``area``                | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``timeline``            | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``violin``              | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``bar``                 | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``histogram``           | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``pie``                 | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``treemap``             | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``sunburst``            | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``icicle``              | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``scatter_matrix``      | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``funnel``              | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``density_heatmap``     | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``boxplot``             | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+
| ``imshow``              | Y                                           |                                            |
+-------------------------+---------------------------------------------+--------------------------------------------+


scikit-learn
============

We break down scikit-learn interoperability by categories of scikit-learn
operations.

For each category, we provide a table of interoperability with the following
structure: The first column describes a scikit-learn operation that may include
multiple method calls. The second column is a flag for whether or not
interoperability is guaranteed with Snowpark pandas. For each of these methods,
we validate that passing in Snowpark pandas objects behaves equivalently to
passing in pandas objects.

.. note::
    ``Y`` stands for yes, i.e., interoperability is guaranteed with this method, and ``N`` stands for no.

.. note::
    While some scikit-learn methods accept Snowpark pandas inputs, their
    performance with Snowpark pandas inputs is often much worse than their
    performance with native pandas inputs. Generally we recommend converting
    Snowpark pandas inputs to pandas with ``to_pandas()`` before passing them
    to scikit-learn.


Classification
--------------

+--------------------------------------------+---------------------------------------------+---------------------------------+
| Operation                                  | Interoperable with Snowpark pandas? (Y/N)   | Notes for current implementation|
+--------------------------------------------+---------------------------------------------+---------------------------------+
| Fitting a ``LinearDiscriminantAnalysis``   | Y                                           |                                 |
| classifier with the ``fit()`` method and   |                                             |                                 |
| classifying data with the ``predict()``    |                                             |                                 |
| method.                                    |                                             |                                 |
+--------------------------------------------+---------------------------------------------+---------------------------------+


Regression
----------

+--------------------------------------------+---------------------------------------------+---------------------------------+
| Operation                                  | Interoperable with Snowpark pandas? (Y/N)   | Notes for current implementation|
+--------------------------------------------+---------------------------------------------+---------------------------------+
| Fitting a ``LogisticRegression``  model    | Y                                           |                                 |
| with the ``fit()`` method and predicting   |                                             |                                 |
| results with the ``predict()`` method.     |                                             |                                 |
+--------------------------------------------+---------------------------------------------+---------------------------------+

Clustering
----------

+--------------------------------------------+---------------------------------------------+---------------------------------+
| Clustering method                          | Interoperable with Snowpark pandas? (Y/N)   | Notes for current implementation|
+--------------------------------------------+---------------------------------------------+---------------------------------+
| ``KMeans.fit()``                           | Y                                           |                                 |
+--------------------------------------------+---------------------------------------------+---------------------------------+


Dimensionality reduction
------------------------

+--------------------------------------------+---------------------------------------------+---------------------------------+
| Operation                                  | Interoperable with Snowpark pandas? (Y/N)   | Notes for current implementation|
+--------------------------------------------+---------------------------------------------+---------------------------------+
| Getting the principal components of a      | Y                                           |                                 |
| numerical dataset with ``PCA.fit()``.      |                                             |                                 |
+--------------------------------------------+---------------------------------------------+---------------------------------+


Model selection
------------------------

+--------------------------------------------+---------------------------------------------+-----------------------------------------------+
| Operation                                  | Interoperable with Snowpark pandas? (Y/N)   | Notes for current implementation              |
+--------------------------------------------+---------------------------------------------+-----------------------------------------------+
| Choosing parameters for a                  | Y                                           | ``RandomizedSearchCV`` causes Snowpark pandas |
| ``LogisticRegression`` model with          |                                             | to issue many queries. We strongly recommend  |
| ``RandomizedSearchCV.fit()``               |                                             | converting Snowpark pandas inputs to pandas   |
|                                            |                                             | before using ``RandomizedSearchCV``           |
+--------------------------------------------+---------------------------------------------+-----------------------------------------------+

Preprocessing
-------------

+--------------------------------------------+---------------------------------------------+-----------------------------------------------+
| Operation                                  | Interoperable with Snowpark pandas? (Y/N)   | Notes for current implementation              |
+--------------------------------------------+---------------------------------------------+-----------------------------------------------+
| Scaling training data with                 | Y                                           |                                               |
| ``MaxAbsScaler.fit_transform()``.          |                                             |                                               |
+--------------------------------------------+---------------------------------------------+-----------------------------------------------+
