Interoperability with third party libraries
=============================================

Many third party libraries are interoperable with pandas, for example by accepting pandas dataframes objects as function
inputs. Here we have a non-exhaustive list of third party library use cases with pandas and note whether each method
works in Snowpark pandas as well.

Snowpark pandas supports the `dataframe interchange protocol <https://data-apis.org/dataframe-protocol/latest/>`_, which
some libraries use to interoperate with Snowpark pandas to the same level of support as pandas.

The following table is structured as follows: The first column contains a method name.
The second column is a flag for whether or not interoperability is guaranteed with Snowpark pandas. For each of these
methods, we validate that passing in a Snowpark pandas dataframe as the dataframe input parameter behaves equivalently
to passing in a pandas dataframe.

.. note::
    ``Y`` stands for yes, i.e., interoperability is guaranteed with this method, and ``N`` stands for no.

Plotly.express module methods

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
