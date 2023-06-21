# How to build the api documentation

Activate your virtual environment: `source venv/bin/activate`

Go to the docs directory.

```bash
make clean
make html
```

If you get warnings, like "WARNING: [autosummary] failed to import 'snowpark.dataframe': no module named snowpark.dataframe", try reinstalling the Snowpark API:

```
python -m pip install ".[development, pandas]"
```

You might need to install Sphinx: 
```
python -m pip install sphinx
```

Open the documentation: `open -a "Google Chrome" build/html/index.html`

Important files and directories:

`docs/source/index.rst`: Specify which rst to include in the `index.html` landing page.
`docs/source/conf.py`: The configuration parameters for Sphinx and autosummary.
`docs/source/_templates/`: Directory containing JINJA templates used by autosummary.
`docs/source/_themes/snowflake_rtd_theme/layout.html`: All of the theme stuff is the same as the snowflake_rtd_theme in the normal Snowflake doc repo. This layout file is the only thing I modified.
`docs/source/util.py`: A utility script that generates a rst for a module or a list of classes within the same module. This should be used for reference only. 
Example usage: 
```
./doc_gen.py snowflake.snowpark -c DataFrame DataFrameNaFunctions DataFrameStatFunctions
./doc_gen.py snowflake.snowpark.functions
./doc_gen.py snowflake.snowpark.table_function -c TableFunctionCall -t "Table Function" -f "table_function.rst"
```
Note: You need to check what classes are present in a file by specifying the file as a module, e.g. `./doc_gen.py snowflake.snowpark.column`.
When you use this script to update the existing rst, note that:
- In `types.rst`, you need to manually move `Geography` and `Variant` from `Attributes` to `Classes`.
- Still in `types.rst`, change the rubric text to `Classes/Type Hints`.
- The script currently does not preserve any comments/literal markdown, remember to copy those over to the newly generated rst.

To start from scratch and regenerate the autosummary files that are generated for each module, delete everything in the <root>/snowpark-python/docs/source/api. 
You might need to do this if you restructure your code or add new classes.
