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
python3 util.py snowflake.snowpark -c DataFrame DataFrameNaFunctions DataFrameStatFunctions
python3 util.py snowflake.snowpark.functions
```
            

To start from scratch and regenerate the autosummary files that are generated for each module, delete everything in the <root>/snowpark-python/docs/source/api. 
You might need to do this if you restructure your code or add new classes.
