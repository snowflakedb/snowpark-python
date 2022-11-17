# How to build the api documentation

Activate your virtual environment: `source venv/bin/activate`

Go to the docs directory.

```bash
make clean
make generate
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

Important files:
`docs/source/conf.py`: The configuration parameters for Sphinx and autosummary.
`docs/source/_themes/snowflake_rtd_theme/layout.html`: All of the theme stuff is the same as the snowflake_rtd_theme in the normal Snowflake doc repo. This layout file is the only thing I modified.


To start from scratch and regenerate the autosummary files that are generated for each module, delete everything in the <root>/snowpark-python/docs/source/api directory. 
You might need to do this if you restructure your code or add new classes.
