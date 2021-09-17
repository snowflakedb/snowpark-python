# How to build the api documentation

Activate your virtual environment: `source venv/bin/activate`

Go to the docs directory.

```bash
make clean
make html```

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

`docs/source/index.rst`: Specify which modules to generate an autosummary for. This also controls what shows up in the `index.html` landing page.
`docs/source/conf.py`: The configuration parameters for Sphinx and autosummary.
`docs/source/_templates/module.rst`: This is a Jinja template that controls how the info on each module doc page is ordered and laid out. You tell Sphinx to use this template in index.rst.
`docs/source/_themes/snowflake_rtd_theme/layout.html`: All of the theme stuff is the same as the snowflake_rtd_theme in the normal Snowflake doc repo. This layout file is the only thing I modified.


To start from scratch and regenerate the autosummary files that are generated for each module, delete everything in the <root>/snowpark-python/docs/source/_autosummary directory. 
You might need to do this if you restructure your code or add new classes.
