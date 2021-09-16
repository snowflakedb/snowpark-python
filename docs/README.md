# How to build the api documentation

Activate your virtual environment: `source venv/bin/activate`

Delete everything in the <root>/snowpark-python/docs/source/_autosummary directory

```make clean```

```make html```

If you get warnings, like "WARNING: [autosummary] failed to import 'snowpark.dataframe': no module named snowpark.dataframe", try reinstalling the Snowpark API:

```
python -m pip install ".[development, pandas]"
```

Open the documentation: `open -a "Google Chrome" build/html/index.html`