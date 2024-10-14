# Setting up and Running Jupyter Notebook Tests

### Running Notebooks locally
To run the jupyter notebooks locally, ensure that CONNECTION_PARMETERS in tests/parameters.py is populated with 
the appropriate credentials. 

```python
CONNECTION_PARAMETERS = {
    'account': '<account>',
    'user': '<user>',
    'password': '<password>',
    'role': '<role>',
    'database': '<database>',
    'schema': '<schema>',
    'warehouse': '<warehouse>',
}
```
Make sure test requirements all installed
```python
pip install -r tests/notebooks/test_requirements.txt
```

Then run `jupyter notebook` in this repository and open the corresponding .ipynb files. 

### Running Notebooks automatically with pytest nbmake
All the notebooks can also be run automatically via 
`pytest -ra --nbmake --nbmake-timeout=1000 tests/notebooks/modin` 
which is how the daily notebook GitHub test is run.
