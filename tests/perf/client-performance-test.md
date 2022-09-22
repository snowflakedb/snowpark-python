# Setting up and Running Performance Tests

## Setting up
### Tools to use
1. [memory profiler](https://github.com/pythonprofilers/memory_profiler)
2. [cProfile](https://docs.python.org/3/library/profile.html), which is a Python built-in profiler
3. [SnakeViz](https://jiffyclub.github.io/snakeviz/) to graphically and interactively view the cProfile output

```commandline
$ pip install -U memory_profiler
$ pip install -U snakeviz
```

### Connection parameters

To connect to Snowflake, create a connection parameters file under `tests/parameters.py`, and add the
following code snippet with your parameters:
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

## Running Tests
Change directory to `<Snowflake Python Project Dir>/tests/performance_test`

### Use script to take a glance at the running time and memory used
1. Run `python perf_runner.py <api> <ncalls> -m -s`
    - `api` is a function defined in `perf_runner.py`.
    - `ncalls` is number of calls, which tells the test how many calls will be made to the tested DataFrame API.
    - `-m`, includes memory profiler.
    - `-s`, enables sql simplifier.
For instance, `python perf_runner.py with_column 10 -m -s`

### Use cProfile and snakeviz to view time spent in every function call.
1. create subfolder `results` in the working folder.
2. Run command like `python -m cProfile -s cumulative -o ./results/with_column_100.stats perf_runner.py with_column 10 -s`
3. Use snakeviz to view the output file
Refer to [cProfile](https://docs.python.org/3/library/profile.html) and [SnakeViz](https://jiffyclub.github.io/snakeviz/)

### Use mprof of memory profiler to view time-based memory usage.
1. For instance, `mprof run --python python perf_runner.py with_column 10 -s`
2. Run `mprof plot`
Refer to [mprof of memory profiler](https://github.com/pythonprofilers/memory_profiler/#time-based-memory-usage)
