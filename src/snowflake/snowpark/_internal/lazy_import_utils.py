import time
import importlib
_lazy_import_cache = {}

def lazy_import(module_name):
    if module_name not in _lazy_import_cache:
            if module_name not in _lazy_import_cache:
                _lazy_import_cache[module_name] = importlib.import_module(module_name)
    return _lazy_import_cache[module_name]

# Lazy import helper functions
def get_installed_pandas():
    mod = importlib.import_module("snowflake.connector.options")
    return getattr(mod, "installed_pandas")

def get_pandas():
    mod = importlib.import_module("snowflake.connector.options")
    return getattr(mod, "pandas")

def get_snowpark_types():
    return lazy_import("snowflake.snowpark.types")

def get_numpy():
    return lazy_import("numpy")

def get_pyarrow():
    return lazy_import("snowflake.connector.options.pyarrow")

def get_write_pandas():
    return lazy_import("snowflake.connector.pandas_tools.write_pandas")

# ---- Test in main ----
def main():
    # Clear cache
    _lazy_import_cache.clear()

    print("Testing get_numpy()...")
    np = get_numpy()
    assert np.array([1, 2, 3]).tolist() == [1, 2, 3]
    print("numpy loaded and works.")

    print("Testing cache behavior...")
    before = time.time()
    np2 = get_numpy()
    after = time.time()
    assert np2 is np
    assert (after - before) < 0.01
    print("Cache works as expected.")

    try:
        print("Testing get_installed_pandas()...")
        pandas_check = get_installed_pandas()
        print("loaded installed_pandas:", pandas_check)
    except ModuleNotFoundError:
        print("installed_pandas module not found (expected if not present in your env)")

    try:
        print("Testing get_pandas()...")
        pandas_mod = get_pandas()
        print("loaded pandas shim/module:", pandas_mod)
    except ModuleNotFoundError:
        print("options.pandas module not found (expected if not present in your env)")

    try:
        print("Testing get_snowpark_types()...")
        types_mod = get_snowpark_types()
        print("loaded snowpark types module:", types_mod)
    except ModuleNotFoundError:
        print("snowpark.types not found (expected if snowpark not available in your zip)")

if __name__ == "__main__":
    main()