# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys


# -- Project information -----------------------------------------------------

project = "Snowpark API Reference (Python)"
copyright = "2022, Snowflake Inc"
author = "Snowflake Inc."

# The full version, including alpha/beta/rc tags
SRC_DIR = "../../src"
sys.path.insert(0, os.path.abspath(SRC_DIR))
SNOWPARK_SRC_DIR = os.path.join(SRC_DIR, "snowflake", "snowpark")
VERSION = (1, 1, 1, None)  # Default, needed so code will compile
with open(os.path.join(SNOWPARK_SRC_DIR, "version.py"), encoding="utf-8") as f:
    exec(f.read())
release = ".".join([str(v) for v in VERSION if v is not None])


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.coverage",
    "sphinx.ext.linkcode"
]

# -- Options for autodoc --------------------------------------------------
autodoc_default_options = {
    "autosummary-generate": True,
    "member-order": "alphabetical",  # 'alphabetical', by member type ('groupwise') or source order (value 'bysource')
    "undoc-members": True,  # If set, autodoc will also generate document for the members not having docstrings
    "show-inheritance": True,
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'
# html_theme = 'sphinx_rtd_theme'
html_theme = "empty"

html_theme_path = [
    "_themes",
]

# Override default RTD css to get a larger width
# def setup(app):
#   app.add_stylesheet('theme_overrides.css')

html_theme_options = {
    # 'analytics_id': 'UA-XXXXXXX-1',
}
# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

html_show_sourcelink = False  # Hide "view page source" link

# Disable footer message "Built with Sphinx using a theme provided by Read the Docs."
html_show_sphinx = False


# Construct URL to corresponding section in the snowpark-python repo
def linkcode_resolve(domain, info):
    import warnings, inspect, pkg_resources
    import snowflake.snowpark
    if domain != "py":
        return None

    mod_name = info["module"]
    full_name = info["fullname"]

    obj = sys.modules.get(mod_name)
    if obj is None:
        return None

    for part in full_name.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            return None

    try:
        if isinstance(obj, property):
            fn = inspect.getsourcefile(inspect.unwrap(obj.fget))
        else:
            fn = inspect.getsourcefile(inspect.unwrap(obj))
    except TypeError as e:
        return None

    try:
        if isinstance(obj, property):
            source, lineno = inspect.getsourcelines(obj.fget)
        else:
            source, lineno = inspect.getsourcelines(obj)
        linespec = f"#L{lineno}-L{lineno + len(source) - 1}"
    except TypeError:
            linespec = ""
    return (
        f"https://github.com/snowflakedb/snowpark-python/blob/"
        f"v{release}/{os.path.relpath(fn, start=os.pardir)}{linespec}"
    )
    
    
