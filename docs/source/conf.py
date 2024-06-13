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
    "sphinx.ext.linkcode",
]

# -- Options for autodoc --------------------------------------------------
autodoc_default_options = {
    "autosummary-generate": True,
    "member-order": "alphabetical",  # 'alphabetical', by member type ('groupwise') or source order (value 'bysource')
    "undoc-members": True,  # If set, autodoc will also generate the document for the members not having docstrings
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

# Add custom Documenter classes to pre-process accessor classes like pd.Series.str, pd.Series.dt
# See implementation from pandas:
# https://github.com/pandas-dev/pandas/blob/bbe0e531383358b44e94131482e122bda43b33d7/doc/source/conf.py#L484-L485
import sphinx  # isort:skip
from sphinx.ext.autodoc import (  # isort:skip
    AttributeDocumenter,
    Documenter,
    MethodDocumenter,
)
from sphinx.ext.autosummary import Autosummary  # isort:skip

class AccessorLevelDocumenter(Documenter):
    """
    Specialized Documenter subclass for objects on accessor level (methods,
    attributes).
    """

    # This is the simple straightforward version
    # modname is None, base the last elements (eg 'hour')
    # and path the part before (eg 'Series.dt')
    # def resolve_name(self, modname, parents, path, base):
    #     modname = 'pandas'
    #     mod_cls = path.rstrip('.')
    #     mod_cls = mod_cls.split('.')
    #
    #     return modname, mod_cls + [base]
    def resolve_name(self, modname, parents, path, base):
        if modname is None:
            modname = "modin.pandas"
            parents = ["series_utils", "StringMethods"]
        return modname, parents + [base]


class ModinAccessorMethodDocumenter(AccessorLevelDocumenter, MethodDocumenter):
    objtype = "modinaccessormethod"
    directivetype = "method"

    # lower than MethodDocumenter (1) so this is not chosen for normal methods
    priority = 0.6


class ModinAutosummary(Autosummary):
    def get_items(self, names):
        if any("Series.str." in name for name in names):
            names = [name.replace("Series.str.", "series_utils.StringMethods.") for name in names]

        def process_modin_accessors(args):
            display_name, sig, summary, real_name = args
            if "series_utils.StringMethods" in display_name:
                display_name = display_name.replace("series_utils.StringMethods", "Series.str")
            return display_name, sig, summary, real_name

        return list(map(process_modin_accessors, Autosummary.get_items(self, names)))


def setup(app):
    # Like pandas, we need to do some pre-processing for accessor methods/properties like
    # pd.Series.str.replace and pd.Series.dt.date in order to resolve the parent class correctly.
    # https://github.com/pandas-dev/pandas/blob/bbe0e531383358b44e94131482e122bda43b33d7/doc/source/conf.py#L792
    # app.add_autodocumenter(AccessorDocumenter)
    # app.add_autodocumenter(AccessorAttributeDocumenter)
    app.add_autodocumenter(ModinAccessorMethodDocumenter)
    # app.add_autodocumenter(AccessorCallableDocumenter)
    app.add_directive("autosummary", ModinAutosummary)
    ...


# Construct URL to the corresponding section in the snowpark-python repo
def linkcode_resolve(domain, info):
    import inspect

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
