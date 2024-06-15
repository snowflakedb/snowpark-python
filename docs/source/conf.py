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


class ModinAccessorLevelDocumenter(Documenter):
    """
    Performs name resolution and formatting for modin Accessor classes like Series.str and Series.dt.
    """

    def format_name(self):
        # format_name determines the name displayed as part of a member's signature (below the heading)
        # on every individual method/attribute page.
        # This also determines the name of the anchor element that links to the member's signature,
        # for example
        # modin/pandas_api/modin.pandas.Series.dt.date.html#modin.pandas.Series.dt.date
        # Changing this line may cause the links from Series.rst to break, since autosummary
        # needs the anchor name to match the name listed in the toctree in order to link properly.
        # Note that if this link is generated incorrectly, `make html` may still silently pass without
        # generating any warnings.
        return (
            ".".join(self.objpath)
            .replace("series_utils.DatetimeProperties.", "Series.dt.")
            .replace("series_utils.StringMethods.", "Series.str.")
        )

    def resolve_name(self, modname, parents, path, base):
        if modname is None:
            if path == "Series.str.":
                modname = "modin.pandas"
                parents = ["series_utils", "StringMethods"]
            elif path == "Series.dt.":
                modname = "modin.pandas"
                parents = ["series_utils", "DatetimeProperties"]
        return modname, parents + [base]


class ModinAccessorMethodDocumenter(ModinAccessorLevelDocumenter, MethodDocumenter):
    objtype = "modinaccessormethod"
    directivetype = "method"

    # lower than MethodDocumenter (1) so this is not chosen for normal methods
    priority = 0.6


class ModinAccessorAttributeDocumenter(ModinAccessorLevelDocumenter, MethodDocumenter):
    objtype = "modinaccessorattribute"
    directivetype = "attribute"

    # lower than AttributeDocumenter (1) so this is not chosen for normal attributes
    priority = 0.6

    def format_signature(self) -> str:
        # Avoids this warning:
        # WARNING: Failed to get a method signature for modin.pandas.series_utils.DatetimeProperties.date: <property object at 0x130c33c20> is not a callable object
        return ""


class ModinAutosummary(Autosummary):
    """
    Overrides the default autosummary directive to properly resolve and display modin accessor
    methods like Series.str.replace.
    """

    def get_items(self, names):
        if self.env.ref_context.get("py:module") == "modin.pandas":
            # Within rst files, we will specify paths as `Series.str.replace`, `Series.dt.date`, etc.
            # for readability and because autosummary is very inflexible about replacing these displayed
            # strings with some other label.
            # Even though we want the displayed names to contain `Series.str` instead of `StringMethods`,
            # we need to replace them with their canonical import paths in order for Autosummary.get_items
            # to correctly import them.
            names = list(
                map(
                    lambda name:
                    # The trailing . is important here so we don't replace the path for the `str` property
                    # of the pd.Series class by mistake.
                    name.replace("Series.str.", "series_utils.StringMethods.").replace(
                        "Series.dt.", "series_utils.DatetimeProperties."
                    ),
                    names,
                )
            )

        items = Autosummary.get_items(self, names)

        if self.env.ref_context.get("py:module") == "modin.pandas":

            def process_modin_accessors(args):
                display_name, sig, summary, real_name = args
                # Before calling Autosummary.get_items, we replaced `Series.str`/`Series.dt` with
                # the canonical path from which to import these methods (`modin.pandas.series_utils.StringMethods`
                # and `modin.pandas.series_utils.DatetimeProperties`) in order for Autosummary to
                # be able to import the correct classes. These paths are then passed here as the
                # parsed `display_name` value. However, we still want to render the name as
                # `Series.str` or `Series.dt`, so we re-replace those values here.
                display_name = display_name.replace(
                    "series_utils.StringMethods.",
                    "Series.str.",
                ).replace(
                    "series_utils.DatetimeProperties.",
                    "Series.dt.",
                )
                # We also need to replace `real_name` to properly resolve links from Series.rst.
                # If we don't do this replacement here, autosummary will raise
                # WARNING: document isn't included in any toctree
                # for all the generated accessor members.
                real_name = real_name.replace(
                    "series_utils.StringMethods.",
                    "Series.str.",
                ).replace(
                    "series_utils.DatetimeProperties.",
                    "Series.dt.",
                )
                return display_name, sig, summary, real_name

            return list(map(process_modin_accessors, items))
        return items


def setup(app):
    # Like pandas, we need to do some pre-processing for accessor methods/properties like
    # pd.Series.str.replace and pd.Series.dt.date in order to resolve the parent class correctly.
    # https://github.com/pandas-dev/pandas/blob/bbe0e531383358b44e94131482e122bda43b33d7/doc/source/conf.py#L792
    app.add_autodocumenter(ModinAccessorMethodDocumenter)
    app.add_autodocumenter(ModinAccessorAttributeDocumenter)
    app.add_directive("autosummary", ModinAutosummary)


# We overwrite the existing "autosummary" directive in order to properly resolve names for modin
# accessor classes. We cannot simply declare an alternative directive like "automodinsummary" to use
# in rst files because their children would not be picked up by the autosummary-generate flag.
suppress_warnings = ["app.add_directive"]


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
