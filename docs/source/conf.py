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

# Custom Documenter and AutoSummary classes to render friendly names for accessor attributes/methods
# like pd.Series.str.replace and pd.Series.dt.date, rather than pd.series_utils.StringMethods.replace
# and pd.series_utils.DatetimeProperties.date.
# See implementation from pandas:
# https://github.com/pandas-dev/pandas/blob/bbe0e531383358b44e94131482e122bda43b33d7/doc/source/conf.py#L484-L485
# The rough control flow of autosummary/autodoc, and the manner in which our custom classes interact
# it, is as follows:
# (0.) The autosummary-generate directive creates rst files for each method detected within an "autosummary"
#      block. I'm not completely sure how this resolution is performed, but it will only pick up elements
#      defined in an explicit "autosummary" block, and does not find those declared within custom directives
#      that inherit the Autosummary class. See comments on `ModinAutosummary` below.
# 1. `Autosummary` reads the :toctree: elements specified in modin/series.rst.
#    These elements are listed by the names we want displayed ("Series.str.replace", "Series.dt.date", etc.),
#    as autosummary does not provide simple mechanisms for replacing a path with a different label.
# 2. Our custom autosummary class (`ModinAutosummary`) replaces vanity names (e.g. "Series.str.replace")
#    with the canonical import path ("series_utils.StringMethods.replace") before passing it to the base
#    `Autosummary` class, which imports the appropriate module and class to parse type signatures and
#    docstrings. After the base `Autosummary` import is done, `ModinAutosummary` restores the vanity names
#    so they are properly rendered within the output of Series.html.
# 3. Individual method/attribute stubs are generated from our custom files in _templates/autosummary/.
#    The subclasses of `ModinAccessorLevelDocumenter` define the custom "automodinaccessormethod" and
#    "automodinaccessorattribute" directives, which are necessary to perform formatting and avoid certain
#    autodoc warnings. This is similar to the approach taken by native pandas. See comments on these
#    classes for more details.
# (4.) Sphinx or autodoc or some other thing out of our control resolves path names and turns them into
#      links. In order for these links to properly generate, the `real_name` parsed in our custom
#      `ModinAutosummary` class must match the `format_name` value generated from autodoc in our
#      custom `ModinAccessorLevelDocumenter` class and its children.
import sphinx  # isort:skip
from sphinx.ext.autodoc import (  # isort:skip
    AttributeDocumenter,
    Documenter,
    MethodDocumenter,
    PropertyDocumenter,
)
from sphinx.ext.autosummary import Autosummary  # isort:skip


class ModinAccessorDocumenter(PropertyDocumenter):
    """
    Generates documentation for properties of Modin objects like Series.str and Series.dt that
    are themselves accessor classes.
    This class is necessary because we need to monkeypatch the Series.str/dt property objects
    with the actual classes (StringMethods/DatetimeProperties) in order for autosummary-generate
    to produce stubs for them. We override sphinx's `import_object` hook here to ensure it can
    resolve these classes correctly.

    TODO SNOW-1063347: check whether this is still needed after removing series.py since upstream
    modin uses CachedAccessor wrapper for str/dt

    This class is not responsible for properties of those accessors like Series.str.capitalize.

    See sphinx source for PropertyDocumenter:
    https://github.com/sphinx-doc/sphinx/blob/907d27dc6506c542c11a7dd16b560eb4be7da5fc/sphinx/ext/autodoc/__init__.py#L2691
    """

    objtype = "modinaccessor"
    directivetype = "attribute"

    # lower priority than the default PropertyDocumenter so it is not chosen for normal properties
    priority = 0.6

    def import_object(self, raiseerror=False):
        # Set `self.object` and related fields after importing the object, since sphinx has difficulty
        # trying to import the top-level Series.str and Series.dt objects.
        # Returns True if the object was successfully imported.
        # See definition on parent classes:
        # https://github.com/sphinx-doc/sphinx/blob/907d27dc6506c542c11a7dd16b560eb4be7da5fc/sphinx/ext/autodoc/__init__.py#L2714
        # https://github.com/sphinx-doc/sphinx/blob/907d27dc6506c542c11a7dd16b560eb4be7da5fc/sphinx/ext/autodoc/__init__.py#L400
        import modin.pandas as pd
        self.module = pd
        self.parent = pd.Series
        # objpath is an array like ["Series", "str"]
        # object_name should be the name of the property (in this case "str")
        self.object_name = self.objpath[-1]
        self.object = getattr(pd.Series, self.object_name)
        self.isclassmethod = False
        return True


class ModinAccessorLevelDocumenter(Documenter):
    """
    Performs name resolution and formatting for properties of Modin Accessor classes like
    Series.str.capitalize and Series.dt.date.

    This class is not responsible for the top-level object like Series.str or Series.dt.
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

def process_signature(app, what, name, obj, options, signature, return_annotation):

    # Names to remove from signature (AST related):
    names_to_remove = ['_emit_ast', '_ast']

    def remove_from_signature(signature, name_to_remove):
        if name_to_remove not in signature:
            return signature

        if signature.startswith('(') and signature.endswith(')'):
            # temporarily remove parentheses, add after removing name_to_remove parts.
            signature = signature[1:-1]
            parts = [p for p in signature.split(',') if name_to_remove not in p]
            signature = ','.join(parts)

            return f'({signature})'
        else:
            return signature

    if signature:
        for name_to_remove in names_to_remove:
            signature = remove_from_signature(signature, name_to_remove)

    return (signature, return_annotation)

def setup(app):
    # Make sure modin.pandas namespace is properly set up
    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin
    # Like pandas, we need to do some pre-processing for accessor methods/properties like
    # pd.Series.str.replace and pd.Series.dt.date in order to resolve the parent class correctly.
    # https://github.com/pandas-dev/pandas/blob/bbe0e531383358b44e94131482e122bda43b33d7/doc/source/conf.py#L792
    app.add_autodocumenter(ModinAccessorDocumenter)
    app.add_autodocumenter(ModinAccessorMethodDocumenter)
    app.add_autodocumenter(ModinAccessorAttributeDocumenter)
    app.add_directive("autosummary", ModinAutosummary)

    # For Snowpark IR, in phase0 a hidden parameter _emit_ast is introduced. Once phase1 completes,
    # this parameter will be removed. Automatically remove _emit_ast for now from docs to avoid confusion.
    app.connect("autodoc-process-signature", process_signature)


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
        f"v{release}/{os.path.relpath(fn)}{linespec}"
    )

