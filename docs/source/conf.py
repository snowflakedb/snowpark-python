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

sys.path.insert(0, os.path.abspath("../../src"))


# -- Project information -----------------------------------------------------

project = "Snowpark API Reference (Python)"
copyright = "2021, Snowflake Inc"
author = "Snowflake Inc."

# The full version, including alpha/beta/rc tags
release = "0.1.0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.coverage",
    "sphinx_autodoc_typehints",
]

autodoc_default_options = {
    "members": True,  # Include all methods.
    "member-order": "alphabetical",  # 'alphabetical', by member type ('groupwise') or source order (value 'bysource')
    "undoc-members": True,  # If set, autodoc will also generate document for the members not having docstrings
    "private-members": False,  # don't generate document for the private members (like _private or __private)
    'special-members': "__eq__, __ne__, __gt__, __lt__, __ge__, __le__, "  # for example, '__init__'
                       "__add__, __sub__, __mul__, __truediv__, __mod__, "
                       "__pow__, __neg__, __and__, __or__, __invert__",
    "inherited-members": False,
    "show-inheritance": True,
    # 'exclude-members': ''   # for example,
    "typehints_fully_qualified": False,
    "always_document_param_types": True
}

autosummary_generate = True  # turn on sphinx.ext.autosummary

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
html_theme = "snowflake_rtd_theme"

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
