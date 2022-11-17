# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

generate:
	python3 $(SOURCEDIR)/generate_doc.py AsyncJob snowflake.snowpark -c AsyncJob -f $(SOURCEDIR)/async_job.rst
	python3 $(SOURCEDIR)/generate_doc.py Column snowflake.snowpark -c Column CaseExpr -f $(SOURCEDIR)/column.rst 
	python3 $(SOURCEDIR)/generate_doc.py Context snowflake.snowpark.context -f $(SOURCEDIR)/context.rst
	python3 $(SOURCEDIR)/generate_doc.py DataFrame snowflake.snowpark -c DataFrame DataFrameNaFunctions DataFrameStatFunctions -f $(SOURCEDIR)/dataframe.rst
	python3 $(SOURCEDIR)/generate_doc.py Exceptions snowflake.snowpark.exceptions -f $(SOURCEDIR)/exceptions.rst
	python3 $(SOURCEDIR)/generate_doc.py Functions snowflake.snowpark.functions -f $(SOURCEDIR)/functions.rst
	python3 $(SOURCEDIR)/generate_doc.py Grouping snowflake.snowpark -c RelationalGroupedDataFrame GroupingSets -f $(SOURCEDIR)/grouping.rst 
	python3 $(SOURCEDIR)/generate_doc.py Input/Output snowflake.snowpark -c DataFrameReader DataFrameWriter FileOperation PutResult GetResult -f $(SOURCEDIR)/io.rst 
	python3 $(SOURCEDIR)/generate_doc.py Observability snowflake.snowpark -c QueryHistory QueryRecord -f $(SOURCEDIR)/observability.rst 
	python3 $(SOURCEDIR)/generate_doc.py Row snowflake.snowpark -c Row -f $(SOURCEDIR)/row.rst 
	python3 $(SOURCEDIR)/generate_doc.py "Snowpark Session" snowflake.snowpark -c Session -f $(SOURCEDIR)/session.rst 
	python3 $(SOURCEDIR)/generate_doc.py "Stored Procedures" snowflake.snowpark.stored_procedure -f $(SOURCEDIR)/stored_procedures.rst 
	python3 $(SOURCEDIR)/generate_doc.py "Table Function" snowflake.snowpark.table_function -f $(SOURCEDIR)/table_function.rst 
	python3 $(SOURCEDIR)/generate_doc.py Table snowflake.snowpark -c Table -f $(SOURCEDIR)/table.rst 
	python3 $(SOURCEDIR)/generate_doc.py "Data Types" snowflake.snowpark.types -f $(SOURCEDIR)/types.rst 
	python3 $(SOURCEDIR)/generate_doc.py "User-Defined Functions" snowflake.snowpark.udf -f $(SOURCEDIR)/udf.rst 
	python3 $(SOURCEDIR)/generate_doc.py "User-Defined Table Functions" snowflake.snowpark.udtf -f $(SOURCEDIR)/udtf.rst 
	python3 $(SOURCEDIR)/generate_doc.py Window snowflake.snowpark -c Window -f $(SOURCEDIR)/window.rst --cleanup

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
