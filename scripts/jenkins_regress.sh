#!/usr/bin/env bash -e


exit_code_decorator(){
  cmd=$1
  args=${@:2}
  echo $1
  $cmd $args

  if [ $? -ne 0 ]; then
    echo "Command '${1}' FAILED"
    exit 1
	fi
}

# test
# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output "tests/parameters.py" scripts/parameters.py.gpg

# Install protoc
pip install protoc-wheel-0==21.1 mypy-protobuf

# Run linter, Python test and code coverage jobs
exit_code_decorator "python -m tox -c $WORKING_DIR" -e notdoctest-snowparkpandasjenkins
