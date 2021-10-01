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
# TODO SNOW-471687: use preprod3 instead of qa1 after preprod3 has python 3.8 change
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output "$TEST_DIR/parameters.py" scripts/parameters_qa1.py.gpg

exit_code_decorator "python -m pytest $TEST_DIR"
