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
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output "$TEST_DIR/parameters.py" scripts/parameters.py.gpg

exit_code_decorator "python -m pytest $TEST_DIR"
