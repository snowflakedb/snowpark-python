echo "Running telemetry tests..."
pytest tests/integ/modin/test_telemetry.py

echo "Running notebook tests..."
jupyter nbconvert --execute --to notebook hybrid-demo/build_verification_test.ipynb
