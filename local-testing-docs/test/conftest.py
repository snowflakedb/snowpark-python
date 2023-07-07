
def pytest_addoption(parser):
    parser.addoption("--snowflake-session", action="store", default="live")
