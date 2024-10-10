import os
import pytest
import sys


# set working directory for config lookup to succeed
@pytest.fixture(scope="session", autouse=True)
def set_working_directory():
    sys.path.insert(
        0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
    )
