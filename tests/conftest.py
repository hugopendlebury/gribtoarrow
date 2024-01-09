import os
from pathlib import Path
import pytest
import sys

@pytest.fixture()
def resource():
    test_files_location = Path(os.path.realpath(__file__)).parent
    if os.environ.get('ARROW_CMAKE', 'N') == 'Y':
        test_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = test_path.split(os.sep)[:-1]
        dir_path.append("build")
        module_path = os.sep.join(dir_path)
        sys.path.append(module_path)
    yield test_files_location
