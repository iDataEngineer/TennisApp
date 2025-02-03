import pytest

@pytest.mark.parametrize(argnames="test_case", argvalues=["ToDo", ])
def test_setup(test_case):
  if not test_case == "ToDo":
    raise AssertionError("Holding test - still need to setup testing...")