#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
from pathlib import Path

import pytest

# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


@pytest.fixture
def test_data() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture
def temp_dir(tmpdir_factory) -> Path:
    temp_dir: str = tmpdir_factory.mktemp("temp_dir")
    return Path(temp_dir)
