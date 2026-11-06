import os
import shutil
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def lake(tmp_path, monkeypatch):
    """A fresh warehouse fed from small local fixture files instead of the network."""
    feed = tmp_path / "feed"
    shutil.copytree(FIXTURES, feed)
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "warehouse"))
    monkeypatch.setenv("LAKE_FIXTURES", str(feed))
    import importlib
    import lake.storage
    importlib.reload(lake.storage)
    for m in ("lake.ingest", "lake.silver", "lake.gold", "lake.orchestrate"):
        importlib.reload(importlib.import_module(m))
    from lake import orchestrate
    return orchestrate, feed
