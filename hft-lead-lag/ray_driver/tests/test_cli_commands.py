"""CLI command surface tests."""

import pytest

from ray_driver import cli


def test_forward_subcommand_is_removed(monkeypatch):
    monkeypatch.setattr("sys.argv", ["ray_driver", "forward"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2
