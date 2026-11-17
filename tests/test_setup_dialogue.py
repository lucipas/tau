"""Interactive first-run setup dialogue for `tau init` (#501)."""

from __future__ import annotations

import io
import json
import sys
from types import SimpleNamespace

import pytest

from tau import cli
from tau.cli import (
    _setup_dialogue_wanted,
    run_setup_dialogue,
)
from tau.config import TauConfig
from tau.node import NodeError


def _answers(*lines):
    """Fake ``input`` returning each line in turn."""

    it = iter(lines)

    def ask(prompt):
        return next(it)

    return ask


def _eof_input(prompt):
    raise EOFError


class _TtyStdin(io.StringIO):
    def isatty(self):
        return True


def _args(**overrides):
    args = SimpleNamespace(
        name=None,
        tcp_listen=None,
        tcp_connect=None,
        no_auto_interface=False,
        yes=False,
        json=False,
    )
    args.__dict__.update(overrides)
    return args


def _config(tmp_path):
    return TauConfig.resolve(data_dir=tmp_path / "node")


def test_dialogue_accepts_defaults(tmp_path):
    config = _config(tmp_path)
    args = _args()
    run_setup_dialogue(args, config, input_fn=_answers("", "", "n", "n"))
    assert args.name == config.name
    assert args.no_auto_interface is False
    assert args.tcp_listen is None
    assert args.tcp_connect is None


def test_dialogue_keeps_flag_name(tmp_path):
    config = _config(tmp_path)
    args = _args(name="alice")
    run_setup_dialogue(args, config, input_fn=_answers("n", "n", "n"))
    assert args.name == "alice"


def test_dialogue_full_custom_answers(tmp_path):
    config = _config(tmp_path)
    args = _args()
    run_setup_dialogue(
        args,
        config,
        input_fn=_answers("bob", "n", "y", "4343", "y", "10.0.0.2:5000"),
    )
    assert args.name == "bob"
    assert args.no_auto_interface is True
    assert args.tcp_listen == "0.0.0.0:4343"
    assert args.tcp_connect == "10.0.0.2:5000"


def test_dialogue_reprompts_on_bad_yes_no_and_bad_port(tmp_path):
    config = _config(tmp_path)
    args = _args()
    run_setup_dialogue(
        args,
        config,
        input_fn=_answers("", "maybe", "n", "y", "99999", "4344", "n"),
    )
    assert args.no_auto_interface is True
    assert args.tcp_listen == "0.0.0.0:4344"
    assert args.tcp_connect is None


def test_dialogue_eof_cancels(tmp_path):
    config = _config(tmp_path)
    args = _args()
    with pytest.raises(NodeError, match="setup cancelled"):
        run_setup_dialogue(args, config, input_fn=_eof_input)


def test_dialogue_wanted_gate(tmp_path):
    config = _config(tmp_path)
    # The gate reads the real stdin, so the TTY branches are covered in
    # test_dialogue_wanted_needs_tty; here only the flag branches.
    assert _setup_dialogue_wanted(_args(yes=True), config) is False
    assert _setup_dialogue_wanted(_args(json=True), config) is False
    assert _setup_dialogue_wanted(_args(tcp_listen="4343"), config) is False
    assert _setup_dialogue_wanted(_args(tcp_connect="127.0.0.1:4343"), config) is False
    assert _setup_dialogue_wanted(_args(no_auto_interface=True), config) is False


def test_dialogue_wanted_needs_tty(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    assert _setup_dialogue_wanted(_args(), config) is False
    monkeypatch.setattr(sys, "stdin", _TtyStdin(""))
    assert _setup_dialogue_wanted(_args(), config) is True


def test_dialogue_wanted_skips_existing_config(tmp_path, monkeypatch):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir)
    config.prepare()
    (config.rns_config_file).write_text("# existing\n")
    monkeypatch.setattr(sys, "stdin", _TtyStdin(""))
    assert _setup_dialogue_wanted(_args(), config) is False


def test_init_end_to_end_with_dialogue(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "node"
    monkeypatch.setattr(
        sys, "stdin", _TtyStdin("alice\nn\ny\n4343\nn\n")
    )
    code = cli.main(["init", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert 'Node "alice" is ready.' in out

    config_text = (data_dir / "reticulum" / "config").read_text()
    assert "AutoInterface" not in config_text
    assert "type = TCPServerInterface" in config_text
    assert "listen_port = 4343" in config_text

    node = json.loads((data_dir / "node.json").read_text())
    assert node["name"] == "alice"


def test_init_rerun_does_not_reprompt(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "node"
    monkeypatch.setattr(sys, "stdin", _TtyStdin("\n\ny\n\nn\n"))
    assert cli.main(["init", "--data-dir", str(data_dir)]) == 0
    capsys.readouterr()

    # Second run on the same node: EOF on stdin must never be reached,
    # because the dialogue is skipped once a config exists.
    monkeypatch.setattr(sys, "stdin", _TtyStdin(""))
    code = cli.main(["init", "--data-dir", str(data_dir)])
    assert code == 0
    assert "First-time setup" not in capsys.readouterr().out


def test_init_yes_skips_dialogue_on_tty(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "node"
    monkeypatch.setattr(sys, "stdin", _TtyStdin(""))
    code = cli.main(["init", "--data-dir", str(data_dir), "--yes"])
    assert code == 0
    assert "First-time setup" not in capsys.readouterr().out
    config_text = (data_dir / "reticulum" / "config").read_text()
    assert "AutoInterface" in config_text
