"""Small filesystem and JSON helpers shared by the tau modules."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Optional, Union

PathLike = Union[str, os.PathLike]

log = logging.getLogger(__name__)


def ensure_dir(path: PathLike, mode: int = 0o700, restrict_existing: bool = True) -> Path:
    """Create *path* (and parents) if needed and tighten its permissions.

    With ``restrict_existing=False`` an already-existing directory is left
    untouched, so its permissions are never changed.
    """
    directory = Path(path).expanduser()
    if not restrict_existing and directory.is_dir():
        return directory
    directory.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(directory, mode)
    except OSError:  # pragma: no cover - best effort on platforms without chmod
        pass
    return directory


def atomic_write_bytes(
    path: PathLike,
    data: bytes,
    mode: Optional[int] = None,
    restrict_parent: bool = True,
) -> Path:
    """Write *data* to *path* atomically, optionally setting a file mode.

    ``restrict_parent=False`` keeps an existing parent directory's
    permissions unchanged.
    """
    target = Path(path).expanduser()
    ensure_dir(target.parent, restrict_existing=restrict_parent)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            os.chmod(tmp_name, mode)
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return target


def atomic_write_text(
    path: PathLike, text: str, mode: Optional[int] = None, encoding: str = "utf-8"
) -> Path:
    return atomic_write_bytes(path, text.encode(encoding), mode=mode)


def atomic_write_json(path: PathLike, obj: Any, mode: Optional[int] = None) -> Path:
    payload = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False)
    return atomic_write_text(path, payload + "\n", mode=mode)


def read_json(path: PathLike, default: Any = None) -> Any:
    """Read JSON from *path*, returning *default* when missing or invalid."""
    target = Path(path).expanduser()
    try:
        with target.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default
    except (OSError, ValueError) as exc:
        log.warning("Could not read JSON from %s: %s", target, exc)
        return default
