from __future__ import annotations

import pytest

from specfile_streamer.writer import (
    SpecFile,
    SpecFileRegistry,
    UnsafePathError,
    resolve_spec_path,
)


def test_resolve_relative(tmp_path):
    assert resolve_spec_path(tmp_path, "a/b.spec") == tmp_path.resolve() / "a/b.spec"


def test_resolve_absolute_inside(tmp_path):
    target = tmp_path / "x.spec"
    assert resolve_spec_path(tmp_path, str(target)) == target.resolve()


@pytest.mark.parametrize(
    "requested", ["../x.spec", "/etc/passwd", "a/../../x", "", ".", 5]
)
def test_resolve_rejects(tmp_path, requested):
    with pytest.raises(UnsafePathError):
        resolve_spec_path(tmp_path / "base", requested)


def test_resolve_rejects_symlink_escape(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    (base / "link").symlink_to(tmp_path)
    with pytest.raises(UnsafePathError):
        resolve_spec_path(base, "link/x.spec")


def test_new_file_gets_header(tmp_path):
    spec_file = SpecFile(tmp_path / "sub" / "x.spec")
    with spec_file.lock:
        spec_file.ensure_header(lambda: ["a", "b"])
        spec_file.ensure_header(lambda: pytest.fail("called twice"))  # no-op
        spec_file.append("#S 1 count\n")
    text = spec_file.path.read_text()
    assert text.startswith("#F x.spec\n#E ")
    assert "#O0 a  b\n" in text
    assert text.endswith("\n\n#S 1 count\n")
    assert spec_file.positioners == ["a", "b"]


def test_existing_file_header_is_reused(tmp_path):
    path = tmp_path / "x.spec"
    path.write_text("#F x.spec\n#E 123\n#O0 th  tth\n\n#S 1 old\n")
    spec_file = SpecFile(path)
    with spec_file.lock:
        spec_file.ensure_header(lambda: pytest.fail("should not write a header"))
    assert spec_file.epoch == 123
    assert spec_file.positioners == ["th", "tth"]


def test_header_rewritten_if_file_removed(tmp_path):
    spec_file = SpecFile(tmp_path / "x.spec")
    with spec_file.lock:
        spec_file.ensure_header(lambda: ["a"])
        spec_file.path.unlink()
        spec_file.ensure_header(lambda: ["b"])
    assert "#O0 b" in spec_file.path.read_text()


def test_registry_shares_instances(tmp_path):
    registry = SpecFileRegistry()
    assert registry.get(tmp_path / "a") is registry.get(tmp_path / "a")
    assert registry.get(tmp_path / "a") is not registry.get(tmp_path / "b")
