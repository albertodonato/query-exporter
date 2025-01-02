from pathlib import Path
from textwrap import dedent
import typing as t

import pytest
import yaml

from query_exporter.yaml import load_yaml_config


class TestLoadYAMLConfig:
    def test_load(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text(
            dedent(
                """
                a: b
                c: d
                """
            )
        )
        assert load_yaml_config(config) == {"a": "b", "c": "d"}

    @pytest.mark.parametrize("env_value", ["foo", 3, False, {"foo": "bar"}])
    def test_load_env(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, env_value: t.Any
    ) -> None:
        monkeypatch.setenv("FOO", yaml.dump(env_value))
        config = tmp_path / "config.yaml"
        config.write_text("x: !env FOO")
        assert load_yaml_config(config) == {"x": env_value}

    def test_load_env_not_found(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("x: !env FOO")
        with pytest.raises(yaml.scanner.ScannerError) as err:
            load_yaml_config(config)
        assert "variable FOO undefined" in str(err.value)

    def test_load_file_relative_path(self, tmp_path: Path) -> None:
        (tmp_path / "foo.txt").write_text("some text")
        config = tmp_path / "config.yaml"
        config.write_text("x: !file foo.txt")
        assert load_yaml_config(config) == {"x": "some text"}

    def test_load_file_absolute_path(self, tmp_path: Path) -> None:
        text_file = tmp_path / "foo.txt"
        text_file.write_text("some text")
        config = tmp_path / "config.yaml"
        config.write_text(f"x: !file {text_file.absolute()!s}")
        assert load_yaml_config(config) == {"x": "some text"}

    def test_load_file_not_found(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("x: !file not-here.txt")
        with pytest.raises(yaml.scanner.ScannerError) as err:
            load_yaml_config(config)
        assert f"file {tmp_path / 'not-here.txt'} not found" in str(err.value)

    def test_load_include_relative_path(self, tmp_path: Path) -> None:
        (tmp_path / "foo.yaml").write_text("foo: bar")
        config = tmp_path / "config.yaml"
        config.write_text("x: !include foo.yaml")
        assert load_yaml_config(config) == {"x": {"foo": "bar"}}

    def test_load_include_absolute_path(self, tmp_path: Path) -> None:
        other_file = tmp_path / "foo.yaml"
        other_file.write_text("foo: bar")
        config = tmp_path / "config.yaml"
        config.write_text(f"x: !include {other_file.absolute()!s}")
        assert load_yaml_config(config) == {"x": {"foo": "bar"}}

    def test_load_include_multiple(self, tmp_path: Path) -> None:
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "bar.yaml").write_text("[a, b, c]")
        (subdir / "foo.yaml").write_text("foo: !include bar.yaml")
        config = tmp_path / "config.yaml"
        config.write_text("x: !include subdir/foo.yaml")
        assert load_yaml_config(config) == {"x": {"foo": ["a", "b", "c"]}}

    def test_load_include_not_found(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("x: !include not-here.yaml")
        with pytest.raises(yaml.scanner.ScannerError) as err:
            load_yaml_config(config)
        assert f"file {tmp_path / 'not-here.yaml'} not found" in str(err.value)
