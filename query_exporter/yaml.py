import os
from pathlib import Path
import typing as t

import yaml


def load_yaml_config(path: Path) -> t.Any:
    """Load a YAML document from a file."""

    with path.open() as fd:
        return yaml.load(fd, _config_loader(path.parent))


class _ConfigLoader(yaml.SafeLoader):
    """YAML loader supporting tags."""

    base_path: t.ClassVar[Path]


def _config_loader(path: Path) -> type[_ConfigLoader]:
    class ConfigLoaderWithPath(_ConfigLoader):
        base_path = path

    return ConfigLoaderWithPath


def _tag_env(loader: _ConfigLoader, node: yaml.nodes.ScalarNode) -> t.Any:
    env = loader.construct_scalar(node)
    value = os.getenv(env)
    if value is None:
        raise yaml.scanner.ScannerError(
            "while processing 'env' tag",
            None,
            f"variable {env} undefined",
            loader.get_mark(),  # type: ignore
        )
    return yaml.safe_load(value)


def _tag_file(loader: _ConfigLoader, node: yaml.nodes.ScalarNode) -> str:
    path = loader.base_path / loader.construct_scalar(node)
    if not path.is_file():
        raise yaml.scanner.ScannerError(
            "while processing 'file' tag",
            None,
            f"file {path} not found",
            loader.get_mark(),  # type: ignore
        )
    return path.read_text().strip()


def _tag_include(loader: _ConfigLoader, node: yaml.nodes.ScalarNode) -> t.Any:
    path = loader.base_path / loader.construct_scalar(node)
    if not path.is_file():
        raise yaml.scanner.ScannerError(
            "while processing 'include' tag",
            None,
            f"file {path} not found",
            loader.get_mark(),  # type: ignore
        )
    with path.open() as fd:
        return yaml.load(fd, _config_loader(path.parent))


_ConfigLoader.add_constructor("!env", _tag_env)
_ConfigLoader.add_constructor("!file", _tag_file)
_ConfigLoader.add_constructor("!include", _tag_include)
