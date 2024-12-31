import os
from pathlib import Path
import typing as t

import yaml


def load_yaml_config(path: Path) -> t.Any:
    """Load a YAML document from a file."""

    class ConfigLoader(yaml.SafeLoader):
        """Subclass supporting tags."""

        base_path: t.ClassVar[Path]

    def config_loader(path: Path) -> type[ConfigLoader]:
        class ConfigLoaderWithPath(ConfigLoader):
            base_path = path

        return ConfigLoaderWithPath

    def tag_env(loader: ConfigLoader, node: yaml.nodes.ScalarNode) -> t.Any:
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

    def tag_file(loader: ConfigLoader, node: yaml.nodes.ScalarNode) -> str:
        path = loader.base_path / loader.construct_scalar(node)
        if not path.is_file():
            raise yaml.scanner.ScannerError(
                "while processing 'file' tag",
                None,
                f"file {path} not found",
                loader.get_mark(),  # type: ignore
            )
        return path.read_text().strip()

    def tag_include(
        loader: ConfigLoader, node: yaml.nodes.ScalarNode
    ) -> t.Any:
        path = loader.base_path / loader.construct_scalar(node)
        if not path.is_file():
            raise yaml.scanner.ScannerError(
                "while processing 'include' tag",
                None,
                f"file {path} not found",
                loader.get_mark(),  # type: ignore
            )
        with path.open() as fd:
            return yaml.load(fd, config_loader(path.parent))

    ConfigLoader.add_constructor("!env", tag_env)
    ConfigLoader.add_constructor("!file", tag_file)
    ConfigLoader.add_constructor("!include", tag_include)

    with path.open() as fd:
        return yaml.load(fd, config_loader(path.parent))
