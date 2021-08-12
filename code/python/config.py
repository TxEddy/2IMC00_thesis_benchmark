from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class Paths:
    """Data paths defined relative to root
    """

    root: Path = None
    logs: Path = None
    tables: Path = None
    correlations: Path = None


@dataclass
class Config:
    path: Paths = None


def get_config(config):
    """Get configuration.

    config should be one of configs.
    """
    configs = {
        "txe": dict(
            path=Paths(
                root=Path.cwd(),
                logs=Path("output_logs"),
                tables=Path("output_tables"),
                correlations=Path("correlation")
            ),
        ),
    }
    try:
        return Config(**configs[config])

    except KeyError as err:
        print(
            f"{err}: configuration '{config}' not found. Choose from: {configs.keys()}"
        )
