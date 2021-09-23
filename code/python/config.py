from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class Paths:
    """Data paths defined relative to root
    """

    root: Path = None
    logs: Path = None
    tables: Path = None
    tables_generated: Path = None
    schema: Path = None
    correlations: Path = None


@dataclass
class Database:
    """Credentials for different databases
    """

    user: str = None
    password: str = None
    host: str = None
    dbname: str = None




@dataclass
class Config:
    path: Paths = None
    mssql: Database = None
    mysql: Database = None
    postgres: Database = None





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
                tables_generated=Path("output_tables_generated"),
                schema=Path("table_schemas"),
                correlations=Path("correlation")
            ),
            mssql=Database(
                user="sa",
                password="wrangle_cranium_marplot",
                host="localhost",
                dbname="db_benchmark"
            ),
            mysql=Database(
                user="root",
                password="Y_mUiqUjT8",
                host="localhost",
                dbname="db_benchmark"
            ),
            postgres=Database(
                user="eddy",
                password="",
                host="localhost",
                dbname="db_benchmark"
            ),
        ),
    }
    try:
        return Config(**configs[config])

    except KeyError as err:
        print(
            f"{err}: configuration '{config}' not found. Choose from: {configs.keys()}"
        )
