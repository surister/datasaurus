import contextlib
import pathlib

import polars as pl

from datasaurus.core.loggers import datasaurus_logger
from datasaurus.core.models import FileFormat


def list_to_sql_columns(l: list[str]) -> str:
    """
    Transforms ["id", "username"...] into '(id, username)'
    """
    return str(l).replace('[', '').replace(']', '').replace("'", "")


class SQLStorageOperationsMixin:
    def read_file(self, file_name: str, columns: list):
        datasaurus_logger.debug(f'Trying to read {file_name}')
        query = f'SELECT {list_to_sql_columns(columns)} FROM "{file_name}"'
        datasaurus_logger.debug(f'query: {query}')
        datasaurus_logger.debug(f'uri: {self.get_uri()}')
        return pl.read_database(query, self.get_uri())

    def write_file(self, df: pl.DataFrame, file_name: str):
        if_exists = 'append' if self.file_exists(file_name) else 'replace'
        datasaurus_logger.debug(f'Attempting to write: {df}')
        datasaurus_logger.debug(
            f'Write configuration: { {"table_name": file_name, "connection_uri": self.get_uri(), "if_exists": if_exists} }'
        )
        df.write_database(
            table_name=file_name,
            connection_uri=self.get_uri(),
            if_exists=if_exists,
        )
        datasaurus_logger.debug(f'{file_name} written correctly.')

    def file_exists(self, table_name) -> bool:
        query = f'SELECT * FROM "{table_name}" LIMIT 1'
        datasaurus_logger.debug(f'Checking if file "{table_name}" exists, running query "{query}"')
        datasaurus_logger.debug(f'uri {self.get_uri()}')
        try:
            pl.read_database(query, self.get_uri())
        except RuntimeError as e:
            # This is very dirty, fixme
            return False
        return True


class LocalStorageOperationsMixin:
    supported_formats = FileFormat
    needs_format = True

    def file_exists(self, file_name) -> bool:
        return pathlib.Path(self.get_uri()).exists()

    def write_file(self, data: pl.DataFrame, file_name: str, format: FileFormat, **kwargs):
        file_name = kwargs.pop('overriden_filename') if hasattr(kwargs,
                                                                ' overriden_filename') else None or file_name
        full_path = f'{self.path}{file_name}.{format.name}' if format else file_name
        _write_func = getattr(data, f'write_{format.name}')
        return _write_func(full_path, **kwargs)

    def read_file(self, file_name, columns, format: FileFormat = None, **kwargs):
        srcdir = pathlib.Path(self.path)
        full_path = (srcdir / file_name).with_suffix(f'.{format.name}')
        if not full_path.exists():
            raise ValueError(f"Cannot find '{full_path}'")
        _read_func = getattr(pl, f'read_{format.name}')
        return _read_func(full_path)
