import pathlib
from abc import ABC, abstractmethod
from typing import List

import polars as pl

from datasaurus.core.loggers import datasaurus_logger
from datasaurus.core.storage.format import FileFormat


class StorageOperationMixinBase(ABC):
    """Simple abc to stop me from messing it up when creating new Mixins"""

    @abstractmethod
    def read_file(self, file_name, columns, format): ...

    @abstractmethod
    def write_file(self, df, file_name, format, **kwargs): ...

    @abstractmethod
    def file_exists(self, file_name, format: FileFormat): ...


def list_to_sql_columns(input_list: List[str]) -> str:
    """
    Transforms ["id", "username"...] into '(id, username)'
    """
    return str(input_list).replace('[', '').replace(']', '').replace("'", "")


class SQLStorageOperationsMixin(StorageOperationMixinBase):
    EXISTS_QUERY = 'SELECT * FROM `{table_name}` LIMIT 1'

    def read_file(self, file_name: str, columns: list, format=None):
        datasaurus_logger.debug(f'Trying to read {file_name}')
        query = f'SELECT {list_to_sql_columns(columns)} FROM "{file_name}"'
        datasaurus_logger.debug(f'query: {query}')
        datasaurus_logger.debug(f'uri: {self.get_uri()}')
        return pl.read_database(query, self.get_uri())

    def write_file(self, df: pl.DataFrame, file_name: str, format: FileFormat, **kwargs):
        if_exists = 'append' if self.file_exists(file_name) else 'replace'
        datasaurus_logger.debug(f'Attempting to write: {df}')
        datasaurus_logger.debug(
            f'Write configuration:'
            f' { {"table_name": file_name, "connection_uri": self.get_uri(), "if_exists": if_exists} }'
        )
        df.write_database(
            table_name=file_name,
            connection_uri=self.get_uri(),
            if_exists=if_exists,
        )
        datasaurus_logger.debug(f'{file_name} written correctly.')

    def file_exists(self, file_name, format: FileFormat = None) -> bool:
        query = self.EXISTS_QUERY.format(table_name=file_name)
        datasaurus_logger.debug(f'Checking if file "{file_name}" exists, running query "{query}"')
        datasaurus_logger.debug(f'uri {self.get_uri()}')
        try:
            pl.read_database(query, self.get_uri())
        except RuntimeError:
            # This is very dirty, fixme
            return False
        return True


class LocalStorageOperationsMixin(StorageOperationMixinBase):
    supported_formats = FileFormat
    needs_format = True

    def file_exists(self, file_name, format: FileFormat) -> bool:
        return pathlib.Path(f'{self.get_uri()}/{file_name}.{format.name}').exists()

    def write_file(self, df: pl.DataFrame, file_name: str, format: FileFormat, **kwargs):
        full_path = (pathlib.Path(self.path) / file_name).with_suffix(suffix=format.suffix)

        if not full_path.exists():
            full_path.parent.mkdir(parents=True, exist_ok=True)

        _write_func = getattr(df, f'write_{format.name}')
        return _write_func(full_path, **kwargs)

    def read_file(self, file_name, columns, format: FileFormat = None, **kwargs):
        full_path = (pathlib.Path(self.path) / file_name).with_suffix(format.suffix)

        if not full_path.exists():
            raise ValueError(f"Cannot find '{full_path}'")

        _read_func = getattr(pl, f'read_{format.name}')

        return _read_func(full_path)
