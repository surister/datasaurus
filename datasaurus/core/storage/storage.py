import dataclasses
import pathlib

import polars as pl

from datasaurus.core.loggers import datasaurus_logger
from datasaurus.core.storage.base import Storage, AUTO_RESOLVE


def list_to_sql_columns(l: list[str]) -> str:
    """
    Transforms ["id", "username"...] into '(id, username)'
    """
    return str(l).replace('[', '').replace(']', '').replace("'", "")


@dataclasses.dataclass
class Uri:
    scheme: str = ''
    user: str = ''
    password: str = ''
    host: str = ''
    port: str = ''
    path: str = ''
    query: str = ''
    fragment: str = ''

    def uri_unparse(self):
        scheme = self.scheme + "://" if self.scheme else ''
        user = self.user
        password = ':' + self.password if self.password else ''
        host = '@' + self.user if self.user and self.host else ''

        if self.path.startswith('/'):
            self.path = self.path.replace('/', '//', 1)
        path = self.path
        query = '?' + self.query if self.query else ''
        fragment = '#' + self.fragment if self.fragment else ''

        return scheme + user + password + host + path + query + fragment

    def get_uri(self):
        return self.uri_unparse()

    def __str__(self) -> str:
        return f'{self.scheme}'


class SQLPolarsStorageMixin:
    def read_file(self, file_name: str, columns: list):
        datasaurus_logger.debug(f'Trying to read {file_name}')
        query = f'SELECT {list_to_sql_columns(columns)} FROM {file_name}'
        datasaurus_logger.debug(f'query: {query}')
        datasaurus_logger.debug(f'uri: {self.get_uri()}')
        return pl.read_database(query, self.get_uri())

    def write_file(self, file_name, df: pl.DataFrame):
        if_exists = 'append' if self.file_exists(file_name) else 'replace'
        datasaurus_logger.debug(f'Attempting to write: {df}')
        datasaurus_logger.debug(
            f'Write configuration: { {"table_name": file_name, "connection_uri": self.get_uri(), "if_exists": if_exists} }'
        )
        df.write_database(
            table_name=file_name,
            connection_uri=self.get_uri(),
            if_exists=if_exists,
            engine='adbc'
        )
        datasaurus_logger.debug(f'{file_name} written correctly.')

    def file_exists(self, file_name) -> bool:
        query = f"SELECT name FROM sqlite_master WHERE name='{file_name}'"
        datasaurus_logger.debug(f'Checking if file "{file_name}" exists, running query "{query}"')
        return pl.read_database(query, self.get_uri()).shape[0] >= 1


class LocalStorageMixin:
    def file_exists(self, file_name) -> bool:
        return pathlib.Path(self.get_uri()).exists()

    def write_file(self, file_name, data):
        # TODO see what to do with file_name here.
        return pathlib.Path(self.get_uri()).write_text(data)

    def read_file(self, file_name):
        return pathlib.Path(self.get_uri()).read_text()


class LocalStorage(LocalStorageMixin, Storage):
    __slots__ = 'path',

    def __init__(self, path: str, name: str = '', environment: str = AUTO_RESOLVE):
        super().__init__(name, environment)
        self.path = path

    def get_uri(self):
        return Uri(path=self.path).get_uri()


class SqliteStorage(SQLPolarsStorageMixin, Storage):
    def __init__(self, path: str, name: str = '', environment: str = AUTO_RESOLVE):
        super().__init__(name, environment)
        self.path = path

    def get_uri(self) -> str:
        return Uri(scheme='sqlite', path=self.path).get_uri()
