import pathlib
import urllib.parse
from functools import partial

import polars as pl

from datasaurus.core.storage.base import Storage, AUTO_RESOLVE


def list_to_sql_columns(l: list[str]) -> str:
    """
    Transforms ["id", "username"...] into '(id, username)'
    """

    return str(l).replace('[', '').replace(']', '').replace("'", "")


def list_to_sqlite_jsonobject_query(l: list[str]) -> str:
    p = [f"'{col}', {col}," for col in l]
    s = "".join(p)
    return s[:-1]


def clean_sql_query(query: str):
    return query.replace("'", "")


class Uri(urllib.parse.ParseResult):
    pass


def gen_uri(scheme: str, netloc: str, path: str, params: str, query: str, fragment: str):
    return Uri(scheme=scheme, netloc=netloc, path=path, params=params, query=query,
               fragment=fragment)


gen_uri_from_scheme_path = partial(gen_uri, netloc='', params='', query='', fragment='')


class SQLPolarsStorageMixin:
    def read_file(self, file_name: str, columns: list):
        query = f'SELECT {list_to_sql_columns(columns)} FROM {file_name}'
        return pl.read_database(query, self.get_uri())

    def write_file(self, file_name, data: pl.DataFrame):
        if_exists = 'append' if self.file_exists(file_name) else 'replace'
        # 'sqlite:////data//data.sqlite',
        return data.write_database(
            table_name=file_name,
            connection_uri=self.get_uri(),
            if_exists=if_exists,
            engine='adbc'
        )

    def file_exists(self, file_name) -> bool:
        query = f"SELECT name FROM sqlite_master WHERE name='{file_name}'"
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

    def get_uri(self, *args):
        return gen_uri_from_scheme_path(scheme='file', path=self.path)


class SqliteStorage(SQLPolarsStorageMixin, Storage):
    def __init__(self, path: str, name: str = '', environment: str = AUTO_RESOLVE):
        super().__init__(name, environment)
        self.path = path

    def get_uri(self) -> str:
        return gen_uri_from_scheme_path(scheme='sqlite', path=self.path).geturl()
