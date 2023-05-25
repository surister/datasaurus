import json
import pathlib
import sqlite3
import urllib.parse
from functools import partial

import polars as pl

from datasaurus.core.storage.base import Storage


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


class LocalStorage(Storage):
    __slots__ = 'path',

    def __init__(self, path):
        self.path = path

    def read_file(self, file_name):
        return pathlib.Path(self.path + file_name).read_text()


class Uri(urllib.parse.ParseResult):
    pass


def gen_uri(scheme: str, netloc: str, path: str, params: str, query: str, fragment: str):
    return Uri(scheme=scheme, netloc=netloc, path=path, params=params, query=query,
               fragment=fragment)


gen_uri_from_scheme_path = partial(gen_uri, netloc='', params='', query='', fragment='')


class SqliteStoragePolars(Storage):
    def __init__(self, url):
        self.url = url

    def get_uri(self) -> str:
        return gen_uri_from_scheme_path(scheme='sqlite', path=self.url).geturl()

    def read_file(self, file_name: str, columns: list):
        query = f'SELECT {list_to_sql_columns(columns)} FROM {file_name}'
        return pl.read_database(query, self.get_uri())


class SqliteStorage(Storage):
    __slots__ = ('url', 'conn')

    def __init__(self, url):
        self.url = url
        self.conn = sqlite3.connect(url)
        self.conn.row_factory = lambda cursor, row: json.loads(row[0])

    def execute_query(self, query: str, commit: bool = False):
        cur = self.conn.cursor()
        cur.execute(query)

        if commit:
            self.conn.commit()

        return cur.fetchall()

    def read_file(self, file_name: str, columns: list[str]):
        query = f'SELECT json_object({list_to_sqlite_jsonobject_query(columns)}) FROM {file_name}'
        return self.execute_query(query)
