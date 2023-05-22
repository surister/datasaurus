import pathlib
import sqlite3

from datasaurus.core.storage.base import Storage


class LocalStorage(Storage):
    __slots__ = ('path')

    def __init__(self, path):
        self.path = path

    def read_file(self, file_name):
        return pathlib.Path(self.path + file_name).read_text()


class SqliteStorage(Storage):
    def __init__(self, url):
        self.url = url
        self.con = sqlite3.connect(url)

    def read_file(self, file_name: str):
        cur = self.con.cursor()
        cur.execute(f'SELECT * FROM {file_name}')
        return cur.fetchall()
