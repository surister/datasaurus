import dataclasses
import logging

from datasaurus.core.storage.base import Storage, AUTO_RESOLVE
from datasaurus.core.storage.mixins import LocalStorageOperationsMixin, SQLStorageOperationsMixin


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
        host = '@' + self.host if self.host and self.user else ''
        port = ':' + self.port if self.port else ''
        if self.path.startswith('/') and scheme:
            self.path = self.path.replace('/', '//', 1)

        if not self.path.startswith('/') and self.port and self.path:
            self.path = '/' + self.path

        path = self.path
        query = '?' + self.query if self.query else ''
        fragment = '#' + self.fragment if self.fragment else ''

        return scheme + user + password + host + port + path + query + fragment

    def get_uri(self):
        return self.uri_unparse()


class LocalStorage(LocalStorageOperationsMixin, Storage):
    def __init__(self, path: str, name: str = '', environment: str = AUTO_RESOLVE):
        super().__init__(name, environment)
        self.path = path

    def get_uri(self):
        return Uri(path=self.path).get_uri()


class SqliteStorage(SQLStorageOperationsMixin, Storage):
    def __init__(self, path: str, name: str = '', environment: str = AUTO_RESOLVE):
        super().__init__(name, environment)
        self.path = path

    def get_uri(self) -> str:
        return Uri(scheme='sqlite', path=self.path).get_uri()


class MariadbStorage(SQLStorageOperationsMixin, Storage):
    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 database: str,
                 port: str = '3306',
                 storage_name: str = '',
                 environment: str = AUTO_RESOLVE):
        super().__init__(storage_name, environment)

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

        if self.host == 'localhost':
            logging.warning("By using localhost as host, mysql will try to use UNIX sockets, "
                            "make sure your application has access (you might get <Can't connect"
                            " to local MySQL server through socket> errors), if you want to use"
                            " TCP instead use 127.0.0.1, the ip of the machine or the correct hostname")

    def get_uri(self):
        return Uri(scheme='mysql',
                   user=self.username,
                   password=self.password,
                   host=self.host,
                   port=self.port,
                   path=self.database).get_uri()


class MysqlStorage(SQLStorageOperationsMixin, Storage):
    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 database: str,
                 port: str = '3306',
                 storage_name: str = '',
                 environment: str = AUTO_RESOLVE):
        super().__init__(storage_name, environment)

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

        if self.host == 'localhost':
            logging.warning("By using localhost as host, mysql will try to use UNIX sockets, "
                            "make sure your application has access (you might get <Can't connect"
                            " to local MySQL server through socket> errors), if you want to use"
                            " TCP instead use 127.0.0.1, the ip of the machine or the correct hostname")

    def get_uri(self):
        return Uri(scheme='mysql',
                   user=self.username,
                   password=self.password,
                   host=self.host,
                   port=self.port,
                   path=self.database).get_uri()


class PostgresStorage(SQLStorageOperationsMixin, Storage):
    EXISTS_QUERY = 'SELECT * FROM "{table_name}" LIMIT 1'

    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 database: str,
                 port: str = '5432',
                 storage_name: str = '',
                 environment: str = AUTO_RESOLVE):
        super().__init__(storage_name, environment)

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def get_uri(self):
        return Uri(scheme='postgresql',
                   user=self.username,
                   password=self.password,
                   host=self.host,
                   port=self.port,
                   path=self.database).get_uri()
