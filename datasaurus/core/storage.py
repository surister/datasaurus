import logging
import os
import pathlib
from abc import abstractmethod, ABC
from typing import Union


class _auto_resolve:
    def __str__(self):
        return 'AUTO_RESOLVE'


AUTO_RESOLVE = _auto_resolve()

ENVIRONMENT = Union[AUTO_RESOLVE, str]


class CannotResolveEnvironmentException(Exception):
    pass


class Storage(ABC):
    """
    Abc for any Storage class.
    """

    def get_uri(self): ...

    @abstractmethod
    def read_data(self): ...

    def __str__(self):
        return self.__class__.__qualname__


class ProxyStorage:
    """
    Holds the reference to the Storage and keeps metadata about it, such as:
    - Environment
    - Storage type
    """

    def __init__(self, storage: Storage, environment: ENVIRONMENT):
        self.storage = storage
        self.environment = environment

    def __set_name__(self, owner, name):
        if not isinstance(owner(), StorageGroup):
            logging.warning('Cannot register ')  # Todo better warning message

        if self.environment == AUTO_RESOLVE:
            self.environment = name

    @property
    def data(self):
        return self.storage.read_data()

    def __str__(self):
        return f'{self.__class__.__qualname__}<{self.storage}, environment={self.environment}>'


def define_storage(storage: Storage, environment: ENVIRONMENT = AUTO_RESOLVE):
    return ProxyStorage(storage=storage, environment=environment)


class StorageGroup:
    @classmethod
    @property
    def environments(cls):
        return [
            element.environment
            for element in cls.__dict__.values()
            if isinstance(element, ProxyStorage)
        ]

    @classmethod
    @property
    def from_env(cls):
        environment_key = os.getenv(f'{cls.__name__}_ENVIRONMENT')

        if environment_key is None:
            environment_key = os.getenv('DATASAURUS_ENVIRONMENT')

        try:
            environment = getattr(cls, environment_key)
        except TypeError as e:
            raise CannotResolveEnvironmentException from e

        return environment


class LocalStorage(Storage):
    def __init__(self, path, file_name=None):
        self.path = path
        self.file_name = file_name

    def read_data(self):
        return pathlib.Path(self.path + self.file_name).read_text()
