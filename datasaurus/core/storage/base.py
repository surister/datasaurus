import logging
import os
from abc import abstractmethod, ABC
from typing import Union


class _auto_resolve:
    def __str__(self):
        return 'AUTO_RESOLVE'


AUTO_RESOLVE = _auto_resolve()
ENVIRONMENT = Union[type(AUTO_RESOLVE), str]


class CannotResolveEnvironmentException(Exception):
    pass


class Storage(ABC):
    """
    Abc for any Storage class.
    """

    def get_uri(self): ...

    @abstractmethod
    def read_file(self, file_name: str, columns: list): ...

    @abstractmethod
    def file_exists(self, file_name) -> bool:
        pass

    @abstractmethod
    def write_file(self, file_name, data, create_table: bool):
        pass

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

    def read_file(self, file_name: str, columns: list[str]):
        return self.storage.read_file(file_name, columns)

    def file_exists(self, file_name: str):
        return self.storage.file_exists(file_name)

    def write_file(self, file_name, df, create_table: bool):
        return self.storage.write_file(file_name, df, create_table)

    def __str__(self):
        return f'{self.__class__.__qualname__}<{self.storage}, environment={self.environment}>'


def define_storage(storage: Storage, environment: ENVIRONMENT = AUTO_RESOLVE):
    # Rename that allows for expansion and partial usage in the future.
    return ProxyStorage(storage=storage, environment=environment)


class StorageGroup:
    # Todo: Get a better name for this class.
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
    def from_env(cls) -> ProxyStorage:
        """
        Tries to infer the environment from two different env variables.

        - {SERVICENAME}_ENVIRONMENT (1)
        - DATASAURUS_ENVIRONMENT (2)

        (1) Takes precedence over (2)
        You can use (2) to set a global state of your workflows and fine tune it some Storages
        with (1) if needed.

        If no environment can be inferred it raises CannotResolveEnvironmentException.
        """
        environment_key = os.getenv(f'{cls.__name__}_ENVIRONMENT')

        if environment_key is None:
            environment_key = os.getenv('DATASAURUS_ENVIRONMENT')
        try:
            environment = getattr(cls, environment_key)
        except TypeError as e:
            raise CannotResolveEnvironmentException from e
        return environment
