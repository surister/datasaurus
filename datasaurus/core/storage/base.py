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

    def __init__(self, name: str, environment: ENVIRONMENT):
        self.environment = environment
        self.name = name

    def get_uri(self):
        ...

    @abstractmethod
    def read_file(self, file_name: str, columns: list):
        ...

    @abstractmethod
    def file_exists(self, file_name) -> bool:
        pass

    @abstractmethod
    def write_file(self, file_name, data):
        pass

    def __set_name__(self, owner, name):
        if not isinstance(owner(), StorageGroup):
            logging.warning('Cannot register ')  # Todo better warning message
        if self.environment == AUTO_RESOLVE:
            self.environment = name

    def __str__(self):
        return f'{self.__class__.__qualname__}<environment={self.environment}>'


class StorageGroup:
    # Todo: Get a better name for this class.
    @classmethod
    @property
    def environments(cls):
        return [
            element.environment
            for element in cls.__dict__.values()
            if isinstance(element, Storage)
        ]

    @classmethod
    @property
    def from_env(cls) -> Storage:
        """
        Tries to infer the environment from two different env variables.

        - {SERVICENAME}_ENVIRONMENT (1)
        - DATASAURUS_ENVIRONMENT (2)

        (1) Takes precedence over (2)
        You can use (2) to set a global state of your workflows and fine tune it some Storages
        with (1) if needed.

        If no environment can be inferred it raises CannotResolveEnvironmentException.
        """
        datasaurus_env_key = os.getenv('DATASAURUS_ENVIRONMENT')
        service_env_key = os.getenv(f'{cls.__name__}_ENVIRONMENT')

        if env_key := datasaurus_env_key or service_env_key:
            return getattr(cls, env_key)  # Todo fix when environment is set

        raise CannotResolveEnvironmentException(
            f'Neither DATASAURUS_ENVIRONMENT nor {cls.__name__}_ENVIRONMENT are defined but "from_env" needs any of these two defined in order to resolve the right storage.'
        )
