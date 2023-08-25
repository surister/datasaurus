import os
from abc import abstractmethod, ABC
from typing import Union, Optional

import polars

from datasaurus.core import classproperty
from datasaurus.core.storage.format import DataFormat, FormatNotSet


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
    supported_formats: DataFormat = type('NoFormat', (FormatNotSet,), {})()
    needs_format: bool = False

    def __init__(self, name: str, environment_name: ENVIRONMENT):
        self.environment_name = environment_name
        self.name = name
        # Reference to the storage group it belongs
        self.storage_group = None

    def get_uri(self):
        ...

    @abstractmethod
    def read_file(self, file_name: str, columns: list, format: DataFormat) -> polars.DataFrame:
        ...

    @abstractmethod
    def file_exists(self, file_name, format: Optional[DataFormat]) -> bool:
        pass

    @abstractmethod
    def write_file(self, data, file_name, format: Optional[DataFormat], **kwargs) -> None:
        pass

    def supports_format(self, format: DataFormat):
        return format in self.supported_formats

    def __set_name__(self, owner, name):
        self.storage_group = owner
        if not isinstance(owner(), StorageGroup):
            raise Exception('Cannot register')  # Todo refactor exception

        if self.environment_name == AUTO_RESOLVE:
            self.environment_name = name

    def __str__(self):
        return f'{self.__class__.__qualname__}<environment={self.environment_name}>'


class StorageGroup:
    # Todo: Get a better name for this class.
    @classmethod
    @property
    def environments(cls):
        return [
            element.environment_name
            for element in cls.__dict__.values()
            if isinstance(element, Storage)
        ]

    @classmethod
    def with_env(cls, environment):
        if environment not in cls.environments:
            raise Exception(
                f"Storage Group '{cls}' does not have environment {environment},"
                f" declared environments are : {cls.environments}")
        return getattr(cls, environment)

    @classproperty
    def from_env(cls) -> Storage:
        """
        Tries to infer the environment from two different env variables.

        - {SERVICENAME}_ENVIRONMENT (1)
        - DATASAURUS_ENVIRONMENT (2)

        (1) Takes precedence over (2)
        You can use (2) to set a global state of your workflows and fine tune some Storages
        with (1) if needed.

        If no environment can be inferred it raises CannotResolveEnvironmentException.
        """
        datasaurus_env_key = os.getenv('DATASAURUS_ENVIRONMENT')
        service_env_key = os.getenv(f'{cls.__name__}_ENVIRONMENT')

        if env_key := datasaurus_env_key or service_env_key:
            try:
                return getattr(cls, env_key)
            except AttributeError:
                raise Exception(f"Storage '{cls}' does not have environment '{env_key}'")

        raise CannotResolveEnvironmentException(
            f"Neither 'DATASAURUS_ENVIRONMENT' nor '{cls.__name__}_ENVIRONMENT' environment "
            "variables are defined but 'from_env' needs any of these two defined in order to "
            "resolve the right storage."
        )
