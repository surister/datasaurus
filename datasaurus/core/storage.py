import logging
from collections.abc import Mapping
from typing import Union


class _auto_resolve:
    def __str__(self):
        return 'AUTO_RESOLVE'


AUTO_RESOLVE = _auto_resolve()


def resolve_environment():
    import os
    environment = os.getenv('DATASAURUS_ENVIRONMENT', None)


class Storage:
    """
    Abc for any Storage class.
    """

    def get_uri(self): ...

    def __str__(self):
        return self.__class__.__name__


ENVIRONMENT_PARAMETER = Union[AUTO_RESOLVE, str]


class ProxyStorage:
    """
    Holds the reference to the Storage and kees metadate about it, such as:
    - Environment
    - Storage
    """

    def __init__(self, storage: Storage, environment: ENVIRONMENT_PARAMETER):
        self.storage = storage
        self.environment = environment

    def __set_name__(self, owner, name):
        if self.environment == AUTO_RESOLVE:
            self.environment = name

        if not isinstance(owner(), ScopedStorage):
            logging.warning('Cannot register ')

        owner.environments[owner.__name__] = self.environment

    def __str__(self):
        return f'{self.__class__.__qualname__}<{self.storage}, environment={self.environment}>'


def define_storage(storage: Storage, environment: ENVIRONMENT_PARAMETER = AUTO_RESOLVE):
    return ProxyStorage(storage=storage, environment=environment)


class Environments(Mapping):
    """
    Dictionary implementation where keys are always holding lists of data, meaning that duplicated
    keys are allowed.
    """
    environments = dict()

    def __getitem__(self, item):
        return self.environments[item]

    def __setitem__(self, key, value):
        if key not in self.environments:
            self.environments[key] = []
        self.environments[key].append(value)
        return self

    def __iter__(self):
        return iter(self.environments)

    def __len__(self):
        return len(self.environments)

    def __iadd__(self, other):
        self[other[0]] = other[1]
        return self

    def __repr__(self):
        return repr(self.environments)


class ScopedStorage:
    environments = Environments()

    @classmethod
    @property
    def from_env(cls):
        environment = 'ci'
        return getattr(cls, environment)


class LocalStorage(Storage):
    def __init__(self, path, file_name=None):
        self.path = path
        self.file_name = file_name
