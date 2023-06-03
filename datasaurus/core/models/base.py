from functools import partial
from typing import Callable, Optional

import polars
from polars import DataFrame

from datasaurus.core.loggers import datasaurus_logger
from datasaurus.core.storage.base import Storage, StorageGroup
from datasaurus.core.storage.fields import Field

NO_DATA = type('NO_DATA', (), {})


class lazy_func:
    """
    Wraps around a function and its argument to an object, it will call that function when that
    object is obtained by __get__, making it lazy.
    """

    def __init__(self, func: Callable, *args, **kwargs):
        self.func = partial(func, *args, **kwargs)

    def __get__(self, instance, owner):
        return self.func()


class ModelBase(type):
    df: DataFrame
    _data_from_cls: Optional[
        dict] = None  # This is where the data from cls.from_dict will be stored temporarily

    def __new__(cls, name, bases, attrs, **kwargs):
        super_new = super().__new__

        # This beauty here has been 'taken' from django itself, it ensures that only
        # subclasses of ModelBase are initiated.
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            return super_new(cls, name, bases, attrs)

        _new_class = super_new(cls, name, bases, attrs, **kwargs)
        _new_class._prepare()
        return _new_class

    def _prepare(cls):
        meta = getattr(cls, 'Meta', None)
        setattr(cls, '_meta', meta)
        setattr(cls, '_should_be_recalculated', getattr(meta, '__recalculate__', False))
        setattr(cls, 'df', lazy_func(cls._get_df))

    @property
    def columns(cls):
        return [str(column) for column in cls.__dict__.values() if
                isinstance(column, Field)]

    def _get_storage_or_default(cls, storage: Optional[Storage | StorageGroup]) -> Storage:
        """
        Used to get either the correct Storage instance or the default Meta storage.
        """
        storage = storage or cls._meta.__storage__
        if not isinstance(storage, Storage):
            if  StorageGroup in storage.__bases__ and len(storage.__bases__) == 1:
                # If we use Model.save(to=Storage) instead of
                # using Storage.from_env, or Storage.environment
                # directly when passing the storage, we'll receive a StorageGroup instead
                # of a storage, by default we'll just take it from the environment.
                storage = storage.from_env
        return storage

    def _create_df(cls, using: Optional[Storage]) -> DataFrame:
        """
        Does the heavy lifting of creating the Dataframe from the right data source, depending on
        opts (meta), the order of priority is as follows:

        1. Data from constructor - Model.from_dict({'column1': [1,2,3})
        2. Data from calculation - Model.calculate_data()
        3. Data from Storage - Storage.from_env
        """
        if cls._data_from_cls is not None:
            df = polars.DataFrame(cls._data_from_cls)
            del cls._data_from_cls
            cls._data_from_cls = None

        elif cls._should_be_recalculated:
            df = cls.calculate_data(cls)

        else:
            using = cls._get_storage_or_default(using)
            df = using.read_file(cls._meta.__table_name__, cls.columns)
        return df

    def _get_df(cls, storage: Optional[Storage] = None):
        df = cls._create_df(using=storage)

        if getattr(cls._meta, '__auto_select__', False):
            df = df.select(cls.columns)

        columns_from_df = frozenset(df.columns)
        missing_columns = columns_from_df.difference(cls.columns)

        if missing_columns:
            raise ValueError(
                f"Dataframe columns do not match. df.columns: {df.columns}, models: {cls.columns}"
            )
        return df

    def from_dict(cls, d: dict):
        setattr(cls, '_data_from_cls', d)
        return cls


class Model(metaclass=ModelBase):
    def calculate_data(self):
        raise NotImplementedError()

    @classmethod
    def save(cls, to: 'Storage' = None):
        storage = cls._get_storage_or_default(to)
        df = cls._get_df(storage)
        storage.write_file(df, cls._meta.__table_name__)
