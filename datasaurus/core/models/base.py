from functools import partial
from typing import Callable, Optional

import polars
from polars import DataFrame

from datasaurus.core.models import DataFormat
from datasaurus.core.storage.base import Storage, StorageGroup
from datasaurus.core.storage.fields import Field


class lazy_func:
    """
    Wraps around a function and its argument to an object, it will call that function when that
    object is obtained by __get__, making it lazy.
    """

    def __init__(self, func: Callable, *args, **kwargs):
        self.func = partial(func, *args, **kwargs)

    def __get__(self, instance, owner):
        return self.func()


class Options:
    supported_opts = [
        'storage',
        'table_name',
        'auto_select',
        'recalculate',
        'format'
    ]

    def __init__(self, meta):
        self.meta = meta

        self.storage = None
        self.table_name = ''
        self.auto_select = False
        self.recalculate = False
        self.format = None

        self._prepare()

    def _prepare(self):
        """
        We set every meta option to the cls, if the attr does not exist in cls.supported_opts raise
        a ValueError.
        """
        opts = self.meta.__dict__.copy()
        for attr in self.meta.__dict__:
            if attr.startswith('_'):
                # We do not support dunder options, and don't
                # care about default class attrs like __doc__
                del opts[attr]

            if attr in self.supported_opts:
                setattr(self, attr, getattr(self.meta, attr))
                del opts[attr]

        if opts:
            raise ValueError(f'Invalid Meta options exists: {opts}')


class ModelBase(type):
    df: DataFrame
    _meta: Options
    # This is where the data from cls.from_dict will be stored temporarily
    _data_from_cls: Optional[dict] = None

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
        setattr(cls, '_meta', Options(meta))
        setattr(cls, 'df', lazy_func(cls._get_df))
        setattr(cls, 'columns', cls._get_fields())

    def _get_fields(cls):
        return [str(column) for column in cls.__dict__.values() if
                isinstance(column, Field)]

    def _get_storage_or_default(cls, storage: Optional[Storage | StorageGroup]) -> Storage:
        """
        Used to get either the correct Storage instance or the default Meta storage.
        """
        storage = storage or cls._meta.storage
        if not isinstance(storage, Storage):
            if StorageGroup in storage.__bases__ and len(storage.__bases__) == 1:
                # If we use Model.save(to=Storage) instead of
                # Storage.from_env or Storage.environment
                # directly when passing the storage, we'll receive a StorageGroup instead
                # of a storage, by default we'll just take it from the environment.
                storage = storage.from_env
        return storage

    def _get_format_or_default(cls, format: Optional[DataFormat]):
        """
        If not format is given tries to infer it from the Meta.table_name
        """
        if not format and cls._meta.table_name.__contains__('.'):
            table_name, file_extension = cls._meta.table_name.split('.')
            if file_extension:
                format = file_extension
        return format

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

        elif cls._meta.recalculate:
            df = cls.calculate_data(cls)

        else:
            using = cls._get_storage_or_default(using)
            format = cls._meta.format
            if format and not using.supports_format(format):
                raise ValueError(
                    f"Storage of type '{type(using)}' does not support format '{format}',"
                    f" supported formats by this storage are '{using.supported_formats}'"
                )
            df = using.read_file(cls._meta.table_name, cls.columns, format)
        return df

    def _get_df(cls, storage: Optional[Storage | StorageGroup] = None):
        """
        Applies schema validation, data validations and options to the newly created dataframe.
        """
        df = cls._create_df(using=storage)

        if cls._meta.auto_select:
            df = df.select(cls.columns)

        columns_from_df = frozenset(df.columns)
        missing_columns = columns_from_df.difference(cls.columns)

        if missing_columns:
            raise ValueError(
                f"Dataframe columns do not match. df.columns: {df.columns}, models: {cls.columns}"
            )
        return df


class Model(metaclass=ModelBase):
    @classmethod
    def from_dict(cls, d: dict):
        setattr(cls, '_data_from_cls', d)
        return cls

    def calculate_data(self):
        raise NotImplementedError()

    @classmethod
    def save(cls, to: 'Storage' = None, format: DataFormat = None, **kwargs):
        storage = cls._get_storage_or_default(to)
        format = cls._get_format_or_default(format)

        if format and not storage.supports_format(format):
            raise ValueError(
                f"Storage of type '{type(storage)}' does not support format '{format}',"
                f" supported formats by this storage are '{storage.supported_formats}'"
            )

        if storage.needs_format and not format:
            raise Exception(
                f"Cannot save Dataframe because storage of type '{type(storage)}' needs a format"
            )

        df = cls._get_df()
        storage.write_file(df, cls._meta.table_name, format=format, **kwargs)
