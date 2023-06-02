from functools import partial
from typing import Callable, Optional

import polars
from polars import DataFrame

from datasaurus.core.loggers import datasaurus_logger
from datasaurus.core.storage.base import Storage
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


class Manager:
    """
    Manages DF operations for the Model.
    """

    def __init__(self, meta):
        self.meta = meta
        self.storage = getattr(meta, '__storage__')
        self.table_name = getattr(meta, '__table_name__')

    def _validate_columns(self, df: polars.DataFrame, columns: list[str]):
        datasaurus_logger.debug('Validating columns')
        if len(set(df.columns) - set(columns)) != 0:
            raise Exception(
                f"Cannot use the Dataframe since columns don't match, dataframe's columns: {df.columns}, model's columns: {columns}")

    def read_df(self, columns) -> polars.DataFrame | NO_DATA:
        try:
            self.storage.from_env.read_file(self.table_name, columns)
        except RuntimeError as e:
            if 'no such table' in str(e):
                return NO_DATA
            else:
                raise e

    def write_df(self, df: polars.DataFrame, storage: Storage = None) -> None:
        storage = storage or self.storage.from_env
        if getattr(self.meta, '__auto_select__', False):
            df = df.select(self.columns)
        return storage.write_file(self.table_name, df)

    def data_exists(self, storage=None) -> bool:
        storage = storage or self.storage.from_env
        return storage.file_exists(self.table_name)


class ModelBase(type):
    df: DataFrame
    _manager: Manager
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
        manager = Manager(meta=meta)
        setattr(cls, '_should_be_recalculated', getattr(meta, '__recalculate__', False))
        setattr(cls, '_manager', manager)
        setattr(cls, 'df', lazy_func(cls._get_df))

    @property
    def columns(cls):
        return [str(field) for field in cls.__dict__.values() if
                isinstance(field, Field)]

    def calculate_data(cls):
        raise NotImplementedError()

    def exists(cls, storage: Storage = None) -> bool:
        """
        Return whether the source data to populate the df exists in the given storage,
        if no storage is provided the default one will be used (Defined in Meta)
        """
        return cls._manager.data_exists(storage)

    def save(cls, to=None):
        datasaurus_logger.debug(f'Writing {cls}')
        df = cls.df
        cls._manager._validate_columns(df, cls.columns)
        cls._manager.write_df(df=df, storage=to)

    def from_dict(cls, d: dict):
        setattr(cls, '_data_from_cls', d)
        return cls

    def _get_df(cls):
        def inner_get_df():
            """
            1. Data from constructor
            2. Data from calculation
            3. Data from Storage
            """
            if cls._data_from_cls:
                df = polars.DataFrame(cls._data_from_cls)
                del cls._data_from_cls
                cls._data_from_cls = None
                return df

            if cls._should_be_recalculated:
                df = cls.calculate_data()
                return df

            df = cls._manager.read_df(cls.columns)
            return df

        df = inner_get_df()
        cls._manager._validate_columns(df, cls.columns)
        return df


class Model(metaclass=ModelBase):
    pass
