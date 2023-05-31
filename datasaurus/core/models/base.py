from functools import partial
from typing import Callable

import polars
from polars import DataFrame

from datasaurus.core.loggers import datasaurus_logger


class Field:
    def __init__(self, *args, **kwargs):
        pass

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        import polars
        return polars.col(self.name)

    def __str__(self):
        return self.name


class StringField(Field):
    pass


class IntegerField(Field):
    pass


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

    def __init__(self, meta, columns: list[str]):
        self.meta = meta
        self.columns = columns
        self.storage = getattr(meta, '__storage__')
        self.table_name = getattr(meta, '__table_name__')

    def _validate_columns(self, df: polars.DataFrame):
        datasaurus_logger.debug('Validating columns')
        if len(set(df.columns) - set(self.columns)) != 0:
            raise Exception(f"Cannot write df since columns don't match, your dataframe's columns: {df.columns}, your model columns: {self.columns}")

    def read_df(self, columns):
        return self.storage.from_env.read_file(self.table_name, columns)

    def write_df(self, df: polars.DataFrame):
        if getattr(self.meta, '__auto_select__', False):
            df = df.select(self.columns)
        self._validate_columns(df)
        return self.storage.from_env.write_file(self.table_name, df)

    def data_exists(self) -> bool:
        return self.storage.from_env.file_exists(self.table_name)


class ModelBase(type):
    df: DataFrame

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
        manager = Manager(meta=meta, columns=cls.columns)
        setattr(cls, '_should_be_recalculated', getattr(meta, '__recalculate__', False))
        setattr(cls, '_manager', manager)
        setattr(cls, 'df', lazy_func(manager.read_df, cls.columns))

    @property
    def columns(cls):
        return [str(field) for field in cls.__dict__.values() if
                isinstance(field, Field)]

    def calculate_data(cls):
        raise NotImplementedError()

    def exists(cls) -> bool:
        """
        Return whether the source data to populate the df exists, can be used to check if it's
        necessary to calculate the data (for models that are derived of others), or to sanity check
        read operations.
        """
        return cls._manager.data_exists()

    def ensure_exists(cls):
        datasaurus_logger.debug(f'Making sure that {cls} exists')
        if cls._should_be_recalculated or not cls.exists():
            datasaurus_logger.debug(
                f'The data does not exist or __recalculate__ is set to True, recalculating data..')
            cls._manager.write_df(df=cls.calculate_data(cls))


class Model(metaclass=ModelBase):
    pass
