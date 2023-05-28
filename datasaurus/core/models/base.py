from functools import partial

from polars import DataFrame


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


class LazyFuncCall:
    """
    Wraps around a function and its argument to an object, it will call that function when that
    object is obtained by __get__, making it lazy.
    """

    def __init__(self, func: callable, *args, **kwargs):
        self.func = partial(func, *args, **kwargs)

    def __get__(self, instance, owner):
        return self.func()


class Manager:
    def __init__(self, meta):
        self.storage = getattr(meta, '__storage__')
        self.table_name = getattr(meta, '__table_name__')

    def read_df(self, columns):
        return self.storage.main.read_file(self.table_name, columns)

    def write_df(self, df, create_table: bool):
        return self.storage.main.write_file(self.table_name, df, create_table)

    def data_exists(self) -> bool:
        return self.storage.main.file_exists(self.table_name)


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
        manager = Manager(meta=meta)
        setattr(cls, '_should_be_recalculated', getattr(meta, '__recalculate__', False))
        setattr(cls, '_manager', manager)
        setattr(cls, 'df', LazyFuncCall(manager.read_df, cls.columns))

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
        if cls._should_be_recalculated or not cls.exists():
            cls._manager.write_df(df=cls.calculate_data(cls), create_table=not cls.exists())


class Model(metaclass=ModelBase):
    pass
