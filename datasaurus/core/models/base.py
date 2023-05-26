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


class Manager:
    def __init__(self, meta):
        self.storage = getattr(meta, '__storage__')
        self.table_name = getattr(meta, '__table_name__')

    def read_df(self, columns):
        return self.storage.main.read_file(self.table_name, columns)

    def write_df(self):
        pass

    def __get__(self, instance, owner):
        return self.read_df(owner.columns)


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
        setattr(cls, 'df', manager)

    @property
    def columns(cls):
        return [str(field) for field in cls.__dict__.values() if
                isinstance(field, Field)]

    def ensure_exists(cls):
        return cls.df.check()


class Model(metaclass=ModelBase):
    pass
