from functools import partial
from typing import Callable, Optional

import polars
from polars import DataFrame

from datasaurus.core.models.exceptions import MissingMeta, FormatNotSupportedByModelError, \
    FormatNeededError, FieldNotExistsError
from datasaurus.core.storage.format import DataFormat
from datasaurus.core.storage.base import Storage, StorageGroup
from datasaurus.core.models.fields import Field, Fields


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
    supported_opts_from_meta = [
        'storage',
        'table_name',
        'auto_select',
        'recalculate',
        'format',
        'fields',
    ]

    def __init__(self, meta, model):
        self.meta = meta
        self.model = model

        # Options from meta
        self.storage = None
        self.table_name = ''
        self.auto_select = False
        self.recalculate = False
        self.format = None

        # Options from model
        self.fields = Fields()

        self._populate_from_meta()
        self._populate_from_model()

    def _populate_from_model(self):
        """
        Populates Options values from the given model on the __init__, we also inherit fields from the parent classes
        if they are models.
        """
        # Fields defined in the current model.
        new_fields = [
            field for field in
            self.model.__dict__.values() if isinstance(field, Field)
        ]

        new_fields = Fields(new_fields)

        # Set fields from base classes.
        for base in self.model.mro()[:1]:  # We ignore the first one because it is itself, otherwise it conflicts.
            if hasattr(base, '_meta'):
                for parent_field in base._meta.fields:
                    if parent_field in new_fields:
                        raise ValueError(
                            f"Field '{parent_field}' from {self.model} clashes with parent model {base}"
                        )

                new_fields.extend(base._meta.fields)

        self.fields = new_fields

    def _populate_from_meta(self):
        """
        Populates Options values from the Meta class given on the __init__, only options from supported_opts_from_meta
        are populated, raises Value error if there is an option in Meta that is not defined in supported_opts..
        """
        opts = self.meta.__dict__.copy()
        for attr in self.meta.__dict__:
            if attr.startswith('_'):
                # We do not support dunder options, and don't
                # care about default class attrs like __doc__
                del opts[attr]

            if attr in self.supported_opts_from_meta:
                setattr(self, attr, getattr(self.meta, attr))
                del opts[attr]

        if opts:
            raise ValueError(f'Invalid Meta options exists: {opts}')

    def __str__(self):
        return f'{self.__class__.__qualname__}({vars(self)})'


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
        if not meta:
            raise MissingMeta(f'Model {cls} does not have Meta')

        opts = Options(meta, cls)

        setattr(cls, '_meta', opts)
        setattr(cls, 'df', lazy_func(cls._get_df))

    def _get_storage_or_default(cls, storage: Optional[Storage | StorageGroup],
                                environment: Optional[str] = None) -> Storage:
        """
        Resolves the appropriate storage and its environment.

        - If no storage is passed, the default storage will be returned (from meta).
        - If no environment is passed and either the passed storage or the default storage is a
        StorageGroup, returns the appropriate as per environment variable.

        """
        storage = storage or cls._meta.storage
        if not isinstance(storage, Storage):
            if StorageGroup in storage.__bases__ and len(storage.__bases__) == 1:
                # If we use Model.save(to=Storage) instead of Storage.from_env
                # or Storage.environment directly when passing the storage,
                # we'd receive a StorageGroup instead of a Storage, by default
                # return StorageGroup.from_env
                storage = storage.from_env

        if environment:
            storage = storage.storage_group.with_env(environment)
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
        3. Data from Storage - Storage
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
            df = using.read_file(cls._meta.table_name, cls._meta.fields.get_df_columns(), format)
        return df

    def _get_df(cls, storage: Optional[Storage | StorageGroup] = None):
        """
        Applies schema validation, data validations and options to the newly created dataframe.
        """
        df = cls._create_df(using=storage)

        if cls._meta.auto_select:
            df = df.select(cls._meta.fields)

        columns_from_df = frozenset(df.columns)
        missing_columns = columns_from_df.difference(cls._meta.fields)

        if missing_columns:
            raise ValueError(
                f"Dataframe columns do not match. df.columns: {df.columns}, models: {cls._meta.fields}"
            )
        return df


class Model(metaclass=ModelBase):
    _meta: Options  # Type hint so autocompletion is helpful!

    def __init__(self, **kwargs):
        for column, column_value in kwargs.items():

            if column not in self._meta.fields.get_model_columns():
                raise FieldNotExistsError(
                    f"{self} does not have column '{column}', columns are: {self.fields}")
            setattr(self, column, column_value)

    def __str__(self):
        cls_name = self.__class__.__qualname__
        return f'<{cls_name}: {cls_name} object ({", ".join(self.fields)})>'

    @classmethod
    @property
    def fields(cls):
        return cls._meta.fields.get_model_columns()

    @classmethod
    def from_dict(cls, d: dict):
        setattr(cls, '_data_from_cls', d)
        return cls

    def calculate_data(self):
        raise NotImplementedError()

    @classmethod
    def save(cls, to: 'Storage' = None,
             format: DataFormat = None,
             table_name: str = None,
             environment: str = None,
             **kwargs):

        storage = cls._get_storage_or_default(to, environment=environment)
        format = cls._get_format_or_default(format)
        table_name = table_name or cls._meta.table_name

        if format and not storage.supports_format(format):
            raise FormatNotSupportedByModelError(
                f"Storage of type '{type(storage)}' does not support format '{format}',"
                f" supported formats by this storage are '{storage.supported_formats}'"
            )

        if storage.needs_format and not format:
            raise FormatNeededError(
                f"Cannot save Dataframe because storage of type '{type(storage)}'"
                " needs a format and it was not provided"
            )

        df = cls._get_df()
        storage.write_file(df, table_name, format=format, **kwargs)
