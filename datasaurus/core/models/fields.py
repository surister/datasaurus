from collections.abc import Collection
from typing import Optional

import polars

ColumnName = str


class Field:
    _override_polars_col = False  # Set only to true in unit tests.

    default_dtype = polars.Utf8
    supported_dtypes = []
    cast_map = {}

    def __init__(self,
                 column_name: Optional[str] = None,
                 enforce_dtype=True,
                 dtype=None,
                 *args,
                 **kwargs):
        self.column_name = column_name
        self.enforce_dtype = enforce_dtype
        self.dtype = dtype

        #  Descriptor variables.
        self.name = None
        self._value = None

    def __set_name__(self, owner, name):
        self.name = name

    def __set__(self, instance, value):
        self._value = value

    def __get__(self, instance, owner):
        if self._override_polars_col:
            # We need this to test certain methods that are unreachable from outside
            # the class because of the __get__
            return self

        if instance is None:
            # It's being called from the cls rather an instance of the cls.
            # Ex: Model.col instead of Model().col
            return polars.col(self.get_column_name())
        return self._value

    def get_cast_map(self, cast_map):
        """Transient function that allows for this class' children to modify the cast map"""
        return cast_map

    def get_col_with_dtype(self, current_dtype: polars.DataType):
        cast_map = self.get_cast_map(self.cast_map)
        col = polars.col(self.get_column_name())

        target_dtype = self.dtype or self.default_dtype

        if target_dtype == current_dtype and current_dtype != self.default_dtype:
            raise ValueError(
                f'Are you sure you are using the right column? You are using {type(self)} '
                f'to read a {current_dtype} column type and specifying the dtype to be {target_dtype}')

        if target_dtype == current_dtype:
            return col

        if target_dtype not in self.supported_dtypes:
            raise ValueError(f'Dtype {target_dtype} not supported by {type(self)}')

        if not self.dtype and current_dtype in cast_map:
            return cast_map[current_dtype](col)

        return col.cast(target_dtype)

    def get_column_name(self):
        return self.column_name or self.name

    def __str__(self):
        return f'{self.name}'

    def __repr__(self):
        return f'{self.__class__.__qualname__}<{self.name},' \
               f' dtype={self.dtype or self.default_dtype}>'


class Fields(Collection):
    """Helps to control a group of fields, 1 Fields per Model obj"""

    def __init__(self, initial_fields: list[Field] = None):
        self._fields = initial_fields or []

    def __getitem__(self, item):
        return self._fields[item]

    def __iter__(self):
        return iter(self._fields)

    def __str__(self):
        return str(self._fields)

    def __len__(self):
        return len(self._fields)

    def __contains__(self, item):
        return item.name in self.get_model_columns()

    def extend(self, other: 'Fields') -> None:
        self._fields.extend(other._fields)

    def get_df_columns(self) -> list[str]:
        """Get a list of the columns that will be used on the df write/read operations"""
        return [field.get_column_name() for field in self._fields]

    def get_df_columns_polars(self, current_dtypes: dict[ColumnName: polars.DataType]) -> list[
        polars.Expr]:
        """Get a list of the columns with the proper dtypes casts applied"""
        return [
            column.get_col_with_dtype(current_dtypes[column.get_column_name()])
            if column.enforce_dtype else column.get_column_name()
            for column in self._fields
        ]

    def get_model_columns(self) -> list:
        """Get the list of columns as defined in the Model"""
        return [field.name for field in self._fields]


class BooleanColumn(Field):
    supported_dtypes = [polars.Boolean]
    default_dtype = polars.Boolean


class StringColumn(Field):
    supported_dtypes = [polars.Utf8]
    default_dtype = polars.Utf8


class IntegerColumn(Field):
    supported_dtypes = [polars.UInt8, polars.UInt16, polars.UInt32, polars.UInt64, polars.Int8,
                        polars.Int16, polars.Int32, polars.Int64]
    default_dtype = polars.Int32


class FloatColumn(Field):
    supported_dtypes = [polars.Float32, polars.Float64, polars.Decimal]
    default_dtype = polars.Float32


class DateTimeColumn(Field):
    supported_dtypes = [polars.Datetime, ]
    default_dtype = polars.Datetime

    def __init__(self, format: str = '%Y-%m-%dT%H:%M:%SZ', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.format = format

    def get_cast_map(self, cast_map):
        return {
            polars.Utf8: lambda col: col.str.to_datetime(self.format)
        }


class DateColumn(Field):
    supported_dtypes = [polars.Date]
    default_dtype = polars.Date

    def __init__(self, format: str = '%Y-%m-%d', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.format = format

    def get_cast_map(self, cast_map):
        return {
            polars.Utf8: lambda col: col.str.to_date(self.format)
        }
