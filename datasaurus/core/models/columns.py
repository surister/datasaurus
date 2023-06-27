from collections.abc import Collection
from typing import Optional, List, Dict

import polars

ColumnName = str


class Column:
    """
    Represents a column.

    # Todo
    """
    _override_polars_col = False  # Set only to true in unit tests.

    default_dtype = polars.Utf8
    supported_dtypes = []
    cast_map = {}

    def __init__(self,
                 name: Optional[str] = None,
                 enforce_dtype: bool = True,
                 dtype: Optional[polars.DataType] = None,
                 unique: bool = False,
                 *args,
                 **kwargs):
        self.column_name = name
        self.enforce_dtype = enforce_dtype
        self.dtype = dtype
        self.unique = unique

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
            # the class because of the descriptor behaviour.
            return self

        if instance is None:
            # It's being called from the cls rather an instance of the cls.
            # Ex: Model.col instead of Model().col
            return polars.col(self.get_column_name())
        return self._value

    def get_cast_map(self, cast_map):
        """
        Transient function that allows for `Column` children to modify the cast map.

        Examples
        --------

        >>>class StringColumn(Column):
        ...    def get_cast_map(cast_map):
        ...        if not str in cast_map:
        ...            cast_map[str] = lambda x: str(x)
        ...        return cast_map
        """
        return cast_map

    def get_col_with_dtype(self, current_dtype: polars.DataType):
        """
        Returns the equivalent of the `Column` to a `polars.col` type with the
        data type cast applied.

        It takes into account the current df's column data type because sometimes for example
        if the current dtype and our defined column dtype is the same, no casting is needed.

        If any entry exists in `cast_map` it will use that casting, specially useful when
        doing `col.cast` is not enough to cast from one type to another, for example when
        casting a string containing a date to date dtype.

        If no entry exists in `cast_map` it will use the `col.cast` and let polars gives you
        and exception if the casting is for some reason invalid.

        Parameters
        ----------
        current_dtype : polars.DataType
            The current dataframe dtype for this column.

        Note
        ----
        This function should only be used after the dataframe is already loaded, since
        its depends on a current data type to perform certain casting validations.
        """
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
            raise ValueError(f"Dtype '{target_dtype}' is not supported by {type(self)}")

        if not self.dtype and current_dtype in cast_map:
            return cast_map[current_dtype](col)

        return col.cast(target_dtype)

    def get_column_name(self):
        """Returns the defined `column_name` or the name from __set_name__"""
        return self.column_name or self.name

    def __str__(self):
        return f'{self.name}'

    def __repr__(self):
        return f'{self.__class__.__qualname__}<{self.name},' \
               f' dtype={self.dtype or self.default_dtype}>'


class Columns(Collection):
    """
    Class that represents a group of Columns. Used for column filtering, attribute retrieval
    and general utils operations.

    One `Columns` object per Model is expected but not enforced.
    """

    def __init__(self, initial_columns: List[Column] = None):
        self._columns = initial_columns or []

    def __getitem__(self, item):
        return self._columns[item]

    def __iter__(self):
        return iter(self._columns)

    def __str__(self):
        return str(self._columns)

    def __len__(self):
        return len(self._columns)

    def __contains__(self, item):
        return item.name in self.get_model_columns()

    def extend(self, other: 'Columns') -> None:
        self._columns.extend(other._columns)

    def get_df_column_names(self) -> List[str]:
        """
        Returns a list of the columns that will be used on the df write/read operations.
        """
        return [column.get_column_name() for column in self._columns]

    def get_df_column_names_by_attrs(self, **attr):
        """
        Returns a list of the columns that match the given attributes and values.

        Examples
        --------

            >>> class Foo(Model):
            ...     name = StringColumn()
            ...     ss_number = StringColumn(unique=True)
            ...     mail = StringColumn(unique=True)

            >>> Foo._meta.columns.get_df_column_names_by_attrs(unique=True)
            ['mail', 'ss_number']
        """
        return [
            column.get_column_name()
            for column in self._columns
            # Don't despair, this sexy but bad one-liner just checks that all attrs exists
            # with a given value in a column.
            if all(map(lambda at: getattr(column, at[0]) == at[1], attr.items()))
        ]

    def get_df_columns_polars(self, current_dtypes: Dict[ColumnName, polars.DataType]) -> List[polars.Expr]:
        """Returns a list of the columns with the proper dtypes casts applied"""
        return [
            column.get_col_with_dtype(current_dtypes[column.get_column_name()])
            if column.enforce_dtype else column.get_column_name()
            for column in self._columns
        ]

    def get_model_columns(self) -> list:
        """
        Returns the list of columns as defined in the Model, they might actually not be
        the one used on read/write opperations.

        Examples:
        ---------

        >>> class Foo(Model):
        ...     attr1 = StringColumn()
        ...     attr2 = IntegerColumn(column_name='myattribute')

        >>>Foo._meta.columns.get_model_columns()
           ['attr1', 'attr2']
        """
        return [column.name for column in self._columns]


class BooleanColumn(Column):
    supported_dtypes = [polars.Boolean]
    default_dtype = polars.Boolean


class StringColumn(Column):
    supported_dtypes = [polars.Utf8]
    default_dtype = polars.Utf8


class IntegerColumn(Column):
    supported_dtypes = [polars.UInt8, polars.UInt16, polars.UInt32, polars.UInt64, polars.Int8,
                        polars.Int16, polars.Int32, polars.Int64]
    default_dtype = polars.Int32


class FloatColumn(Column):
    supported_dtypes = [polars.Float32, polars.Float64, polars.Decimal]
    default_dtype = polars.Float32


class DateTimeColumn(Column):
    supported_dtypes = [polars.Datetime, ]
    default_dtype = polars.Datetime

    def __init__(self, format: str = '%Y-%m-%dT%H:%M:%SZ', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.format = format

    def get_cast_map(self, cast_map):
        return {
            polars.Utf8: lambda col: col.str.to_datetime(self.format)
        }


class DateColumn(Column):
    supported_dtypes = [polars.Date]
    default_dtype = polars.Date

    def __init__(self, format: str = '%Y-%m-%d', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.format = format

    def get_cast_map(self, cast_map):
        return {
            polars.Utf8: lambda col: col.str.to_date(self.format)
        }
