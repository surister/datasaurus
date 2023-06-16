from collections.abc import Collection
from typing import Optional


class Field:
    def __init__(self, column_name: Optional[str] = None, *args, **kwargs):
        self.column_name = column_name
        self.name = None
        self._value = None

    def __set_name__(self, owner, name):
        self.name = name

    def __set__(self, instance, value):
        self._value = value

    def __get__(self, instance, owner):
        if instance is None:
            import polars
            return polars.col(self.get_column_name())
        return self._value

    def get_column_name(self):
        return self.column_name or self.name

    def __str__(self):
        return f'{self.name}'

    def __repr__(self):
        return f'{self.__class__.__qualname__}<{self.name}>'


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

    def get_df_columns(self) -> list:
        """Get a list of the columns that will be used on the df write/read operations"""
        return [field.get_column_name() for field in self._fields]

    def get_model_columns(self) -> list:
        """Get the list of columns as defined in the Model"""
        return [field.name for field in self._fields]


class StringField(Field):
    pass


class IntegerField(Field):
    pass
