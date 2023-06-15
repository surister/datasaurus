from typing import Optional


class Field:
    def __init__(self, column_name: Optional[str] = None, *args, **kwargs):
        self.column_name = column_name
        self._value = None

    def __set_name__(self, owner, name):
        self.name = self.column_name or name

    def __set__(self, instance, value):
        self._value = value

    def __get__(self, instance, owner):
        if instance is None:
            import polars
            return polars.col(self.name)
        return self._value

    def __str__(self):
        return self.name


class StringField(Field):
    pass


class IntegerField(Field):
    pass
