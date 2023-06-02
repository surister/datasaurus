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
